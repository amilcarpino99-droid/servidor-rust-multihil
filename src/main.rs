
use std::fs;
use std::io::{prelude::*, BufReader};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process;
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};


const DIR_ESTATICOS: &str = "public";


const NUM_HILOS: usize = 8;


const DIRECCION: &str = "127.0.0.1:7878";


static ARRANQUE: OnceLock<Instant> = OnceLock::new();


fn marca() -> String {
    let s = ARRANQUE.get_or_init(Instant::now).elapsed().as_secs_f64();
    format!("[+{s:8.3} s]")
}

fn main() {
    let escucha = match TcpListener::bind(DIRECCION) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("❌ No se pudo enlazar {DIRECCION}: {e}");
            process::exit(1);
        }
    };

    let grupo = GrupoDeHilos::new(NUM_HILOS);

    println!("──────────────────────────────────────────");
    println!("  ✅ Servidor encendido");
    println!("  🌐 Escuchando en http://{DIRECCION}");
    println!("  🧵 Grupo de {NUM_HILOS} hilos trabajadores");
    println!("  📁 Estáticos en ./{DIR_ESTATICOS}/  (Ctrl+C para detener)");
    println!("──────────────────────────────────────────");

    for flujo in escucha.incoming() {
        match flujo {
            Ok(flujo) => grupo.ejecutar(move || manejar_conexion(flujo)),
            Err(e) => eprintln!("⚠️  Error aceptando conexión: {e}"),
        }
    }
}

/// Atiende UNA conexión HTTP completa (se ejecuta dentro de un hilo trabajador).
fn manejar_conexion(mut flujo: TcpStream) {
    let inicio = Instant::now();
    let origen = flujo
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "desconocido".to_string());

    // Clonamos el socket para leerlo con un BufReader
    // (la respuesta se escribe en el original).
    let mut lector = match flujo.try_clone() {
        Ok(f) => BufReader::new(f),
        Err(e) => {
            eprintln!("⚠️  No se pudo clonar el socket: {e}");
            return;
        }
    };

    // 1) Línea de petición: "GET /ruta HTTP/1.1"
    let mut linea_peticion = String::new();
    if lector.read_line(&mut linea_peticion).unwrap_or(0) == 0 {
        return; // el cliente cerró la conexión sin enviar nada
    }
    let mut partes = linea_peticion.split_whitespace();
    let metodo = partes.next().unwrap_or("").to_string();
    let ruta = partes.next().unwrap_or("").to_string();

    // 2) Cabeceras: se leen y descartan hasta la línea en blanco.
    let mut linea = String::new();
    loop {
        linea.clear();
        if lector.read_line(&mut linea).unwrap_or(0) == 0 || linea.trim().is_empty() {
            break;
        }
    }

    println!("{} → {metodo} {ruta} · desde {origen}", marca());

    // 3) Enrutado: separamos el camino de la cadena de consulta (?a=b&c=d).
    let (camino, consulta) = match ruta.split_once('?') {
        Some((c, q)) => (c, Some(q)),
        None => (ruta.as_str(), None),
    };

    match (metodo.as_str(), camino) {
        ("GET", "/" | "/index.html") => {
            servir_archivo(&mut flujo, "index.html", "text/html; charset=utf-8");
        }
        ("GET", "/styles.css") => {
            servir_archivo(&mut flujo, "styles.css", "text/css; charset=utf-8");
        }
        ("GET", "/tarea") => {
            // Nombre del hilo trabajador que está atendiendo esta petición.
            let hilo = thread::current().name().unwrap_or("sin-nombre").to_string();
            atender_tarea(&mut flujo, consulta, &hilo);
        }
        _ => responder_error(
            &mut flujo,
            404,
            "Página no encontrada",
            "La ruta solicitada no existe en este servidor.",
        ),
    }

    println!(
        "{} ← {metodo} {ruta} · listo en {} ms",
        marca(),
        inicio.elapsed().as_millis()
    );
}

/// Sirve un archivo de ./public con el Content-Type indicado.
fn servir_archivo(flujo: &mut TcpStream, nombre: &str, tipo: &str) {
    let ruta = Path::new(DIR_ESTATICOS).join(nombre);
    match fs::read(&ruta) {
        Ok(contenido) => enviar(flujo, 200, "OK", tipo, &contenido),
        Err(_) => responder_error(
            flujo,
            404,
            "Archivo no encontrado",
            &format!("No se pudo leer {}", ruta.display()),
        ),
    }
}

/// Endpoint de la demo: /tarea?id=N&delay=S
/// Duerme `delay` segundos (máx. 10) y responde un JSON con el hilo que la atendió.
fn atender_tarea(flujo: &mut TcpStream, consulta: Option<&str>, hilo: &str) {
    let mut id = 0u32;
    let mut delay = 1u64;

    if let Some(q) = consulta {
        for par in q.split('&') {
            if let Some((clave, valor)) = par.split_once('=') {
                match clave {
                    "id" => id = valor.parse().unwrap_or(id),
                    "delay" => {
                        if let Ok(segundos) = valor.parse::<u64>() {
                            delay = segundos.min(10); // tope de seguridad
                        }
                    }
                    _ => {} // parámetro desconocido: se ignora
                }
            }
        }
    }

    let inicio = Instant::now();
    println!("{}    🧵 {hilo} tomó /tarea?id={id} · duerme {delay} s…", marca());
    thread::sleep(Duration::from_secs(delay));
    let ms = inicio.elapsed().as_millis();
    println!("{}    ✔ {hilo} terminó la tarea {id} en {ms} ms", marca());

    let cuerpo = format!(
        "{{\"id\":{id},\"delay\":{delay},\"hilo\":\"{hilo}\",\"servidor_ms\":{ms}}}"
    );
    enviar(
        flujo,
        200,
        "OK",
        "application/json; charset=utf-8",
        cuerpo.as_bytes(),
    );
}

/// Escribe una respuesta HTTP completa (cabeceras + cuerpo) y cierra la conexión.
fn enviar(flujo: &mut TcpStream, codigo: u16, mensaje: &str, tipo: &str, cuerpo: &[u8]) {
    let cabeceras = format!(
        "HTTP/1.1 {codigo} {mensaje}\r\n\
         Content-Type: {tipo}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        cuerpo.len()
    );
    let _ = flujo.write_all(cabeceras.as_bytes());
    let _ = flujo.write_all(cuerpo);
    let _ = flujo.flush();
}

/// Página de error mínima (404, etc.).
fn responder_error(flujo: &mut TcpStream, codigo: u16, titulo: &str, detalle: &str) {
    let cuerpo = format!(
        "<!DOCTYPE html><html lang=\"es\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>{codigo} · {titulo}</title></head>\
         <body style=\"font-family:system-ui,sans-serif;text-align:center;padding:4rem 1rem\">\
         <h1>{codigo} — {titulo}</h1><p>{detalle}</p>\
         <p><a href=\"/\">Volver al inicio</a></p></body></html>"
    );
    let mensaje = if codigo == 404 { "Not Found" } else { "Error" };
    enviar(
        flujo,
        codigo,
        mensaje,
        "text/html; charset=utf-8",
        cuerpo.as_bytes(),
    );
}

// ─────────────────────────────────────────────────────────────────────
// Grupo de hilos (pool de trabajadores)
// ─────────────────────────────────────────────────────────────────────

/// Una tarea: una función que un trabajador ejecutará una sola vez.
type Tarea = Box<dyn FnOnce() + Send + 'static>;

/// Lo que se envía por el canal a los trabajadores.
enum Mensaje {
    NuevaTarea(Tarea),
    Salir,
}

struct GrupoDeHilos {
    trabajadores: Vec<Trabajador>,
    emisor: mpsc::Sender<Mensaje>,
}

impl GrupoDeHilos {
    /// Crea el canal y lanza `tamano` hilos esperando tareas.
    fn new(tamano: usize) -> GrupoDeHilos {
        assert!(tamano > 0, "el grupo necesita al menos un hilo");

        let (emisor, receptor) = mpsc::channel::<Mensaje>();
        // El receptor se comparte entre todos los trabajadores:
        // Arc = propiedad compartida, Mutex = solo uno hace recv() a la vez.
        let receptor = Arc::new(Mutex::new(receptor));

        let mut trabajadores = Vec::with_capacity(tamano);
        for id in 0..tamano {
            trabajadores.push(Trabajador::new(id, Arc::clone(&receptor)));
        }

        GrupoDeHilos { trabajadores, emisor }
    }

    /// Encola una tarea; el primer trabajador libre la toma.
    fn ejecutar<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.emisor.send(Mensaje::NuevaTarea(Box::new(f)));
    }
}

struct Trabajador {
    hilo: Option<thread::JoinHandle<()>>,
}

impl Trabajador {
    fn new(id: usize, receptor: Arc<Mutex<mpsc::Receiver<Mensaje>>>) -> Trabajador {
        // El nombre del hilo ("worker-N") es el que la demo muestra como "hilo".
        let hilo = thread::Builder::new()
            .name(format!("worker-{id}"))
            .spawn(move || loop {
                
                let mensaje = {
                    let guardia = receptor.lock().expect("Mutex envenenado");
                    guardia.recv().expect("el canal se cerró inesperadamente")
                };

                match mensaje {
                    Mensaje::NuevaTarea(tarea) => tarea(),
                    Mensaje::Salir => {
                        println!("{}    🧵 worker-{id} se apaga", marca());
                        break;
                    }
                }
            })
            .expect("no se pudo crear el hilo trabajador");

        Trabajador { hilo: Some(hilo) }
    }
}

impl Drop for GrupoDeHilos {
    fn drop(&mut self) {
        // Apagado ordenado: se envía un Salir por cada trabajador y se espera a
        // que terminen las tareas que tengan en curso antes de cerrar el proceso.
        for _ in &self.trabajadores {
            let _ = self.emisor.send(Mensaje::Salir);
        }
        for trabajador in &mut self.trabajadores {
            if let Some(hilo) = trabajador.hilo.take() {
                let _ = hilo.join();
            }
        }
    }
}
