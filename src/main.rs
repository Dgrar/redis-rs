#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;
fn main() {
    let listener =
        TcpListener::bind("127.0.0.1:6379").expect("Hubo un error al conectar con el puerto");

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                let mut buf = [0; 512];

                loop {
                    let bytes_read = _stream.read(&mut buf).expect("Error al leer el input");
                    if bytes_read == 0 {
                        println!("El comando no puede estar vacío");
                        break;
                    }

                    _stream.write_all(b"+PONG\r\n").expect("Error de conexión");
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
