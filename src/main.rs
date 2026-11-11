#![allow(unused_imports)]
use std::io::Write;
use std::net::TcpListener;
fn main() {
    let listener =
        TcpListener::bind("127.0.0.1:6379").expect("Hubo un error al conectar con el puerto");

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                _stream.write_all(b"+PONG\r\n").expect("Error de conexión");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
