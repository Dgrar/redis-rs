#![allow(unused_imports)]
use std::net::TcpListener;

fn main() {
    let listener =
        TcpListener::bind("127.0.0.1:6379").expect("Hubo un error al conectar con el puerto");

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    // Uncomment the code below to pass the first stage
    //
    // let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    //
    // for stream in listener.incoming() {
    //     match stream {
    //         Ok(_stream) => {
    //             println!("accepted new connection");
    //         }
    //         Err(e) => {
    //             println!("error: {}", e);
    //         }
    //     }
    // }
}
