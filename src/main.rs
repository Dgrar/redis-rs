mod commands;
mod parser;
mod resp;
mod storage;

use anyhow::Result;
use bytes::{Buf, BytesMut};
use resp::RespValue;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use storage::Storage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> Result<()> {
    // Creamos un Tcp listener de tokio para poder enlazar puertos de forma asíncrona
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Servidor escuchando en 127.0.0.1:6379");

    let db: Storage = Arc::new(RwLock::new(HashMap::new()));
    let notify = Arc::new(Notify::new());

    loop {
        // Listener.accept() devuelve el stream y la localización de quien esta conectado
        let (socket, peer_addr) = listener.accept().await?;
        println!("Nueva conexión desde {peer_addr}");
        let client_db = Arc::clone(&db);
        let client_notify = notify.clone();
        // Creamos una green thread para cada cliente
        tokio::spawn(async move {
            // separamos responsabilidades de la conexion a manejar como va el cliente
            if let Err(e) = handle_client(socket, client_db, client_notify).await {
                eprintln!("Error con el cliente {peer_addr}: {e}");
            }
            println!("Conexión cerrada: {peer_addr}");
        });
    }
}

async fn handle_client(mut socket: TcpStream, db: Storage, notify: Arc<Notify>) -> Result<()> {
    // Creamos un buffer de 4096 bytes donde se almacenarán los comandos
    let mut buf = BytesMut::with_capacity(4096);
    // Loopeamos para poder recibir mas de 1 comando por thread
    loop {
        // Leemos los bits del buffer
        let bytes_read = socket.read_buf(&mut buf).await?;
        // Si no se lee nada, cerramos el socket
        if bytes_read == 0 {
            return Ok(());
        }

        // Parseamos los mensajes completos que hayan llegado al buffer
        loop {
            match parser::parse_message(&buf) {
                Ok(Some((command, consumed))) => {
                    buf.advance(consumed);

                    let response = commands::execute_command(command, &db, &notify).await;
                    socket.write_all(&response.serialize()).await?;
                }
                Ok(None) => break,
                Err(e) => {
                    let err_resp = RespValue::Error(format!("ERR {}", e));
                    socket.write_all(&err_resp.serialize()).await?;
                    return Ok(());
                }
            }
        }
    }
}
