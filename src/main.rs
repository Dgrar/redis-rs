use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    // Creamos un Tcp listener de tokio para poder enlazar puertos de forma asíncrona
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Servidor escuchando en 127.0.0.1:6379");

    loop {
        // Listener.accept() devuelve el stream y la localización de quien esta conectado
        let (socket, peer_addr) = listener.accept().await?;
        println!("Nueva conexión desde {peer_addr}");
        // Creamos una green thread para cada cliente
        tokio::spawn(async move {
            // separamos responsabilidades de la conexion a manejar como va el cliente
            if let Err(e) = handle_client(socket).await {
                eprintln!("Error con el cliente {peer_addr}: {e}");
            }
            println!("Conexión cerrada: {peer_addr}");
        });
    }
}

async fn handle_client(mut socket: TcpStream) -> Result<()> {
    // Creamos un buffer de 512 bytes donde se almacenarán los comandos
    let mut buf = [0; 512];
    // Loopeamos para poder recibir mas de 1 comando por thread
    loop {
        // Leemos los bits del buffer
        let bytes_read = socket.read(&mut buf).await?;
        // Si no se lee nada, cerramos el socket
        if bytes_read == 0 {
            return Ok(());
        }
        // Escribimos la respuesta
        socket.write_all(b"+PONG\r\n").await?;
    }
}
