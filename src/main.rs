use anyhow::Result;
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

use crate::RespValue::BulkString;

#[derive(Debug, PartialEq, Clone)]
enum RespValue {
    SimpleString(String),
    Error(String),
    Integers(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    fn serialize(&self) -> Vec<u8> {
        match self {
            Self::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Self::Error(e) => format!("-{}\r\n", e).into_bytes(),
            Self::Integers(i) => format!(":{}\r\n", i).into_bytes(),
            Self::BulkString(None) => b"$-1\r\n".to_vec(),
            Self::BulkString(Some(data)) => {
                let mut response = format!("${}\r\n", data.len()).into_bytes();
                response.extend_from_slice(data);
                response.extend_from_slice(b"\r\n");
                response
            }
            // CORREGIDO: Array nulo es *-1\r\n, no *0\r\n
            Self::Array(None) => b"*-1\r\n".to_vec(),
            Self::Array(Some(data)) => {
                let mut response = format!("*{}\r\n", data.len()).into_bytes();
                for element in data {
                    response.extend_from_slice(&element.serialize());
                }
                response
            }
        }
    }
}

pub fn parse_message(buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
    // CORREGIDO: Si está vacío, faltan datos (Ok(None)), no es un error
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        // CORREGIDO: Añadido el soporte para Arrays
        b'*' => parse_array(buf),
        _ => Err(format!("Byte inicial desconocido: {}", buf[0] as char)),
    }
}

fn read_until_crlf(buf: &[u8]) -> Option<(&[u8], usize)> {
    for i in 0..buf.len().saturating_sub(1) {
        // CORREGIDO: buf[i + 1] == b'\n'
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some((&buf[..i], i + 2));
        }
    }
    None
}

fn parse_simple_string(buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
    if let Some((line, len)) = read_until_crlf(&buf[1..]) {
        let s = String::from_utf8(line.to_vec()).map_err(|_| "UTF-8 inválido")?;
        return Ok(Some((RespValue::SimpleString(s), len + 1)));
    }
    Ok(None)
}

fn parse_error(buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
    if let Some((line, len)) = read_until_crlf(&buf[1..]) {
        let s = String::from_utf8(line.to_vec()).map_err(|_| "UTF-8 inválido")?;
        return Ok(Some((RespValue::Error(s), len + 1)));
    }
    Ok(None)
}

fn parse_integer(buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
    if let Some((line, len)) = read_until_crlf(&buf[1..]) {
        let s = std::str::from_utf8(line).map_err(|_| "UTF-8 inválido")?;
        let num = s.parse::<i64>().map_err(|_| "Integer inválido")?;
        return Ok(Some((RespValue::Integers(num), len + 1)));
    }
    Ok(None)
}

fn parse_bulk_string(buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
    let (line, header_len) = match read_until_crlf(&buf[1..]) {
        Some(res) => res,
        None => return Ok(None),
    };

    let len_str = std::str::from_utf8(line).map_err(|_| "UTF-8 inválido")?;
    let str_len = len_str.parse::<isize>().map_err(|_| "Longitud inválida")?;

    if str_len == -1 {
        return Ok(Some((RespValue::BulkString(None), header_len + 1)));
    }
    if str_len < 0 {
        return Err("Longitud negativa no válida".to_string());
    }

    let str_len = str_len as usize;
    let total_len = 1 + header_len + str_len + 2;

    if buf.len() < total_len {
        return Ok(None);
    }
    if &buf[total_len - 2..total_len] != b"\r\n" {
        return Err("BulkString no termina en CRLF".to_string());
    }

    let data = buf[1 + header_len..total_len - 2].to_vec();
    Ok(Some((RespValue::BulkString(Some(data)), total_len)))
}

fn parse_array(buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
    let (line, header_len) = match read_until_crlf(&buf[1..]) {
        Some(res) => res,
        None => return Ok(None),
    };

    let len_str = std::str::from_utf8(line).map_err(|_| "UTF-8 inválido")?;
    let count = len_str
        .parse::<isize>()
        .map_err(|_| "Longitud de array inválida")?;

    if count == -1 {
        return Ok(Some((RespValue::Array(None), header_len + 1)));
    }
    if count < 0 {
        return Err("Longitud de array negativa".to_string());
    }

    let mut elements = Vec::with_capacity(count as usize);
    let mut offset = 1 + header_len;

    for _ in 0..count {
        match parse_message(&buf[offset..])? {
            Some((val, consumed)) => {
                elements.push(val);
                offset += consumed;
            }
            None => return Ok(None),
        }
    }

    Ok(Some((RespValue::Array(Some(elements)), offset)))
}

async fn execute_command(command: RespValue) -> RespValue {
    match command {
        RespValue::Array(Some(elements)) => parse_array_command(elements).await,
        RespValue::Array(None) => RespValue::Error("ERR array nulo recibido".to_string()),
        _ => RespValue::Error("ERR se esperaba un Array RESP".to_string()),
    }
}

async fn parse_array_command(elements: Vec<RespValue>) -> RespValue {
    // 1. Separamos el primer elemento (el comando) del resto (los argumentos)
    let Some((first, args)) = elements.split_first() else {
        return RespValue::Error("ERR comando vacío".to_string());
    };

    // 2. Extraemos los bytes del nombre del comando
    let cmd_bytes = match first {
        RespValue::BulkString(Some(bytes)) => bytes,
        _ => {
            return RespValue::Error(
                "ERR el nombre del comando debe ser un BulkString".to_string(),
            );
        }
    };

    // 3. Normalizamos a mayúsculas para comparar (ej: "ping" -> b"PING")
    match cmd_bytes.to_ascii_uppercase().as_slice() {
        b"PING" => handle_ping(args).await,
        b"ECHO" => handle_echo(args).await,
        // b"SET"  => handle_set(args),
        // b"GET"  => handle_get(args),
        other => {
            let cmd_name = String::from_utf8_lossy(other);
            RespValue::Error(format!("ERR comando desconocido '{}'", cmd_name))
        }
    }
}

async fn handle_ping(args: &[RespValue]) -> RespValue {
    RespValue::SimpleString("PONG".to_string())
}

async fn handle_echo(args: &[RespValue]) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("Wrong number of arguments for command ECHO".to_string());
    }
    println!("{:?}", args);
    let message: String = match &args[0] {
        RespValue::SimpleString(s) => return RespValue::SimpleString(s.to_string()),
        RespValue::Integers(i) => return RespValue::Integers(*i),
        RespValue::BulkString(b) => {
            if let Some(data) = b {
                let string = String::from_utf8(data.to_owned());

                match string {
                    Ok(s) => {
                        println!("String recibida en echo: {s}");
                        return RespValue::BulkString(Some(s.into_bytes()));
                    }
                    Err(err) => return RespValue::Error(err.to_string()),
                }
            }
            return RespValue::BulkString(None);
        }
        _ => {
            return RespValue::Error("Datatipo incorrecto".to_string());
        }
    };
}

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
    let mut buf = BytesMut::with_capacity(512);
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
            println!("{}", String::from_utf8(buf.to_vec())?);
            match parse_message(&buf) {
                Ok(Some((command, consumed))) => {
                    buf.advance(consumed);

                    println!("Comando recibido: {:?}", command);

                    // Escribimos la respuesta
                    let response = execute_command(command).await;
                    println!("Respuesta cruda: {:?}", response);
                    println!(
                        "Respuesta serializada: {:?}",
                        String::from_utf8(response.serialize())
                    );
                    socket.write_all(&response.serialize()).await?
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

#[cfg(test)]
mod redis_tests {
    use super::*;

    mod serialization {
        use super::*;

        #[test]
        fn serialize_simple_str() {
            let value = RespValue::SimpleString("Hello, world!".to_string());
            assert_eq!(value.serialize(), b"+Hello, world!\r\n");
        }

        #[test]
        fn serialize_error() {
            let value = RespValue::Error("Error de memoria".to_string());
            assert_eq!(value.serialize(), b"-Error de memoria\r\n");
        }

        #[test]
        fn serialize_integer() {
            let value = RespValue::Integers(23212);
            assert_eq!(value.serialize(), b":23212\r\n");

            let negative = RespValue::Integers(-42);
            assert_eq!(negative.serialize(), b":-42\r\n");
        }

        #[test]
        fn serialize_bulk_string() {
            let value = RespValue::BulkString(Some(b"foobar".to_vec()));
            assert_eq!(value.serialize(), b"$6\r\nfoobar\r\n");

            let empty = RespValue::BulkString(Some(b"".to_vec()));
            assert_eq!(empty.serialize(), b"$0\r\n\r\n");

            let null = RespValue::BulkString(None);
            assert_eq!(null.serialize(), b"$-1\r\n");
        }

        #[test]
        fn serialize_array() {
            let arr = RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"clave".to_vec())),
                RespValue::BulkString(Some(b"valor".to_vec())),
            ]));
            assert_eq!(
                arr.serialize(),
                b"*3\r\n$3\r\nSET\r\n$5\r\nclave\r\n$5\r\nvalor\r\n"
            );

            let empty_arr = RespValue::Array(Some(vec![]));
            assert_eq!(empty_arr.serialize(), b"*0\r\n");

            let null_arr = RespValue::Array(None);
            assert_eq!(null_arr.serialize(), b"*-1\r\n");
        }
    }

    mod parsing {
        use super::*;

        #[test]
        fn parse_simple_string() {
            let input = b"+OK\r\n";
            let res = parse_message(input).unwrap();
            assert_eq!(res, Some((RespValue::SimpleString("OK".to_string()), 5)));
        }

        #[test]
        fn parse_error() {
            let input = b"-ERR unknown command\r\n";
            let res = parse_message(input).unwrap();
            assert_eq!(
                res,
                Some((RespValue::Error("ERR unknown command".to_string()), 22))
            );
        }

        #[test]
        fn parse_integer() {
            let input = b":1000\r\n";
            let res = parse_message(input).unwrap();
            assert_eq!(res, Some((RespValue::Integers(1000), 7)));

            let neg = b":-50\r\n";
            let res_neg = parse_message(neg).unwrap();
            assert_eq!(res_neg, Some((RespValue::Integers(-50), 6)));
        }

        #[test]
        fn parse_bulk_string() {
            let input = b"$6\r\nfoobar\r\n";
            let res = parse_message(input).unwrap();
            assert_eq!(
                res,
                Some((RespValue::BulkString(Some(b"foobar".to_vec())), 12))
            );

            // Bulk string con \r\n dentro de los propios datos (binario seguro)
            let binary_input = b"$8\r\nfoo\r\nbar\r\n";
            let res_bin = parse_message(binary_input).unwrap();
            assert_eq!(
                res_bin,
                Some((RespValue::BulkString(Some(b"foo\r\nbar".to_vec())), 14))
            );

            // Null Bulk String
            let null_input = b"$-1\r\n";
            let res_null = parse_message(null_input).unwrap();
            assert_eq!(res_null, Some((RespValue::BulkString(None), 5)));
        }

        #[test]
        fn parse_array() {
            let input = b"*2\r\n$4\r\nECHO\r\n$4\r\nHola\r\n";
            let res = parse_message(input).unwrap();
            let expected = RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"ECHO".to_vec())),
                RespValue::BulkString(Some(b"Hola".to_vec())),
            ]));
            assert_eq!(res, Some((expected, input.len())));

            // Null Array
            let null_arr = b"*-1\r\n";
            assert_eq!(
                parse_message(null_arr).unwrap(),
                Some((RespValue::Array(None), 5))
            );
        }

        #[test]
        fn parse_nested_arrays() {
            // Array que contiene otro array y un entero
            let input = b"*2\r\n*2\r\n:1\r\n:2\r\n+OK\r\n";
            let res = parse_message(input).unwrap();
            let expected = RespValue::Array(Some(vec![
                RespValue::Array(Some(vec![RespValue::Integers(1), RespValue::Integers(2)])),
                RespValue::SimpleString("OK".to_string()),
            ]));
            assert_eq!(res, Some((expected, input.len())));
        }

        #[test]
        fn parse_incomplete_messages() {
            // Buffer vacío
            assert_eq!(parse_message(b"").unwrap(), None);
            // Simple string sin terminar
            assert_eq!(parse_message(b"+OK\r").unwrap(), None);
            // Bulk string al que le faltan bytes del cuerpo
            assert_eq!(parse_message(b"$6\r\nomg").unwrap(), None);
            // Array al que le falta el segundo elemento
            assert_eq!(parse_message(b"*2\r\n$3\r\nGET\r\n").unwrap(), None);
        }

        #[test]
        fn parse_invalid_protocol() {
            // Byte inicial no reconocido
            assert!(parse_message(b"?invalid\r\n").is_err());
            // Entero no numérico
            assert!(parse_message(b":abc\r\n").is_err());
            // Longitud de bulk string negativa inválida
            assert!(parse_message(b"$-5\r\n").is_err());
            // Bulk string que no termina en \r\n después de leer su longitud
            assert!(parse_message(b"$3\r\nfooXX").is_err());
        }

        #[test]
        fn parse_multiple_messages_in_buffer() {
            // Simula pipelining (dos comandos pegados en el mismo paquete TCP)
            let buffer = b"+PING\r\n:42\r\n";

            let (first, consumed1) = parse_message(buffer).unwrap().unwrap();
            assert_eq!(first, RespValue::SimpleString("PING".to_string()));
            assert_eq!(consumed1, 7);

            let (second, consumed2) = parse_message(&buffer[consumed1..]).unwrap().unwrap();
            assert_eq!(second, RespValue::Integers(42));
            assert_eq!(consumed2, 5);
        }
    }
}
