use crate::resp::RespValue;

pub fn parse_message(buf: &[u8]) -> Result<Option<(RespValue, usize)>, String> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        _ => Err(format!("Byte inicial desconocido: {}", buf[0] as char)),
    }
}

fn read_until_crlf(buf: &[u8]) -> Option<(&[u8], usize)> {
    for i in 0..buf.len().saturating_sub(1) {
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

#[cfg(test)]
mod tests {
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
