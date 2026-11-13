use crate::resp::RespValue;
use crate::storage::{Entry, Storage};
use std::time::{Duration, SystemTime};

pub fn execute_command(command: RespValue, db: &Storage) -> RespValue {
    match command {
        RespValue::Array(Some(elements)) => dispatch(elements, db),
        RespValue::Array(None) => RespValue::Error("ERR array nulo recibido".to_string()),
        _ => RespValue::Error("ERR se esperaba un Array RESP".to_string()),
    }
}

fn dispatch(elements: Vec<RespValue>, db: &Storage) -> RespValue {
    let Some((first, args)) = elements.split_first() else {
        return RespValue::Error("ERR comando vacío".to_string());
    };

    let RespValue::BulkString(Some(cmd_bytes)) = first else {
        return RespValue::Error("ERR el nombre del comando debe ser un BulkString".to_string());
    };

    match cmd_bytes.to_ascii_uppercase().as_slice() {
        b"PING" => handle_ping(args),
        b"ECHO" => handle_echo(args),
        b"SET" => handle_set(args, db),
        b"GET" => handle_get(args, db),
        b"RPUSH" => handle_rpush(args, db),
        other => RespValue::Error(format!(
            "ERR comando desconocido '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

fn handle_ping(_args: &[RespValue]) -> RespValue {
    RespValue::SimpleString("PONG".to_string())
}

fn handle_echo(args: &[RespValue]) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'echo' command".to_string());
    }
    match &args[0] {
        RespValue::BulkString(data) => RespValue::BulkString(data.clone()),
        RespValue::SimpleString(s) => RespValue::SimpleString(s.clone()),
        RespValue::Integers(i) => RespValue::Integers(*i),
        _ => RespValue::Error("ERR invalid data type".to_string()),
    }
}

fn handle_set(args: &[RespValue], db: &Storage) -> RespValue {
    if ![2, 4].contains(&args.len()) {
        return RespValue::Error("ERR wrong number of arguments for 'set' command".to_string());
    }

    let (RespValue::BulkString(Some(key)), RespValue::BulkString(Some(val))) = (&args[0], &args[1])
    else {
        return RespValue::Error("ERR key and value must be BulkString".to_string());
    };

    let expires_at = if args.len() == 4 {
        match parse_expiration(&args[2], &args[3]) {
            Ok(deadline) => Some(deadline),
            Err(err) => return err,
        }
    } else {
        None
    };

    let mut map = match db.write() {
        Ok(guard) => guard,
        Err(err) => return RespValue::Error(format!("ERR {}", err)),
    };

    map.insert(key.clone(), Entry::new(val.clone(), expires_at));
    RespValue::SimpleString("OK".to_string())
}

fn parse_expiration(exp_type: &RespValue, exp_num: &RespValue) -> Result<SystemTime, RespValue> {
    let RespValue::BulkString(Some(exp_type_bytes)) = exp_type else {
        return Err(RespValue::Error("ERR syntax error".to_string()));
    };

    let exp_num: i64 = match exp_num {
        RespValue::Integers(n) => *n,
        RespValue::BulkString(Some(bytes)) => match String::from_utf8_lossy(bytes).parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                return Err(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ));
            }
        },
        _ => {
            return Err(RespValue::Error(
                "ERR value is not an integer or out of range".to_string(),
            ));
        }
    };

    // Redis no permite tiempos de expiración menores o iguales a 0
    if exp_num <= 0 {
        return Err(RespValue::Error(
            "ERR invalid expire time in 'set' command".to_string(),
        ));
    }

    let duration = match String::from_utf8_lossy(exp_type_bytes)
        .to_lowercase()
        .as_str()
    {
        "px" => Duration::from_millis(exp_num as u64),
        "ex" => Duration::from_secs(exp_num as u64),
        _ => return Err(RespValue::Error("ERR syntax error".to_string())),
    };

    SystemTime::now()
        .checked_add(duration)
        .ok_or_else(|| RespValue::Error("ERR invalid expire time in 'set' command".to_string()))
}

fn handle_get(args: &[RespValue], db: &Storage) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'get' command".to_string());
    }

    let RespValue::BulkString(Some(key)) = &args[0] else {
        return RespValue::Error("ERR wrong number of arguments for 'get' command".to_string());
    };

    let map = match db.read() {
        Ok(guard) => guard,
        Err(err) => return RespValue::Error(format!("ERR {}", err)),
    };

    match map.get(key) {
        Some(entry) if !entry.is_expired() => RespValue::BulkString(Some(entry.value.clone())),
        // Clave inexistente o expirada: Redis responde con un BulkString nulo
        _ => RespValue::BulkString(None),
    }
}

const LIST_TYPE: u8 = 0x02;

pub fn handle_rpush(args: &[RespValue], db: &Storage) -> RespValue {
    if args.len() <= 1 {
        return RespValue::Error("ERR wrong number of arguments for 'rpush' command".to_string());
    }

    let RespValue::BulkString(Some(key)) = &args[0] else {
        return RespValue::Error("ERR wrong number of arguments for 'rpush' command".to_string());
    };

    let mut map = match db.write() {
        Ok(guard) => guard,
        Err(e) => return RespValue::Error(format!("ERR {}", e)),
    };

    let entry = map.entry(key.to_vec()).or_insert_with(|| Entry {
        value: vec![LIST_TYPE],
        expires_at: None,
    });

    if entry.value.first() != Some(&LIST_TYPE) {
        return RespValue::Error(
            "ERR Operation against a key holding the wrong kind of value".to_string(),
        );
    }

    for argument in &args[1..] {
        let RespValue::BulkString(Some(val)) = argument else {
            return RespValue::Error("ERR key and value must be BulkString".to_string());
        };

        let len = (val.len() as u32).to_be_bytes();
        entry.value.extend_from_slice(&len);
        entry.value.extend_from_slice(val);
    }

    let mut count = 0;
    let mut cursor = 1;

    while cursor + 4 <= entry.value.len() {
        let len_bytes: [u8; 4] = match entry.value[cursor..cursor + 4].try_into() {
            Ok(len) => len,
            Err(_) => return RespValue::Error("ERR could not get the entry slice".to_string()),
        };
        let elem_len = u32::from_be_bytes(len_bytes) as usize;

        cursor += 4 + elem_len;
        count += 1
    }

    RespValue::Integers(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    fn new_db() -> Storage {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn cmd(parts: &[&str]) -> RespValue {
        RespValue::Array(Some(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Some(p.as_bytes().to_vec())))
                .collect(),
        ))
    }

    #[test]
    fn ping_returns_pong() {
        let db = new_db();
        let res = execute_command(cmd(&["PING"]), &db);
        assert_eq!(res, RespValue::SimpleString("PONG".to_string()));
    }

    #[test]
    fn echo_returns_argument() {
        let db = new_db();
        let res = execute_command(cmd(&["ECHO", "hola"]), &db);
        assert_eq!(res, RespValue::BulkString(Some(b"hola".to_vec())));
    }

    #[test]
    fn set_and_get_roundtrip() {
        let db = new_db();
        let set = execute_command(cmd(&["SET", "clave", "valor"]), &db);
        assert_eq!(set, RespValue::SimpleString("OK".to_string()));

        let get = execute_command(cmd(&["GET", "clave"]), &db);
        assert_eq!(get, RespValue::BulkString(Some(b"valor".to_vec())));
    }

    #[test]
    fn get_missing_key_returns_null_bulk_string() {
        let db = new_db();
        let res = execute_command(cmd(&["GET", "no-existe"]), &db);
        assert_eq!(res, RespValue::BulkString(None));
    }

    #[test]
    fn get_expired_key_returns_null_bulk_string() {
        let db = new_db();
        let deadline = SystemTime::now() - Duration::from_secs(1);
        db.write().unwrap().insert(
            b"vieja".to_vec(),
            Entry::new(b"valor".to_vec(), Some(deadline)),
        );

        let res = execute_command(cmd(&["GET", "vieja"]), &db);
        assert_eq!(res, RespValue::BulkString(None));
    }

    #[test]
    fn set_with_expiration_in_the_past() {
        let db = new_db();
        let res = execute_command(cmd(&["SET", "k", "v", "PX", "50"]), &db);
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));
        std::thread::sleep(Duration::from_millis(80));
        let get = execute_command(cmd(&["GET", "k"]), &db);
        assert_eq!(get, RespValue::BulkString(None));
    }

    #[test]
    fn unknown_command_returns_error() {
        let db = new_db();
        let res = execute_command(cmd(&["FOO"]), &db);
        assert!(matches!(res, RespValue::Error(_)));
    }
}
