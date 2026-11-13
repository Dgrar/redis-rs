use tokio::sync::Notify;

use crate::resp::RespValue;
use crate::storage::{Entry, Storage};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

type Anotify = Arc<Notify>;

pub async fn execute_command(command: RespValue, db: &Storage, notify: &Anotify) -> RespValue {
    match command {
        RespValue::Array(Some(elements)) => dispatch(elements, db, notify).await,
        RespValue::Array(None) => RespValue::Error("ERR array nulo recibido".to_string()),
        _ => RespValue::Error("ERR se esperaba un Array RESP".to_string()),
    }
}

async fn dispatch(elements: Vec<RespValue>, db: &Storage, notify: &Anotify) -> RespValue {
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
        b"RPUSH" => {
            let r = handle_rpush(args, db);
            notify.notify_waiters();
            r
        }
        b"LPUSH" => {
            let r = handle_lpush(args, db);
            notify.notify_waiters();
            r
        }
        b"LRANGE" => handle_lrange(args, db),
        b"LLEN" => handle_llen(args, db),
        b"LPOP" => handle_lpop(args, db),
        b"BLPOP" => handle_blpop(args, db, notify).await,
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

fn handle_rpush(args: &[RespValue], db: &Storage) -> RespValue {
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

        let len: [u8; 4] = (val.len() as u32).to_be_bytes();
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

fn handle_lpush(args: &[RespValue], db: &Storage) -> RespValue {
    if args.len() <= 1 {
        return RespValue::Error("ERR wrong number of arguments for 'lpush' command".to_string());
    }

    let RespValue::BulkString(Some(key)) = &args[0] else {
        return RespValue::Error("ERR wrong number of arguments for 'lpush' command".to_string());
    };

    let mut map = match db.write() {
        Ok(guard) => guard,
        Err(e) => return RespValue::Error(format!("ERR {}", e)),
    };

    println!("Entry");

    let entry = map.entry(key.to_vec()).or_insert_with(|| Entry {
        value: vec![LIST_TYPE],
        expires_at: None,
    });

    if entry.value.first() != Some(&LIST_TYPE) {
        return RespValue::Error(
            "ERR Operation against a key holding the wrong kind of value".to_string(),
        );
    }

    let mut parsed_vals = Vec::with_capacity(args.len() - 1);
    let mut extra_bytes = 0;
    for argument in &args[1..] {
        let RespValue::BulkString(Some(val)) = argument else {
            return RespValue::Error("ERR key and value must be BulkString".to_string());
        };
        extra_bytes += 4 + val.len();
        parsed_vals.push(val);
    }

    let mut new_vec = Vec::with_capacity(entry.value.len() + extra_bytes);

    new_vec.push(LIST_TYPE);

    for value in parsed_vals.into_iter().rev() {
        let len = (value.len() as i32).to_be_bytes();
        new_vec.extend_from_slice(&len);
        new_vec.extend_from_slice(value);
    }
    new_vec.extend_from_slice(&entry.value[1..]);
    entry.value = new_vec;

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

fn handle_lpop(args: &[RespValue], db: &Storage) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::Error("ERR wrong number of arguments for 'lpop' command".to_string());
    }

    let RespValue::BulkString(Some(key)) = &args[0] else {
        return RespValue::Error("ERR wrong number of arguments for 'lpop' command".to_string());
    };

    let has_count_arg = args.len() == 2;
    let count_target: usize = if has_count_arg {
        let RespValue::BulkString(Some(val)) = &args[1] else {
            return RespValue::Error("ERR value is not an integer or out of range".to_string());
        };
        let Ok(num) = String::from_utf8_lossy(val).parse::<usize>() else {
            return RespValue::Error("ERR value is not an integer or out of range".to_string());
        };
        num
    } else {
        1
    };

    let mut map = match db.write() {
        Ok(guard) => guard,
        Err(e) => return RespValue::Error(format!("ERR {}", e)),
    };

    let Some(entry) = map.get_mut(key) else {
        return if has_count_arg {
            RespValue::Array(None)
        } else {
            RespValue::BulkString(None)
        };
    };

    if entry.value.first() != Some(&LIST_TYPE) {
        return RespValue::Error(
            "ERR Operation against a key holding the wrong kind of value".to_string(),
        );
    }

    if has_count_arg && count_target == 0 {
        return RespValue::Array(Some(vec![]));
    }

    let mut popped_count = 0;
    let mut cursor = 1;
    let mut items_popped: Vec<Vec<u8>> = Vec::new();

    while popped_count < count_target && cursor + 4 <= entry.value.len() {
        let len_bytes: [u8; 4] = match entry.value[cursor..cursor + 4].try_into() {
            Ok(len) => len,
            Err(_) => return RespValue::Error("ERR could not get the entry slice".to_string()),
        };
        let elem_len = u32::from_be_bytes(len_bytes) as usize;

        let Some(data_slice) = entry.value.get(cursor + 4..cursor + 4 + elem_len) else {
            break;
        };
        items_popped.push(data_slice.to_vec());
        cursor += 4 + elem_len;
        popped_count += 1;
    }

    if cursor >= entry.value.len() {
        map.remove(key);
    } else {
        entry.value.copy_within(cursor.., 1);
        entry.value.truncate(1 + (entry.value.len() - cursor));
    }

    if !has_count_arg {
        RespValue::BulkString(items_popped.into_iter().next())
    } else {
        let values = items_popped
            .into_iter()
            .map(|el| RespValue::BulkString(Some(el)))
            .collect();
        RespValue::Array(Some(values))
    }
}

async fn handle_blpop(args: &[RespValue], db: &Storage, notify: &Anotify) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'blpop' command".to_string());
    }
    let (timeout_arg, keys) = args.split_last().unwrap();

    let timeout_secs = match timeout_arg {
        RespValue::BulkString(Some(t)) => match String::from_utf8_lossy(&t).parse::<f64>() {
            Ok(t) if t.is_finite() && t >= 0.0 => t,
            Ok(t) if t < 0.0 => return RespValue::Error("ERR timeout is negative".to_string()),
            _ => return RespValue::Error("ERR timeout is not a float or out of range".to_string()),
        },
        _ => return RespValue::Error("ERR timeout is not a float or out of range".to_string()),
    };

    let mut key_bytes: Vec<&[u8]> = Vec::with_capacity(keys.len());

    for k in keys {
        let RespValue::BulkString(Some(key)) = k else {
            return RespValue::Error("ERR BLPOP keys must be BulkString".to_string());
        };
        key_bytes.push(key);
    }
    let deadline = (timeout_secs > 0.0)
        .then(|| tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs));

    loop {
        if let Some(r_val) = try_pop_keys(&key_bytes, db) {
            return r_val;
        }

        let notified = notify.notified();

        tokio::pin!(notified);

        notified.as_mut().enable();

        if let Some(r_val) = try_pop_keys(&key_bytes, db) {
            return r_val;
        }

        match deadline {
            Some(dur) => {
                let remaining = dur.saturating_duration_since(tokio::time::Instant::now());
                if tokio::time::timeout(remaining, &mut notified)
                    .await
                    .is_err()
                {
                    return RespValue::Array(None);
                }
            }
            None => notified.await,
        }
    }
}

fn pop_left(key: &[u8], db: &Storage) -> Result<Option<Vec<u8>>, RespValue> {
    let mut map = db
        .write()
        .map_err(|e| RespValue::Error(format!("ERR {e}")))?;
    let Some(entry) = map.get_mut(key) else {
        return Ok(None);
    };
    if entry.value.first() != Some(&LIST_TYPE) {
        return Err(RespValue::Error(
            "ERR Operation against a key holding the wrong kind of value".into(),
        ));
    }
    if entry.value.len() < 5 {
        return Ok(None);
    }
    let len = u32::from_be_bytes(entry.value[1..5].try_into().unwrap()) as usize;
    let Some(slice) = entry.value.get(5..5 + len) else {
        return Ok(None);
    };
    let data = slice.to_vec();
    let cursor = 5 + len;
    if cursor >= entry.value.len() {
        map.remove(key);
    } else {
        entry.value.copy_within(cursor.., 1);
        entry.value.truncate(entry.value.len() - (cursor - 1));
    }
    Ok(Some(data))
}

fn try_pop_keys(keys: &[&[u8]], db: &Storage) -> Option<RespValue> {
    for &key in keys {
        match pop_left(key, db) {
            Ok(Some(value)) => {
                return Some(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(key.to_vec())),
                    RespValue::BulkString(Some(value)),
                ])));
            }
            Ok(None) => continue,
            Err(err) => return Some(err), // WRONGTYPE se propaga
        }
    }
    None
}

fn handle_lrange(args: &[RespValue], db: &Storage) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'lrange' command".to_string());
    }

    let RespValue::BulkString(Some(key)) = &args[0] else {
        return RespValue::Error("ERR wrong number of arguments for 'lrange' command".to_string());
    };

    let (RespValue::BulkString(Some(start)), RespValue::BulkString(Some(end))) =
        (&args[1], &args[2])
    else {
        return RespValue::Error("ERR wrong number of arguments for 'lrange' command".to_string());
    };

    let map = match db.read() {
        Ok(guard) => guard,
        Err(e) => return RespValue::Error(format!("ERR {}", e)),
    };

    let Some(entry) = map.get(key) else {
        return RespValue::Array(Some(vec![]));
    };

    let Ok(end_num) = String::from_utf8_lossy(end).parse::<isize>() else {
        return RespValue::Error("ERR the end value is not an integer".to_string());
    };

    let Ok(start_num) = String::from_utf8_lossy(start).parse::<isize>() else {
        return RespValue::Error("ERR the end value is not an integer".to_string());
    };
    let mut count = 0;
    let mut cursor = 1;

    while cursor + 4 <= entry.value.len() {
        let len_bytes: [u8; 4] = match entry.value[cursor..cursor + 4].try_into() {
            Ok(val) => val,
            Err(_) => return RespValue::Error("ERR could not get the entry slice".to_string()),
        };
        let elem_len = u32::from_be_bytes(len_bytes) as usize;
        cursor += 4 + elem_len;
        count += 1;
    }

    let end_num = if end_num <= -1 {
        count + end_num
    } else {
        end_num
    };
    let start_num = if start_num <= -1 {
        count + start_num
    } else {
        start_num
    };

    if start_num > end_num {
        return RespValue::Array(Some(vec![]));
    }

    let mut elements: Vec<&[u8]> = Vec::new();
    cursor = 1;
    count = 0;
    while cursor + 4 <= entry.value.len() && count <= end_num {
        let len_bytes: [u8; 4] = match entry.value[cursor..cursor + 4].try_into() {
            Ok(len) => len,
            Err(_) => return RespValue::Error("ERR could not get the entry slice".to_string()),
        };
        let elem_len = u32::from_be_bytes(len_bytes) as usize;
        if count >= start_num {
            elements.push(match entry.value.get(cursor + 4..cursor + 4 + elem_len) {
                Some(b) => b,
                None => break,
            });
        }
        cursor += 4 + elem_len;
        count += 1
    }
    let values = elements
        .into_iter()
        .map(|el| RespValue::BulkString(Some(el.to_vec())))
        .collect();
    println!("Values: {:?}", values);
    RespValue::Array(Some(values))
}

fn handle_llen(args: &[RespValue], db: &Storage) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'lrange' command".to_string());
    }

    let RespValue::BulkString(Some(key)) = &args[0] else {
        return RespValue::Error("ERR wrong number of arguments for 'lrange' command".to_string());
    };

    let map = match db.read() {
        Ok(guard) => guard,
        Err(e) => return RespValue::Error(format!("ERR {}", e)),
    };

    let Some(entry) = map.get(key) else {
        return RespValue::Integers(0);
    };

    let mut count = 0;
    let mut cursor = 1;

    while cursor + 4 <= entry.value.len() {
        let len_bytes: [u8; 4] = match entry.value[cursor..cursor + 4].try_into() {
            Ok(val) => val,
            Err(_) => return RespValue::Error("ERR could not get the entry slice".to_string()),
        };
        let elem_len = u32::from_be_bytes(len_bytes) as usize;
        cursor += 4 + elem_len;
        count += 1;
    }

    RespValue::Integers(count)
}
