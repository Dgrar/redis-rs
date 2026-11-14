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
        b"TYPE" => handle_type(args, db),
        b"XADD" => handle_xadd(args, db),
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
fn handle_type(args: &[RespValue], db: &Storage) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'type' command".to_string());
    }

    let RespValue::BulkString(Some(key)) = &args[0] else {
        return RespValue::Error("ERR wrong number of arguments for 'lrange' command".to_string());
    };

    let map = match db.read() {
        Ok(guard) => guard,
        Err(e) => return RespValue::Error(format!("ERR {}", e)),
    };

    let Some(entry) = map.get(key) else {
        return RespValue::SimpleString("none".to_string());
    };

    match entry.value.first() {
        Some(&LIST_TYPE) => RespValue::SimpleString("list".to_string()),
        Some(&STREAM_TYPE) => RespValue::SimpleString("stream".to_string()),
        _ => RespValue::SimpleString("string".to_string()),
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct StreamId {
    ms: u64,
    seq: u64,
}

impl StreamId {
    fn to_bytes(self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.ms.to_be_bytes());
        buf[8..16].copy_from_slice(&self.seq.to_be_bytes());
        buf
    }

    fn from_bytes(buf: &[u8]) -> Self {
        let ms = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let seq = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        Self { ms, seq }
    }
}

const STREAM_TYPE: u8 = 0x03;

/// Cabecera fija de un stream: [tipo 1][nº entradas u32 BE][último ID: ms u64 BE + seq u64 BE]
const STREAM_HEADER_LEN: usize = 21;

fn handle_xadd(args: &[RespValue], db: &Storage) -> RespValue {
    // key + id + pares campo/valor => longitud par y al menos 4
    if args.len() < 4 || !args.len().is_multiple_of(2) {
        return RespValue::Error("ERR wrong number of arguments for 'xadd' command".to_string());
    }

    let RespValue::BulkString(Some(key)) = &args[0] else {
        return RespValue::Error("ERR wrong number of arguments for 'xadd' command".to_string());
    };

    let RespValue::BulkString(Some(raw_id)) = &args[1] else {
        return stream_id_error_message();
    };

    let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity((args.len() - 2) / 2);
    for pair in args[2..].as_chunks::<2>().0 {
        let (RespValue::BulkString(Some(field)), RespValue::BulkString(Some(value))) =
            (&pair[0], &pair[1])
        else {
            return RespValue::Error("ERR fields and values must be BulkString".to_string());
        };
        pairs.push((field, value));
    }

    let mut map = match db.write() {
        Ok(guard) => guard,
        Err(e) => return RespValue::Error(format!("ERR {}", e)),
    };

    let last = match map.get(key) {
        Some(entry) => {
            if entry.value.first() != Some(&STREAM_TYPE) {
                return RespValue::Error(
                    "ERR Operation against a key holding the wrong kind of value".to_string(),
                );
            }
            if entry.value.len() < STREAM_HEADER_LEN {
                return RespValue::Error("ERR invalid stream encoding".to_string());
            }
            let count = u32::from_be_bytes(entry.value[1..5].try_into().unwrap());
            (count != 0).then(|| StreamId::from_bytes(&entry.value[5..21]))
        }
        None => None,
    };

    let id = match parse_xadd_id(raw_id, last) {
        Ok(id) => id,
        Err(err) => return err,
    };

    if id.ms == 0 && id.seq == 0 {
        return RespValue::Error(
            "ERR The ID specified in 'xadd' must be greater than 0-0".to_string(),
        );
    }

    if let Some(previous) = last
        && id <= previous
    {
        return RespValue::Error(
            "ERR The ID specified in 'xadd' is equal or smaller than the target stream top item"
                .to_string(),
        );
    }

    let entry = map.entry(key.to_vec()).or_insert_with(|| Entry {
        value: create_stream_header(),
        expires_at: None,
    });

    // Entrada: [ID 16][nº pares u32][ (len campo, campo) (len valor, valor) ]...
    entry.value.extend_from_slice(&id.to_bytes());
    entry
        .value
        .extend_from_slice(&(pairs.len() as u32).to_be_bytes());
    for (field, value) in &pairs {
        entry
            .value
            .extend_from_slice(&(field.len() as u32).to_be_bytes());
        entry.value.extend_from_slice(field);
        entry
            .value
            .extend_from_slice(&(value.len() as u32).to_be_bytes());
        entry.value.extend_from_slice(value);
    }

    let count = u32::from_be_bytes(entry.value[1..5].try_into().unwrap());
    entry.value[1..5].copy_from_slice(&(count + 1).to_be_bytes());
    entry.value[5..21].copy_from_slice(&id.to_bytes());

    RespValue::BulkString(Some(format!("{}-{}", id.ms, id.seq).into_bytes()))
}

fn stream_id_error_message() -> RespValue {
    RespValue::Error("ERR Invalid stream ID specified as stream command argument".to_string())
}

fn parse_xadd_id(raw: &[u8], last: Option<StreamId>) -> Result<StreamId, RespValue> {
    if raw == b"*" {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0);
        // Si el reloj retrocede (o empata), seguimos siendo estrictamente crecientes
        return Ok(match last {
            Some(previous) if now_ms <= previous.ms => StreamId {
                ms: previous.ms,
                seq: previous.seq + 1,
            },
            _ => StreamId { ms: now_ms, seq: 0 },
        });
    }

    let text = String::from_utf8_lossy(raw);
    let (ms_part, seq_part) = text.split_once('-').ok_or_else(stream_id_error_message)?;
    let ms: u64 = ms_part.parse().map_err(|_| stream_id_error_message())?;
    let seq: u64 = if seq_part == "*" {
        match last {
            Some(previous) if previous.ms == ms => previous.seq + 1,
            _ => 0,
        }
    } else {
        seq_part.parse().map_err(|_| stream_id_error_message())?
    };

    Ok(StreamId { ms, seq })
}

fn create_stream_header() -> Vec<u8> {
    let mut vec = vec![STREAM_TYPE];
    vec.extend_from_slice(&0u32.to_be_bytes());
    vec.extend_from_slice(&[0u8; 16]);
    vec
}
