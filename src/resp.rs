#[derive(Debug, PartialEq, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integers(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn serialize(&self) -> Vec<u8> {
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
            Self::Array(None) => b"*0\r\n".to_vec(),
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

#[cfg(test)]
mod tests {
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
