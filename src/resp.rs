use crate::command::Command;
use anyhow::anyhow;
use bytes::Buf;
use std::io::Cursor;
use std::string::FromUtf8Error;

#[derive(PartialEq, Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    SimpleError(String),
    Integer(i64),
    Array(Vec<RespValue>),
    NullArray,
}

#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    Other(anyhow::Error),
}

impl From<FromUtf8Error> for ParseError {
    fn from(value: FromUtf8Error) -> Self {
        ParseError::Other(anyhow!("invalid utf8 {:?}", value))
    }
}

impl RespValue {
    pub fn parse_next(data: &mut Cursor<&[u8]>) -> Result<RespValue, ParseError> {
        if !data.has_remaining() {
            return Err(ParseError::Incomplete);
        }

        match data.get_u8() {
            b'+' => {
                let s = get_line(data)?;
                Ok(RespValue::SimpleString(String::from_utf8(s.to_vec())?))
            }
            b'-' => {
                let s = get_line(data)?;
                Ok(RespValue::SimpleError(String::from_utf8(s.to_vec())?))
            }
            b'$' => {
                let line = get_line(data)?;
                if *line == *b"-1" {
                    Ok(RespValue::NullBulkString)
                } else {
                    let s = get_line(data)?;
                    Ok(RespValue::BulkString(String::from_utf8(s.to_vec())?))
                }
            }
            b':' => {
                let s = get_line(data)?;
                let i = str::from_utf8(s)
                    .map_err(|e| ParseError::Other(e.into()))?
                    .parse::<i64>()
                    .map_err(|e| ParseError::Other(e.into()))?;
                Ok(RespValue::Integer(i))
            }

            b'*' => {
                let line = get_line(data)?;
                if *line == *b"-1" {
                    Ok(RespValue::NullArray)
                } else {
                    let len: usize = str::from_utf8(line)
                        .map_err(|e| ParseError::Other(e.into()))?
                        .parse::<usize>()
                        .map_err(|e| ParseError::Other(e.into()))?;

                    let mut array = Vec::new();
                    while array.len() < len {
                        let next = Self::parse_next(data)?;

                        array.push(next);
                    }

                    Ok(RespValue::Array(array))
                }
            }

            _ => todo!(),
        }
    }
}

fn get_line<'a>(data: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ParseError> {
    let start = data.position() as usize;
    let crlf_start_index = data.get_ref()[start..]
        .windows(2)
        .position(|x| x == b"\r\n")
        .ok_or_else(|| ParseError::Incomplete)?
        + start;

    data.set_position((crlf_start_index + 2) as u64);
    Ok(&data.get_ref()[start..crlf_start_index])
}

impl TryFrom<Command> for RespValue {
    type Error = anyhow::Error;

    fn try_from(value: Command) -> Result<Self, Self::Error> {
        match value {
            Command::Ping => Ok(RespValue::Array(vec![RespValue::BulkString(String::from(
                "PING",
            ))])),
            Command::Echo(s) => Ok(RespValue::Array(vec![
                RespValue::BulkString(String::from("ECHO")),
                RespValue::BulkString(s),
            ])),
            Command::Set {
                key,
                value,
                expiry_duration,
            } => {
                if expiry_duration.is_some() {
                    Err(anyhow!("Respvalue from command not fully implemented"))
                } else {
                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(String::from("SET")),
                        RespValue::BulkString(key),
                        RespValue::BulkString(value),
                    ]))
                }
            }
            _ => Err(anyhow!("Respvalue from command not fully implemented")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn parse_next(bytes: Bytes) -> Result<RespValue, ParseError> {
        let mut cursor = Cursor::new(&bytes[..]);
        RespValue::parse_next(&mut cursor)
    }

    #[test]
    fn parse_bulk_strings() {
        let s = Bytes::from("$-1\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::NullBulkString);

        let s = Bytes::from("$0\r\n\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::BulkString("".into()));

        let s = Bytes::from("$5\r\nhello\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::BulkString("hello".into()));
    }

    #[test]
    fn parse_array_with_mixed_types() {
        let s =
            Bytes::from("*4\r\n*3\r\n:1\r\n:2\r\n:3\r\n$-1\r\n*-1\r\n*2\r\n+Hello\r\n-World\r\n");
        let result = parse_next(s);
        assert_eq!(
            result.unwrap(),
            RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::Integer(1),
                    RespValue::Integer(2),
                    RespValue::Integer(3),
                ]),
                RespValue::NullBulkString,
                RespValue::NullArray,
                RespValue::Array(vec![
                    RespValue::SimpleString("Hello".into()),
                    RespValue::SimpleError("World".into())
                ])
            ]),
        );
    }

    #[test]
    fn parse_simple_string() {
        let s = Bytes::from("+OK\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".into()));
    }

    #[test]
    fn parse_bulk_string() {
        let s = Bytes::from("$5\r\nhello\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::BulkString("hello".into()));
    }

    #[test]
    fn parse_empty_string() {
        let s = Bytes::from("$0\r\n\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::BulkString("".into()));
    }

    #[test]
    fn parse_simple_error() {
        let s = Bytes::from("-this is an error message\r\n");
        let result = parse_next(s);
        assert_eq!(
            result.unwrap(),
            RespValue::SimpleError("this is an error message".into())
        );
    }

    #[test]
    fn parse_integer() {
        let s = Bytes::from(":0\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::Integer(0));

        let s = Bytes::from(":1000\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::Integer(1000));

        let s = Bytes::from(":-1\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::Integer(-1));

        let s = Bytes::from(":-1000\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::Integer(-1000));

        let s = Bytes::from(":+0\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::Integer(0));

        let s = Bytes::from(":+1000\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::Integer(1000));
    }

    #[test]
    fn parse_null_bulk_string() {
        let s = Bytes::from("$-1\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::NullBulkString);
    }

    #[test]
    fn parse_empty_array() {
        let s = Bytes::from("*0\r\n");
        let result = parse_next(s);
        assert_eq!(result.unwrap(), RespValue::Array(vec![]));
    }

    #[test]
    fn parse_array() {
        let s = Bytes::from("*2\r\n:1\r\n:2\r\n");
        let result = parse_next(s);
        assert_eq!(
            result.unwrap(),
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)])
        );
    }

    #[test]
    fn returns_incomplete_if_partial_frame_read() {
        let s = Bytes::from("");
        let result = parse_next(s);
        assert!(matches!(result.unwrap_err(), ParseError::Incomplete));

        let s = Bytes::from(":0\r");
        let result = parse_next(s);
        assert!(matches!(result.unwrap_err(), ParseError::Incomplete));

        let s = Bytes::from(":1000");
        let result = parse_next(s);
        assert!(matches!(result.unwrap_err(), ParseError::Incomplete));

        let s = Bytes::from(":-1");
        let result = parse_next(s);
        assert!(matches!(result.unwrap_err(), ParseError::Incomplete));

        let s = Bytes::from("$0\r\n");
        let result = parse_next(s);
        assert!(matches!(result.unwrap_err(), ParseError::Incomplete));

        let s = Bytes::from("$5\r\nhello");
        let result = parse_next(s);
        assert!(matches!(result.unwrap_err(), ParseError::Incomplete));
    }

    #[test]
    fn command_to_resp() {
        let command = Command::Ping;
        assert_eq!(
            RespValue::try_from(command).unwrap(),
            RespValue::Array(vec![RespValue::BulkString(String::from("PING"))])
        );

        let command = Command::Echo("hello".to_string());
        assert_eq!(
            RespValue::try_from(command).unwrap(),
            RespValue::Array(vec![
                RespValue::BulkString(String::from("ECHO")),
                RespValue::BulkString(String::from("hello")),
            ])
        );

        let command = Command::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
            expiry_duration: None,
        };

        assert_eq!(
            RespValue::try_from(command).unwrap(),
            RespValue::Array(vec![
                RespValue::BulkString(String::from("SET")),
                RespValue::BulkString(String::from("foo")),
                RespValue::BulkString(String::from("bar")),
            ])
        );
    }
}
