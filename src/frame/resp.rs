use super::{Frame, ParseError, get_length, get_line, peek_u8, skip};
use crate::command::Command;
use anyhow::anyhow;
use bytes::Buf;
use std::io::Cursor;

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

impl Frame for RespValue {
    fn parse(data: &mut Cursor<&[u8]>) -> Result<RespValue, ParseError> {
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
                if b'-' == peek_u8(data)? {
                    let line = get_line(data)?;
                    if line != b"-1" {
                        return Err(ParseError::Other(anyhow!("invalid resp data")));
                    }

                    // null array
                    Ok(RespValue::NullArray)
                } else {
                    let len = get_length(data)?;

                    let mut array = Vec::new();
                    while array.len() < len {
                        let next = Self::parse(data)?;

                        array.push(next);
                    }

                    Ok(RespValue::Array(array))
                }
            }
            _ => Err(ParseError::Other(anyhow!("invalid resp data"))),
        }
    }

    fn check(data: &mut Cursor<&[u8]>) -> Result<(), ParseError> {
        if !data.has_remaining() {
            return Err(ParseError::Incomplete);
        }

        match data.get_u8() {
            b'+' => {
                get_line(data)?;
                Ok(())
            }
            b'-' => {
                get_line(data)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(data)? {
                    let line = get_line(data)?;
                    if line != b"-1" {
                        return Err(ParseError::Other(anyhow!("invalid resp data")));
                    }
                    Ok(())
                } else {
                    let len: usize = get_length(data)?;
                    skip(data, len + 2)
                }
            }
            b':' => {
                let s = get_line(data)?;
                let _ = str::from_utf8(s)
                    .map_err(|e| ParseError::Other(e.into()))?
                    .parse::<i64>()
                    .map_err(|e| ParseError::Other(e.into()))?;
                Ok(())
            }

            b'*' => {
                if b'-' == peek_u8(data)? {
                    let line = get_line(data)?;
                    if line != b"-1" {
                        return Err(ParseError::Other(anyhow!("invalid resp data")));
                    }

                    // null array
                    Ok(())
                } else {
                    let len = get_length(data)?;

                    for _ in 0..len {
                        RespValue::check(data)?;
                    }

                    Ok(())
                }
            }
            _ => Err(ParseError::Other(anyhow!("invalid resp data"))),
        }
    }
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

    fn parse(bytes: Bytes) -> Result<RespValue, ParseError> {
        let mut cursor = Cursor::new(&bytes[..]);
        assert_eq!(RespValue::check(&mut cursor).unwrap(), ());

        cursor.set_position(0);

        RespValue::parse(&mut cursor)
    }

    fn parse_incomplete(bytes: Bytes) {
        let mut cursor = Cursor::new(&bytes[..]);
        assert!(matches!(
            RespValue::check(&mut cursor).unwrap_err(),
            ParseError::Incomplete
        ));

        cursor.set_position(0);

        assert!(matches!(
            RespValue::parse(&mut cursor).unwrap_err(),
            ParseError::Incomplete
        ));
    }

    #[test]
    fn parse_bulk_strings() {
        let s = Bytes::from("$-1\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::NullBulkString);

        let s = Bytes::from("$0\r\n\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::BulkString("".into()));

        let s = Bytes::from("$5\r\nhello\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::BulkString("hello".into()));
    }

    #[test]
    fn parse_array_with_mixed_types() {
        let s =
            Bytes::from("*4\r\n*3\r\n:1\r\n:2\r\n:3\r\n$-1\r\n*-1\r\n*2\r\n+Hello\r\n-World\r\n");
        let result = parse(s);
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
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".into()));
    }

    #[test]
    fn parse_bulk_string() {
        let s = Bytes::from("$5\r\nhello\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::BulkString("hello".into()));
    }

    #[test]
    fn parse_empty_string() {
        let s = Bytes::from("$0\r\n\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::BulkString("".into()));
    }

    #[test]
    fn parse_simple_error() {
        let s = Bytes::from("-this is an error message\r\n");
        let result = parse(s);
        assert_eq!(
            result.unwrap(),
            RespValue::SimpleError("this is an error message".into())
        );
    }

    #[test]
    fn parse_integer() {
        let s = Bytes::from(":0\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::Integer(0));

        let s = Bytes::from(":1000\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::Integer(1000));

        let s = Bytes::from(":-1\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::Integer(-1));

        let s = Bytes::from(":-1000\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::Integer(-1000));

        let s = Bytes::from(":+0\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::Integer(0));

        let s = Bytes::from(":+1000\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::Integer(1000));
    }

    #[test]
    fn parse_null_bulk_string() {
        let s = Bytes::from("$-1\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::NullBulkString);
    }

    #[test]
    fn parse_empty_array() {
        let s = Bytes::from("*0\r\n");
        let result = parse(s);
        assert_eq!(result.unwrap(), RespValue::Array(vec![]));
    }

    #[test]
    fn parse_array() {
        let s = Bytes::from("*2\r\n:1\r\n:2\r\n");
        let result = parse(s);
        assert_eq!(
            result.unwrap(),
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)])
        );
    }

    #[test]
    fn returns_incomplete_if_partial_frame_read() {
        let s = Bytes::from("");
        parse_incomplete(s);

        let s = Bytes::from(":0\r");
        parse_incomplete(s);

        let s = Bytes::from(":1000");
        parse_incomplete(s);

        let s = Bytes::from(":-1");
        parse_incomplete(s);

        let s = Bytes::from("$0\r\n");
        parse_incomplete(s);

        let s = Bytes::from("$5\r\nhello");
        parse_incomplete(s);
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
