use anyhow::anyhow;
use bytes::Bytes;

#[derive(PartialEq, Debug)]
enum RespValue {
    SimpleString(Bytes),
    BulkString(Bytes),
    NullBulkString,
    SimpleError(Bytes),
    Integer(i64),
    Array(Vec<RespValue>),
    NullArray,
}

impl RespValue {
    fn new(data: &Bytes) -> anyhow::Result<RespValue> {
        let tokens = scan_tokens(&data)?;
        parse_resp_tokens(&tokens[..])
    }
}

#[derive(PartialEq, Debug)]
enum RespToken {
    SimpleString(Bytes),
    BulkString(Bytes),
    NullBulkString,
    SimpleError(Bytes),
    Integer(i64),
    ArrayStart(usize),
    NullArray,
}

fn scan_tokens(data: &Bytes) -> anyhow::Result<Vec<RespToken>> {
    let mut tokens = Vec::new();
    let mut index = 0;

    while index < data.len() {
        match &data[index..index + 1] {
            b"+" => {
                let crlf_idx = find_next_crlf(&data, index)?;
                let s = data.slice(index + 1..crlf_idx);

                tokens.push(RespToken::SimpleString(s));
                index = crlf_idx + 2;
            }
            b"-" => {
                let crlf_idx = find_next_crlf(&data, index)?;

                let s = data.slice(index + 1..crlf_idx);

                tokens.push(RespToken::SimpleError(s));
                index = crlf_idx + 2;
            }
            b"$" => {
                if data[index..].starts_with(b"$-1\r\n") {
                    tokens.push(RespToken::NullBulkString);
                    index = index + "$-1\r\n".len();
                } else {
                    let crlf_before_data_index = find_next_crlf(&data, index)?;

                    let len: usize =
                        str::from_utf8(&data[index + 1..crlf_before_data_index])?.parse()?;
                    let data_start_index = crlf_before_data_index + 2;
                    let crlf_after_data_index = find_next_crlf(&data, data_start_index)?;

                    let s = data.slice(data_start_index..crlf_after_data_index);

                    tokens.push(RespToken::BulkString(s));
                    index = crlf_after_data_index + 2;
                }
            }
            b":" => {
                let mut sign = 1;
                let rest_start_index = if &data[index + 1..index + 2] == b"-" {
                    sign = -1;

                    index + 2
                } else if &data[index + 1..index + 2] == b"+" {
                    index + 2
                } else {
                    index + 1
                };

                let crlf_idx = find_next_crlf(&data, rest_start_index)?;

                let i: i64 = str::from_utf8(&data[rest_start_index..crlf_idx])?.parse()?;

                tokens.push(RespToken::Integer(sign * i));
                index = crlf_idx + 2;
            }

            b"*" => {
                if data[index..].starts_with(b"*-1\r\n") {
                    tokens.push(RespToken::NullArray);
                    index = index + "*-1\r\n".len();
                } else {
                    let crlf_idx = find_next_crlf(&data, index)?;

                    let len: usize = str::from_utf8(&data[index + 1..crlf_idx])?.parse()?;

                    tokens.push(RespToken::ArrayStart(len));
                    index = crlf_idx + 2;
                }
            }

            _ => todo!(),
        }
    }

    Ok(tokens)
}

fn parse_resp_tokens(tokens: &[RespToken]) -> anyhow::Result<RespValue> {
    Ok(parse_next_token(tokens)?.0)
}

fn parse_next_token(tokens: &[RespToken]) -> anyhow::Result<(RespValue, &[RespToken])> {
    let value = match &tokens[0] {
        RespToken::SimpleString(s) => (RespValue::SimpleString(s.clone()), &tokens[1..]),
        RespToken::BulkString(s) => (RespValue::BulkString(s.clone()), &tokens[1..]),
        RespToken::NullBulkString => (RespValue::NullBulkString, &tokens[1..]),
        RespToken::SimpleError(s) => (RespValue::SimpleError(s.clone()), &tokens[1..]),
        RespToken::Integer(i) => (RespValue::Integer(*i), &tokens[1..]),
        RespToken::ArrayStart(length) => parse_array(&tokens)?,
        RespToken::NullArray => (RespValue::NullArray, &tokens[1..]),
    };

    Ok(value)
}

fn parse_array(mut tokens: &[RespToken]) -> anyhow::Result<(RespValue, &[RespToken])> {
    if let RespToken::ArrayStart(length) = &tokens[0] {
        let mut array = Vec::new();

        tokens = &tokens[1..];
        while array.len() < *length {
            let parsed_next = parse_next_token(&tokens)?;
            let parsed_value = parsed_next.0;
            tokens = parsed_next.1;
            array.push(parsed_value);
        }

        Ok((RespValue::Array(array), tokens))
    } else {
        Err(anyhow!("error parsing array {:?}", tokens))
    }
}

fn find_next_crlf(data: &[u8], index: usize) -> anyhow::Result<usize> {
    Ok(&data[index..]
        .windows(2)
        .position(|x| x == b"\r\n")
        .ok_or_else(|| anyhow!("can't find crlf"))?
        + index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_bulk_strings() {
        let s = Bytes::from("$-1\r\n");
        let result = scan_tokens(&s);
        assert_eq!(result.unwrap(), vec![RespToken::NullBulkString]);

        let s = Bytes::from("$0\r\n\r\n");
        let result = scan_tokens(&s);
        assert_eq!(result.unwrap(), vec![RespToken::BulkString("".into())]);

        let s = Bytes::from("$5\r\nhello\r\n");
        let result = scan_tokens(&s);
        assert_eq!(result.unwrap(), vec![RespToken::BulkString("hello".into())]);
    }

    #[test]
    fn scan_array() {
        let s =
            Bytes::from("*4\r\n*3\r\n:1\r\n:2\r\n:3\r\n$-1\r\n*-1\r\n*2\r\n+Hello\r\n-World\r\n");
        let result = scan_tokens(&s);
        assert_eq!(
            result.unwrap(),
            vec![
                RespToken::ArrayStart(4),
                RespToken::ArrayStart(3),
                RespToken::Integer(1),
                RespToken::Integer(2),
                RespToken::Integer(3),
                RespToken::NullBulkString,
                RespToken::NullArray,
                RespToken::ArrayStart(2),
                RespToken::SimpleString("Hello".into()),
                RespToken::SimpleError("World".into())
            ]
        );
    }

    // TODO
    // #[test]
    // fn returns_error_if_invalid_bytes_at_end() {
    //     let s = Bytes::from("$-1\r\nabcd");
    //     let result = scan_tokens(&s);
    //     assert!(result.is_err());

    //     let s = Bytes::from("$-1\r\na");
    //     let result = scan_tokens(&s);
    //     assert!(result.is_err());

    //     let s = Bytes::from("$0\r\n\r\na");
    //     let result = scan_tokens(&s);
    //     assert!(result.is_err());

    //     let s = Bytes::from("$5\r\nhello\r\na");
    //     let result = scan_tokens(&s);
    //     assert!(result.is_err());
    // }

    #[test]
    fn parse_simple_string() {
        let s = Bytes::from("+OK\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".into()));
    }

    #[test]
    fn parse_bulk_string() {
        let s = Bytes::from("$5\r\nhello\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::BulkString("hello".into()));
    }

    #[test]
    fn parse_empty_string() {
        let s = Bytes::from("$0\r\n\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::BulkString("".into()));
    }

    #[test]
    fn parse_simple_error() {
        let s = Bytes::from("-this is an error message\r\n");
        let result = RespValue::new(&s);
        assert_eq!(
            result.unwrap(),
            RespValue::SimpleError("this is an error message".into())
        );
    }

    #[test]
    fn parse_integer() {
        let s = Bytes::from(":0\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::Integer(0));

        let s = Bytes::from(":1000\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::Integer(1000));

        let s = Bytes::from(":-1\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::Integer(-1));

        let s = Bytes::from(":-1000\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::Integer(-1000));

        let s = Bytes::from(":+0\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::Integer(0));

        let s = Bytes::from(":+1000\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::Integer(1000));
    }

    #[test]
    fn parse_null_bulk_string() {
        let s = Bytes::from("$-1\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::NullBulkString);
    }

    #[test]
    fn parse_empty_array() {
        let s = Bytes::from("*0\r\n");
        let result = RespValue::new(&s);
        assert_eq!(result.unwrap(), RespValue::Array(vec![]));
    }

    #[test]
    fn parse_array() {
        let a = vec![
            RespToken::ArrayStart(2),
            RespToken::Integer(1),
            RespToken::Integer(2),
        ];
        let result = parse_resp_tokens(&a);
        assert_eq!(
            result.unwrap(),
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)])
        );
    }
}
