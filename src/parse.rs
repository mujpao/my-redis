use anyhow::anyhow;

#[derive(PartialEq, Debug)]
enum RespToken {
    SimpleString(String),
    BulkString(String),
    SimpleError(String),
    Integer(i32),
    NullBulkString,
}

fn parse_resp(s: &str) -> anyhow::Result<RespToken> {
    if s == "$-1\r\n" {
        return Ok(RespToken::NullBulkString);
    }
    let first = &s[0..1];
    match first {
        "+" => {
            let split_s = &s[1..]
                .split("\r\n")
                .next()
                .ok_or_else(|| anyhow!("can't split string"))?;
            Ok(RespToken::SimpleString((*split_s).to_string()))
        }
        "-" => {
            let split_s = &s[1..]
                .split("\r\n")
                .next()
                .ok_or_else(|| anyhow!("can't split string"))?;
            Ok(RespToken::SimpleError((*split_s).to_string()))
        }
        "$" => {
            let data = &s[4..]
                .split("\r\n")
                .next()
                .ok_or_else(|| anyhow!("can't split string"))?;
            Ok(RespToken::BulkString((*data).to_string()))
        }
        ":" => {
            let mut sign = 1;
            let rest = if &s[1..2] == "-" {
                sign = -1;
                &s[2..]
            } else if &s[1..2] == "+" {
                &s[2..]
            } else {
                &s[1..]
            };

            let data: i32 = rest
                .split("\r\n")
                .next()
                .ok_or_else(|| anyhow!("can't split string"))?
                .parse()?;
            Ok(RespToken::Integer(sign * data))
        }

        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_string() {
        let s = "+OK\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::SimpleString("OK".into()));
    }

    #[test]
    fn parse_bulk_string() {
        let s = "$5\r\nhello\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::BulkString("hello".into()));
    }

    #[test]
    fn parse_empty_string() {
        let s = "$0\r\n\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::BulkString("".into()));
    }

    #[test]
    fn parse_simple_error() {
        let s = "-this is an error message\r\n";
        let result = parse_resp(s);
        assert_eq!(
            result.unwrap(),
            RespToken::SimpleError("this is an error message".into())
        );
    }

    #[test]
    fn parse_integer() {
        let s = ":0\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::Integer(0));

        let s = ":1000\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::Integer(1000));

        let s = ":-1\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::Integer(-1));

        let s = ":-1000\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::Integer(-1000));

        let s = ":+0\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::Integer(0));

        let s = ":+1000\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::Integer(1000));
    }

    #[test]
    fn parse_null_bulk_string() {
        let s = "$-1\r\n";
        let result = parse_resp(s);
        assert_eq!(result.unwrap(), RespToken::NullBulkString);
    }
}
