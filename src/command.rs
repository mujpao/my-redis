use crate::resp::RespValue;
use std::time::Duration;

pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        expiry_duration: Option<Duration>,
    },
    Get {
        key: String,
    },
    RPush {
        key: String,
        elements: Vec<RespValue>,
    },
    LPush {
        key: String,
        elements: Vec<RespValue>,
    },
    LPop {
        key: String,
    },
    LRange {
        key: String,
        start: i64,
        stop: i64,
    },
    LLen {
        key: String,
    },
}

#[derive(Debug)]
pub enum ParseCommandError {
    InvalidCommandName,
    WrongNumberArguments,
    InvalidArgument,
    UnknownCommand,
    InvalidRespData,
}

impl std::fmt::Display for ParseCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCommandName => write!(f, "Invalid command name"),
            Self::WrongNumberArguments => write!(f, "Wrong number of arguments"),
            Self::InvalidArgument => write!(f, "Invalid argument"),
            Self::UnknownCommand => write!(f, "Unknown command"),
            Self::InvalidRespData => write!(f, "RESP data not valid"),
        }
    }
}

impl TryFrom<RespValue> for Command {
    type Error = ParseCommandError;

    fn try_from(resp_value: RespValue) -> Result<Self, Self::Error> {
        match resp_value {
            RespValue::Array(ref data) => {
                if data.len() == 0 {
                    return Err(ParseCommandError::InvalidCommandName);
                }
                if let RespValue::BulkString(command_name) = &data[0] {
                    match command_name.as_str().to_uppercase().as_str() {
                        "PING" => Ok(Command::Ping),
                        "ECHO" => {
                            if data.len() < 2 {
                                println!("invalid command {:?}", resp_value);

                                return Err(ParseCommandError::WrongNumberArguments);
                            }

                            if let RespValue::BulkString(message) = &data[1] {
                                Ok(Command::Echo(message.into()))
                            } else {
                                println!("invalid command {:?}", resp_value);
                                Err(ParseCommandError::InvalidArgument)
                            }
                        }
                        "SET" => {
                            if data.len() < 3 {
                                println!("invalid command {:?}", resp_value);
                                return Err(ParseCommandError::WrongNumberArguments);
                            }
                            let mut command = match (&data[1], &data[2]) {
                                (RespValue::BulkString(key), RespValue::BulkString(value)) => {
                                    Command::Set {
                                        key: key.to_string(),
                                        value: value.to_string(),
                                        expiry_duration: None,
                                    }
                                }
                                (_, _) => {
                                    println!("invalid command {:?}", resp_value);
                                    return Err(ParseCommandError::InvalidArgument);
                                }
                            };

                            if data.len() == 5 {
                                match (&data[3], &data[4]) {
                                    (RespValue::BulkString(s), RespValue::BulkString(i)) => {
                                        let i: u64 = i.parse().map_err(|e| {
                                            println!("error parsing integer: {:?}", e);
                                            ParseCommandError::InvalidArgument
                                        })?;

                                        let units = s.as_str().to_uppercase();
                                        let duration = match units.as_str() {
                                            "EX" => Duration::from_secs(i),
                                            "PX" => Duration::from_millis(i),
                                            _ => {
                                                return Err(ParseCommandError::InvalidArgument);
                                            }
                                        };

                                        command = if let Command::Set { key, value, .. } = command {
                                            Command::Set {
                                                key,
                                                value,
                                                expiry_duration: Some(duration),
                                            }
                                        } else {
                                            return Err(ParseCommandError::InvalidArgument);
                                        };
                                    }
                                    (_, _) => {
                                        println!("invalid command {:?}", resp_value);
                                        return Err(ParseCommandError::InvalidArgument);
                                    }
                                }
                            }
                            Ok(command)
                        }
                        "GET" => {
                            if data.len() < 2 {
                                println!("invalid command {:?}", resp_value);
                                return Err(ParseCommandError::WrongNumberArguments);
                            }
                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::Get {
                                    key: key.to_string(),
                                }),
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(ParseCommandError::InvalidArgument)
                                }
                            }
                        }
                        "RPUSH" => {
                            if data.len() < 3 {
                                println!("invalid command {:?}", resp_value);
                                return Err(ParseCommandError::WrongNumberArguments);
                            }

                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::RPush {
                                    key: key.to_string(),
                                    elements: data[2..].to_vec(),
                                }),
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(ParseCommandError::InvalidArgument)
                                }
                            }
                        }
                        "LPUSH" => {
                            if data.len() < 3 {
                                println!("invalid command {:?}", resp_value);
                                return Err(ParseCommandError::WrongNumberArguments);
                            }

                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::LPush {
                                    key: key.to_string(),
                                    elements: data[2..].to_vec(),
                                }),
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(ParseCommandError::InvalidArgument)
                                }
                            }
                        }
                        "LRANGE" => {
                            if data.len() != 4 {
                                println!("invalid command {:?}", resp_value);
                                return Err(ParseCommandError::WrongNumberArguments);
                            }

                            match (&data[1], &data[2], &data[3]) {
                                (
                                    RespValue::BulkString(key),
                                    RespValue::BulkString(start),
                                    RespValue::BulkString(stop),
                                ) => {
                                    let start: i64 = start.parse().map_err(|e| {
                                        println!("error parsing integer: {:?}", e);
                                        ParseCommandError::InvalidArgument
                                    })?;

                                    let stop: i64 = stop.parse().map_err(|e| {
                                        println!("error parsing integer: {:?}", e);
                                        ParseCommandError::InvalidArgument
                                    })?;

                                    Ok(Command::LRange {
                                        key: key.to_string(),
                                        start,
                                        stop,
                                    })
                                }
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(ParseCommandError::InvalidArgument)
                                }
                            }
                        }
                        "LLEN" => {
                            if data.len() != 2 {
                                println!("invalid command {:?}", resp_value);
                                return Err(ParseCommandError::WrongNumberArguments);
                            }
                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::LLen {
                                    key: key.to_string(),
                                }),
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(ParseCommandError::InvalidArgument)
                                }
                            }
                        }
                        "LPOP" => {
                            if data.len() != 2 {
                                println!("invalid command {:?}", resp_value);
                                return Err(ParseCommandError::WrongNumberArguments);
                            }
                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::LPop {
                                    key: key.to_string(),
                                }),
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(ParseCommandError::InvalidArgument)
                                }
                            }
                        }
                        _ => {
                            println!("unknown command {:?}", resp_value);
                            Err(ParseCommandError::UnknownCommand)
                        }
                    }
                } else {
                    println!("invalid command {:?}", resp_value);

                    return Err(ParseCommandError::InvalidCommandName);
                }
            }
            _ => {
                println!("respvalue not an array {:?}", resp_value);
                return Err(ParseCommandError::InvalidRespData);
            }
        }
    }
}
