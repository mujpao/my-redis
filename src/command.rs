use crate::resp::RespValue;
use std::time::Duration;
use tracing::{info, instrument};

#[derive(Debug)]
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
        count: Option<usize>,
    },
    LRange {
        key: String,
        start: i64,
        stop: i64,
    },
    LLen {
        key: String,
    },
    BLPop {
        key: String,
        timeout: Option<f64>,
    },
    Type {
        key: String,
    },
    XAdd {
        key: String,
        id: String,
        pairs: Vec<(String, String)>,
    },
    XRange {
        key: String,
        start: String,
        end: String,
    },
    XRead {
        keys_and_ids: Vec<(String, String)>,
        timeout: Option<u64>,
    },
    Incr {
        key: String,
    },
    Multi,
    Exec,
    Transaction {
        commands: Vec<Command>,
    },
    Discard,
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

    #[instrument]
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
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }

                            if let RespValue::BulkString(message) = &data[1] {
                                Ok(Command::Echo(message.into()))
                            } else {
                                let e = ParseCommandError::InvalidArgument;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }
                        }
                        "SET" => {
                            if data.len() < 3 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
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
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            };

                            if data.len() == 5 {
                                match (&data[3], &data[4]) {
                                    (RespValue::BulkString(s), RespValue::BulkString(i)) => {
                                        let i: u64 = i.parse().map_err(|e| {
                                            let e2 = ParseCommandError::InvalidArgument;
                                info!(parsing_error = %e, reason = %e2, ?resp_value, "error parsing integer");
                                            e2
                                        })?;

                                        let units = s.as_str().to_uppercase();
                                        let duration = match units.as_str() {
                                            "EX" => Duration::from_secs(i),
                                            "PX" => Duration::from_millis(i),
                                            d => {
                                                let e = ParseCommandError::InvalidArgument;
                                                info!(reason = %e, ?resp_value, unit=d, "invalid duration unit");
                                                return Err(e);
                                            }
                                        };

                                        command = if let Command::Set { key, value, .. } = command {
                                            Command::Set {
                                                key,
                                                value,
                                                expiry_duration: Some(duration),
                                            }
                                        } else {
                                            let e = ParseCommandError::InvalidArgument;
                                            info!(reason = %e, ?resp_value, "invalid command");
                                            return Err(e);
                                        };
                                    }
                                    (_, _) => {
                                        let e = ParseCommandError::InvalidArgument;
                                        info!(reason = %e, ?resp_value, "invalid command");
                                        return Err(e);
                                    }
                                }
                            }
                            Ok(command)
                        }
                        "GET" => {
                            if data.len() < 2 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }
                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::Get {
                                    key: key.to_string(),
                                }),
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "RPUSH" => {
                            if data.len() < 3 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }

                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::RPush {
                                    key: key.to_string(),
                                    elements: data[2..].to_vec(),
                                }),
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "LPUSH" => {
                            if data.len() < 3 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }

                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::LPush {
                                    key: key.to_string(),
                                    elements: data[2..].to_vec(),
                                }),
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "LRANGE" => {
                            if data.len() != 4 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }

                            match (&data[1], &data[2], &data[3]) {
                                (
                                    RespValue::BulkString(key),
                                    RespValue::BulkString(start),
                                    RespValue::BulkString(stop),
                                ) => {
                                    let start: i64 = start.parse().map_err(|e| {
                                        let e2 = ParseCommandError::InvalidArgument;
                                info!(parsing_error = %e, reason = %e2, ?resp_value, "error parsing integer");
                                            e2
                                    })?;

                                    let stop: i64 = stop.parse().map_err(|e| {
                                        let e2 = ParseCommandError::InvalidArgument;
                                info!(parsing_error = %e, reason = %e2, ?resp_value, "error parsing integer");
                                            e2
                                    })?;

                                    Ok(Command::LRange {
                                        key: key.to_string(),
                                        start,
                                        stop,
                                    })
                                }
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "LLEN" => {
                            if data.len() != 2 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }
                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::LLen {
                                    key: key.to_string(),
                                }),
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "LPOP" => {
                            if data.len() < 2 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }

                            let count = if data.len() > 2 {
                                match &data[2] {
                                    RespValue::BulkString(s) => {
                                        let count: usize = s.parse().map_err(|e| {
                                            let e2 = ParseCommandError::InvalidArgument;
                                info!(parsing_error = %e, reason = %e2, ?resp_value, "error parsing integer");
                                            e2
                                        })?;

                                        Some(count)
                                    }
                                    _ => return Err(ParseCommandError::WrongNumberArguments),
                                }
                            } else {
                                None
                            };

                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::LPop {
                                    key: key.to_string(),
                                    count,
                                }),
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "BLPOP" => {
                            if data.len() != 3 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }
                            match (&data[1], &data[2]) {
                                (RespValue::BulkString(key), RespValue::BulkString(s)) => {
                                    let timeout: f64 = s.parse().map_err(|e| {
                                       let e2 = ParseCommandError::InvalidArgument;
                                info!(parsing_error = %e, reason = %e2, ?resp_value, "error parsing integer");
                                            e2
                                    })?;

                                    let timeout = if timeout < 0.0 {
                                        return Err(ParseCommandError::InvalidArgument);
                                    } else if timeout == 0.0 {
                                        None
                                    } else {
                                        Some(timeout)
                                    };

                                    Ok(Command::BLPop {
                                        key: key.to_string(),
                                        timeout,
                                    })
                                }

                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "TYPE" => {
                            if data.len() < 2 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }
                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::Type {
                                    key: key.to_string(),
                                }),
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "XADD" => {
                            if data.len() < 5 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }
                            match (&data[1], &data[2]) {
                                (RespValue::BulkString(key), RespValue::BulkString(id)) => {
                                    let mut pairs = Vec::new();
                                    let mut field_idx = 3;
                                    let mut value_idx = field_idx + 1;
                                    while value_idx < data.len() {
                                        match (&data[field_idx], &data[value_idx]) {
                                            (
                                                RespValue::BulkString(field),
                                                RespValue::BulkString(value),
                                            ) => {
                                                pairs.push((field.to_string(), value.to_string()));
                                                field_idx = field_idx + 2;
                                                value_idx = field_idx + 1;
                                            }
                                            _ => {
                                                let e = ParseCommandError::InvalidArgument;
                                                info!(reason = %e, ?resp_value, "invalid command");
                                                return Err(e);
                                            }
                                        }
                                    }
                                    Ok(Command::XAdd {
                                        key: key.to_string(),
                                        id: id.to_string(),
                                        pairs,
                                    })
                                }
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "XRANGE" => {
                            if data.len() != 4 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }
                            match (&data[1], &data[2], &data[3]) {
                                (
                                    RespValue::BulkString(key),
                                    RespValue::BulkString(start),
                                    RespValue::BulkString(end),
                                ) => Ok(Command::XRange {
                                    key: key.to_string(),
                                    start: start.to_string(),
                                    end: end.to_string(),
                                }),
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "XREAD" => {
                            if data.len() < 4 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }

                            let (timeout, pairs_start_idx) = match &data[1] {
                                RespValue::BulkString(s) => {
                                    match s.as_str().to_uppercase().as_str() {
                                        "STREAMS" => (None, 2),
                                        "BLOCK" => {
                                            if data.len() < 6 {
                                                let e = ParseCommandError::WrongNumberArguments;
                                                info!(reason = %e, ?resp_value, "invalid command");
                                                return Err(e);
                                            }

                                            if let RespValue::BulkString(timeout) = &data[2] {
                                                let timeout: u64 =
                                                    timeout.parse().map_err(|e| {
                                                        let e2 = ParseCommandError::InvalidArgument;
                                info!(parsing_error = %e, reason = %e2, ?resp_value, "error parsing integer");
                                            e2
                                                    })?;

                                                (Some(timeout), 4)
                                            } else {
                                                let e = ParseCommandError::InvalidArgument;
                                                info!(reason = %e, ?resp_value, "invalid command");
                                                return Err(e);
                                            }
                                        }
                                        _ => {
                                            let e = ParseCommandError::InvalidArgument;
                                            info!(reason = %e, ?resp_value, "invalid command");
                                            return Err(e);
                                        }
                                    }
                                }
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            };

                            if (data.len() - 2) % 2 != 0 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }

                            let mut pairs = vec![];
                            let num_pairs = (data.len() - pairs_start_idx) / 2;
                            for i in 0..num_pairs {
                                match (
                                    &data[pairs_start_idx + i],
                                    &data[pairs_start_idx + i + num_pairs],
                                ) {
                                    (RespValue::BulkString(key), RespValue::BulkString(id)) => {
                                        pairs.push((key.to_string(), id.to_string()))
                                    }
                                    _ => {
                                        let e = ParseCommandError::InvalidArgument;
                                        info!(reason = %e, ?resp_value, "invalid command");
                                        return Err(e);
                                    }
                                }
                            }

                            Ok(Command::XRead {
                                keys_and_ids: pairs,
                                timeout,
                            })
                        }
                        "INCR" => {
                            if data.len() != 2 {
                                let e = ParseCommandError::WrongNumberArguments;
                                info!(reason = %e, ?resp_value, "invalid command");
                                return Err(e);
                            }
                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::Incr {
                                    key: key.to_string(),
                                }),
                                _ => {
                                    let e = ParseCommandError::InvalidArgument;
                                    info!(reason = %e, ?resp_value, "invalid command");
                                    return Err(e);
                                }
                            }
                        }
                        "MULTI" => Ok(Command::Multi),
                        "EXEC" => Ok(Command::Exec),
                        "DISCARD" => Ok(Command::Discard),
                        _ => {
                            let e = ParseCommandError::UnknownCommand;
                            info!(reason = %e, ?resp_value, "unknown command");
                            return Err(e);
                        }
                    }
                } else {
                    let e = ParseCommandError::InvalidCommandName;
                    info!(reason = %e, ?resp_value, "invalid command");
                    return Err(e);
                }
            }
            _ => {
                let e = ParseCommandError::InvalidRespData;
                info!(reason = %e, ?resp_value, "invalid command");
                return Err(e);
            }
        }
    }
}
