use crate::client_connection::ConnCommand;
use crate::frame::resp::RespValue;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, instrument};

#[derive(Debug, Clone)]
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
    Info {
        categories: Vec<String>,
    },
    ReplConf {
        replica_addr: Option<SocketAddr>,
        tx: Option<mpsc::Sender<ConnCommand>>,
    },
    PSync {
        repl_id: Option<String>,
        offset: i64,
        replica_addr: Option<SocketAddr>,
    },
    ReplConfGetAck,
    Wait {
        num_replicas: usize,
        timeout: Duration,
    },
    Ack {
        offset: usize,
    },
}

impl Command {
    pub fn is_write(&self) -> bool {
        match self {
            Command::Get { .. }
            | Command::LRange { .. }
            | Command::LLen { .. }
            | Command::XRange { .. }
            | Command::XRead { .. }
            | Command::Ping
            | Command::Echo(_)
            | Command::Type { .. }
            | Command::Info { .. }
            | Command::ReplConf { .. }
            | Command::PSync { .. }
            | Command::Wait { .. }
            | Command::ReplConfGetAck
            | Command::Ack { .. } => false,
            Command::Set { .. }
            | Command::RPush { .. }
            | Command::LPush { .. }
            | Command::LPop { .. }
            | Command::BLPop { .. }
            | Command::XAdd { .. }
            | Command::Incr { .. }
            | Command::Multi
            | Command::Exec
            | Command::Transaction { .. }
            | Command::Discard => true,
        }
    }

    pub fn size(&self) -> anyhow::Result<usize> {
        let value = RespValue::try_from(self.clone())?;
        Ok(value.size())
    }
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
        let RespValue::Array(ref data) = resp_value else {
            return Err(ParseCommandError::InvalidRespData);
        };

        if data.is_empty() {
            return Err(ParseCommandError::InvalidCommandName);
        }

        let RespValue::BulkString(command_name) = &data[0] else {
            return Err(ParseCommandError::InvalidCommandName);
        };

        match command_name.as_str().to_uppercase().as_str() {
            "PING" => Ok(Command::Ping),
            "ECHO" => {
                if data.len() < 2 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }

                if let RespValue::BulkString(message) = &data[1] {
                    Ok(Command::Echo(message.into()))
                } else {
                    Err(ParseCommandError::InvalidArgument)
                }
            }
            "SET" => {
                if data.len() != 3 && data.len() != 5 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }
                let mut command = match (&data[1], &data[2]) {
                    (RespValue::BulkString(key), RespValue::BulkString(value)) => Command::Set {
                        key: key.to_string(),
                        value: value.to_string(),
                        expiry_duration: None,
                    },
                    _ => {
                        return Err(ParseCommandError::InvalidArgument);
                    }
                };

                if data.len() == 5 {
                    match (&data[3], &data[4]) {
                        (RespValue::BulkString(s), RespValue::BulkString(i)) => {
                            let i: u64 =
                                i.parse().map_err(|_| ParseCommandError::InvalidArgument)?;

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
                            return Err(ParseCommandError::InvalidArgument);
                        }
                    }
                }
                Ok(command)
            }
            "GET" => {
                if data.len() < 2 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }
                match &data[1] {
                    RespValue::BulkString(key) => Ok(Command::Get {
                        key: key.to_string(),
                    }),
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "RPUSH" => {
                if data.len() < 3 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }

                match &data[1] {
                    RespValue::BulkString(key) => Ok(Command::RPush {
                        key: key.to_string(),
                        elements: data[2..].to_vec(),
                    }),
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "LPUSH" => {
                if data.len() < 3 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }

                match &data[1] {
                    RespValue::BulkString(key) => Ok(Command::LPush {
                        key: key.to_string(),
                        elements: data[2..].to_vec(),
                    }),
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "LRANGE" => {
                if data.len() != 4 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }

                match (&data[1], &data[2], &data[3]) {
                    (
                        RespValue::BulkString(key),
                        RespValue::BulkString(start),
                        RespValue::BulkString(stop),
                    ) => {
                        let start: i64 = start
                            .parse()
                            .map_err(|_| ParseCommandError::InvalidArgument)?;

                        let stop: i64 = stop
                            .parse()
                            .map_err(|_| ParseCommandError::InvalidArgument)?;

                        Ok(Command::LRange {
                            key: key.to_string(),
                            start,
                            stop,
                        })
                    }
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "LLEN" => {
                if data.len() != 2 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }
                match &data[1] {
                    RespValue::BulkString(key) => Ok(Command::LLen {
                        key: key.to_string(),
                    }),
                    _ => {
                        let e = ParseCommandError::InvalidArgument;
                        info!(reason = %e, ?resp_value, "invalid command");
                        Err(e)
                    }
                }
            }
            "LPOP" => {
                if data.len() < 2 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }

                let count = if data.len() > 2 {
                    match &data[2] {
                        RespValue::BulkString(s) => {
                            let count: usize =
                                s.parse().map_err(|_| ParseCommandError::InvalidArgument)?;

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
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "BLPOP" => {
                if data.len() != 3 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }
                match (&data[1], &data[2]) {
                    (RespValue::BulkString(key), RespValue::BulkString(s)) => {
                        let timeout: f64 =
                            s.parse().map_err(|_| ParseCommandError::InvalidArgument)?;

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

                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "TYPE" => {
                if data.len() < 2 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }
                match &data[1] {
                    RespValue::BulkString(key) => Ok(Command::Type {
                        key: key.to_string(),
                    }),
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "XADD" => {
                if data.len() < 5 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }
                match (&data[1], &data[2]) {
                    (RespValue::BulkString(key), RespValue::BulkString(id)) => {
                        let mut pairs = Vec::new();
                        let mut field_idx = 3;
                        let mut value_idx = field_idx + 1;
                        while value_idx < data.len() {
                            match (&data[field_idx], &data[value_idx]) {
                                (RespValue::BulkString(field), RespValue::BulkString(value)) => {
                                    pairs.push((field.to_string(), value.to_string()));
                                    field_idx += 2;
                                    value_idx = field_idx + 1;
                                }
                                _ => {
                                    return Err(ParseCommandError::InvalidArgument);
                                }
                            }
                        }
                        Ok(Command::XAdd {
                            key: key.to_string(),
                            id: id.to_string(),
                            pairs,
                        })
                    }
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "XRANGE" => {
                if data.len() != 4 {
                    return Err(ParseCommandError::WrongNumberArguments);
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
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "XREAD" => {
                if data.len() < 4 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }

                let (timeout, pairs_start_idx) = match &data[1] {
                    RespValue::BulkString(s) => match s.as_str().to_uppercase().as_str() {
                        "STREAMS" => (None, 2),
                        "BLOCK" => {
                            if data.len() < 6 {
                                return Err(ParseCommandError::WrongNumberArguments);
                            }

                            if let RespValue::BulkString(timeout) = &data[2] {
                                let timeout: u64 = timeout
                                    .parse()
                                    .map_err(|_| ParseCommandError::InvalidArgument)?;

                                (Some(timeout), 4)
                            } else {
                                return Err(ParseCommandError::InvalidArgument);
                            }
                        }
                        _ => {
                            return Err(ParseCommandError::InvalidArgument);
                        }
                    },
                    _ => {
                        return Err(ParseCommandError::InvalidArgument);
                    }
                };

                if (data.len() - 2) % 2 != 0 {
                    return Err(ParseCommandError::WrongNumberArguments);
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
                            return Err(ParseCommandError::InvalidArgument);
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
                    return Err(ParseCommandError::WrongNumberArguments);
                }
                match &data[1] {
                    RespValue::BulkString(key) => Ok(Command::Incr {
                        key: key.to_string(),
                    }),
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "MULTI" => Ok(Command::Multi),
            "EXEC" => Ok(Command::Exec),
            "DISCARD" => Ok(Command::Discard),
            "INFO" => {
                let mut categories = Vec::new();
                for category in &data[1..] {
                    match category {
                        RespValue::BulkString(category) => {
                            categories.push(category.clone());
                        }
                        _ => {
                            return Err(ParseCommandError::InvalidArgument);
                        }
                    }
                }
                Ok(Command::Info { categories })
            }
            "REPLCONF" => {
                if data.len() != 3 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }

                let RespValue::BulkString(arg) = &data[1] else {
                    return Err(ParseCommandError::InvalidArgument);
                };

                match arg.to_lowercase().as_str() {
                    "listening-port" | "capa" => Ok(Command::ReplConf {
                        replica_addr: None,
                        tx: None,
                    }),
                    "getack" => Ok(Command::ReplConfGetAck),
                    "ack" => {
                        let RespValue::BulkString(arg) = &data[2] else {
                            return Err(ParseCommandError::InvalidArgument);
                        };

                        let offset: usize = arg
                            .parse()
                            .map_err(|_| ParseCommandError::InvalidArgument)?;
                        Ok(Command::Ack { offset })
                    }
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "PSYNC" => {
                if data.len() != 3 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }
                match (&data[1], &data[2]) {
                    (RespValue::BulkString(repl_id), RespValue::BulkString(offset)) => {
                        let offset: i64 = offset
                            .parse()
                            .map_err(|_| ParseCommandError::InvalidArgument)?;

                        let repl_id = if repl_id == "?" {
                            None
                        } else {
                            Some(repl_id.clone())
                        };

                        Ok(Command::PSync {
                            repl_id,
                            offset,
                            replica_addr: None,
                        })
                    }
                    _ => Err(ParseCommandError::InvalidArgument),
                }
            }
            "WAIT" => {
                if data.len() != 3 {
                    return Err(ParseCommandError::WrongNumberArguments);
                }

                let (RespValue::BulkString(num_replicas), RespValue::BulkString(timeout_ms)) =
                    (&data[1], &data[2])
                else {
                    return Err(ParseCommandError::InvalidArgument);
                };

                let num_replicas: usize = num_replicas
                    .parse()
                    .map_err(|_| ParseCommandError::InvalidArgument)?;

                let timeout_ms: u64 = timeout_ms
                    .parse()
                    .map_err(|_| ParseCommandError::InvalidArgument)?;

                let timeout = Duration::from_millis(timeout_ms);

                Ok(Command::Wait {
                    num_replicas,
                    timeout,
                })
            }
            _ => Err(ParseCommandError::UnknownCommand),
        }
    }
}
