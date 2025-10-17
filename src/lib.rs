use crate::resp::RespValue;
use anyhow::anyhow;
use connection::Connection;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::sleep;

mod connection;
mod resp;

pub async fn run(listener: TcpListener) -> anyhow::Result<()> {
    let map: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let lists: Arc<Mutex<HashMap<String, VecDeque<RespValue>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    loop {
        let map2 = Arc::clone(&map);
        let lists2 = Arc::clone(&lists);
        let (stream, _) = listener.accept().await?;
        println!("accepted new connection");

        let _: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut connection = Connection::new(stream);
            loop {
                let value = connection.read_value().await;

                if let Some(value) = value? {
                    println!("read {:?}", value);

                    match Command::try_from(value) {
                        Ok(Command::Ping) => {
                            let to_send = RespValue::SimpleString(String::from("PONG"));
                            connection.write_value(&to_send).await?;
                        }
                        Ok(Command::Echo(s)) => {
                            let to_send = RespValue::BulkString(s);
                            connection.write_value(&to_send).await?;
                        }
                        Ok(Command::Set {
                            key,
                            value,
                            expiry_duration,
                        }) => {
                            let expiry_time = if let Some(duration) = expiry_duration {
                                Some(Instant::now() + duration)
                            } else {
                                None
                            };
                            {
                                let mut map_guard =
                                    map2.lock().map_err(|_| anyhow!("unable to lock map"))?;
                                map_guard.insert(key.to_string(), (value.to_string(), expiry_time));
                            }

                            if let Some(duration) = expiry_duration {
                                let map = Arc::clone(&map2);
                                tokio::spawn(async move {
                                    sleep(duration).await;

                                    {
                                        let mut map_guard = map
                                            .lock()
                                            .map_err(|_| anyhow!("unable to lock map"))?;
                                        map_guard.insert(
                                            key.to_string(),
                                            (value.to_string(), expiry_time),
                                        );

                                        if let Some((_, Some(key_expires_at))) = map_guard.get(&key)
                                        {
                                            if *key_expires_at < Instant::now() {
                                                map_guard.remove(&key);
                                            }
                                        }
                                    }

                                    Ok::<(), anyhow::Error>(())
                                });
                            }

                            let to_send = RespValue::SimpleString(String::from("OK"));
                            connection.write_value(&to_send).await?;
                        }
                        Ok(Command::Get { key }) => {
                            let response = {
                                let map_guard =
                                    map2.lock().map_err(|_| anyhow!("unable to lock map"))?;
                                match map_guard.get(&key) {
                                    Some((value, _)) => RespValue::BulkString(value.clone()),
                                    None => RespValue::NullBulkString,
                                }
                            };

                            connection.write_value(&response).await?;
                        }
                        Ok(Command::RPush { key, elements }) => {
                            let len = {
                                let mut lists_guard =
                                    lists2.lock().map_err(|_| anyhow!("unable to lock lists"))?;
                                let list = lists_guard
                                    .entry(key.to_string())
                                    .or_insert_with(|| VecDeque::new());
                                list.extend(elements);

                                list.len()
                            };

                            connection
                                .write_value(&RespValue::Integer(len as i64))
                                .await?;
                        }
                        Ok(Command::LPush {
                            key,
                            start,
                            mut stop,
                        }) => {
                            let value = {
                                let lists_guard =
                                    lists2.lock().map_err(|_| anyhow!("unable to lock lists"))?;
                                if let Some(entry) = lists_guard.get(&key) {
                                    if start >= entry.len() || start > stop {
                                        Some(Vec::<RespValue>::new())
                                    } else {
                                        stop = std::cmp::min(stop + 1, entry.len());
                                        Some(entry.range(start..stop).cloned().collect::<Vec<_>>())
                                    }
                                } else {
                                    None
                                }
                            };

                            match value {
                                Some(result) => {
                                    connection.write_value(&RespValue::Array(result)).await?;
                                }
                                None => {
                                    connection.write_value(&RespValue::NullArray).await?;
                                }
                            }
                        }
                        Err(e) => {
                            let s = format!("{}", e);
                            let to_send = RespValue::SimpleError(s);
                            connection.write_value(&to_send).await?;
                        }
                    }
                }
            }
        });
    }
}

enum Command {
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
        start: usize,
        stop: usize,
    },
}

#[derive(Debug)]
enum CommandError {
    InvalidCommandName,
    WrongNumberArguments,
    InvalidArgument,
    UnknownCommand,
    InvalidRespData,
}

impl std::fmt::Display for CommandError {
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
    type Error = CommandError;

    fn try_from(resp_value: RespValue) -> Result<Self, Self::Error> {
        match resp_value {
            RespValue::Array(ref data) => {
                if data.len() == 0 {
                    return Err(CommandError::InvalidCommandName);
                }
                if let RespValue::BulkString(command_name) = &data[0] {
                    match command_name.as_str().to_uppercase().as_str() {
                        "PING" => Ok(Command::Ping),
                        "ECHO" => {
                            if data.len() < 2 {
                                println!("invalid command {:?}", resp_value);

                                return Err(CommandError::WrongNumberArguments);
                            }

                            if let RespValue::BulkString(message) = &data[1] {
                                Ok(Command::Echo(message.into()))
                            } else {
                                println!("invalid command {:?}", resp_value);
                                Err(CommandError::InvalidArgument)
                            }
                        }
                        "SET" => {
                            if data.len() < 3 {
                                println!("invalid command {:?}", resp_value);
                                return Err(CommandError::WrongNumberArguments);
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
                                    return Err(CommandError::InvalidArgument);
                                }
                            };

                            if data.len() == 5 {
                                match (&data[3], &data[4]) {
                                    (RespValue::BulkString(s), RespValue::BulkString(i)) => {
                                        let i: u64 = i.parse().map_err(|e| {
                                            println!("error parsing integer: {:?}", e);
                                            CommandError::InvalidArgument
                                        })?;

                                        let units = s.as_str().to_uppercase();
                                        let duration = match units.as_str() {
                                            "EX" => Duration::from_secs(i),
                                            "PX" => Duration::from_millis(i),
                                            _ => {
                                                return Err(CommandError::InvalidArgument);
                                            }
                                        };

                                        command = if let Command::Set { key, value, .. } = command {
                                            Command::Set {
                                                key,
                                                value,
                                                expiry_duration: Some(duration),
                                            }
                                        } else {
                                            return Err(CommandError::InvalidArgument);
                                        };
                                    }
                                    (_, _) => {
                                        println!("invalid command {:?}", resp_value);
                                        return Err(CommandError::InvalidArgument);
                                    }
                                }
                            }
                            Ok(command)
                        }
                        "GET" => {
                            if data.len() < 2 {
                                println!("invalid command {:?}", resp_value);
                                return Err(CommandError::WrongNumberArguments);
                            }
                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::Get {
                                    key: key.to_string(),
                                }),
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(CommandError::InvalidArgument)
                                }
                            }
                        }
                        "RPUSH" => {
                            if data.len() < 3 {
                                println!("invalid command {:?}", resp_value);
                                return Err(CommandError::WrongNumberArguments);
                            }

                            match &data[1] {
                                RespValue::BulkString(key) => Ok(Command::RPush {
                                    key: key.to_string(),
                                    elements: data[2..].to_vec(),
                                }),
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(CommandError::InvalidArgument)
                                }
                            }
                        }
                        "LRANGE" => {
                            if data.len() != 4 {
                                println!("invalid command {:?}", resp_value);
                                return Err(CommandError::WrongNumberArguments);
                            }

                            match (&data[1], &data[2], &data[3]) {
                                (
                                    RespValue::BulkString(key),
                                    RespValue::BulkString(start),
                                    RespValue::BulkString(stop),
                                ) => {
                                    let start: usize = start.parse().map_err(|e| {
                                        println!("error parsing integer: {:?}", e);
                                        CommandError::InvalidArgument
                                    })?;

                                    let stop: usize = stop.parse().map_err(|e| {
                                        println!("error parsing integer: {:?}", e);
                                        CommandError::InvalidArgument
                                    })?;

                                    Ok(Command::LPush {
                                        key: key.to_string(),
                                        start,
                                        stop,
                                    })
                                }
                                _ => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(CommandError::InvalidArgument)
                                }
                            }
                        }
                        _ => {
                            println!("unknown command {:?}", resp_value);
                            Err(CommandError::UnknownCommand)
                        }
                    }
                } else {
                    println!("invalid command {:?}", resp_value);

                    return Err(CommandError::InvalidCommandName);
                }
            }
            _ => {
                println!("respvalue not an array {:?}", resp_value);
                return Err(CommandError::InvalidRespData);
            }
        }
    }
}
