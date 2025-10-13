use crate::resp::RespValue;
use anyhow::anyhow;
use connection::Connection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

mod connection;
mod resp;

pub async fn run(listener: TcpListener) -> anyhow::Result<()> {
    let map: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let map2 = Arc::clone(&map);
        let (stream, _) = listener.accept().await?;
        println!("accepted new connection");

        let _: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut connection = Connection::new(stream);
            loop {
                let value = connection.read_value().await;

                // println!("value: {:?}", value);
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
                        Ok(Command::Set { key, value }) => {
                            {
                                let mut map_guard =
                                    map2.lock().map_err(|_| anyhow!("unable to lock map"))?;
                                map_guard.insert(key.to_string(), value.to_string());
                            }

                            let to_send = RespValue::SimpleString(String::from("OK"));
                            connection.write_value(&to_send).await?;
                        }
                        Ok(Command::Get { key }) => {
                            let response = {
                                let map_guard =
                                    map2.lock().map_err(|_| anyhow!("unable to lock map"))?;
                                match map_guard.get(&key) {
                                    Some(value) => RespValue::BulkString(value.clone()),
                                    None => RespValue::NullBulkString,
                                }
                            };

                            connection.write_value(&response).await?;
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
    Set { key: String, value: String },
    Get { key: String },
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
                            match (&data[1], &data[2]) {
                                (RespValue::BulkString(key), RespValue::BulkString(value)) => {
                                    Ok(Command::Set {
                                        key: key.to_string(),
                                        value: value.to_string(),
                                    })
                                }
                                (_, _) => {
                                    println!("invalid command {:?}", resp_value);
                                    Err(CommandError::InvalidArgument)
                                }
                            }
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
