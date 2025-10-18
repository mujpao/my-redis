use crate::resp::RespValue;
use anyhow::anyhow;
use command::Command;
use connection::Connection;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::sleep;

mod command;
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
                        Ok(Command::LRange { key, start, stop }) => {
                            let value = {
                                let lists_guard =
                                    lists2.lock().map_err(|_| anyhow!("unable to lock lists"))?;
                                match lists_guard.get(&key) {
                                    Some(entry) => {
                                        let start: usize = if start < 0 {
                                            std::cmp::max(start + entry.len() as i64, 0) as usize
                                        } else {
                                            start as usize
                                        };

                                        let mut stop: usize = if stop < 0 {
                                            std::cmp::max(stop + entry.len() as i64, 0) as usize
                                        } else {
                                            stop as usize
                                        };

                                        if start >= entry.len() || start > stop {
                                            Vec::<RespValue>::new()
                                        } else {
                                            stop = std::cmp::min(stop + 1, entry.len());
                                            entry.range(start..stop).cloned().collect::<Vec<_>>()
                                        }
                                    }
                                    _ => Vec::<RespValue>::new(),
                                }
                            };

                            connection.write_value(&RespValue::Array(value)).await?;
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
