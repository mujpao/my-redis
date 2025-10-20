use crate::command::Command;
use crate::connection::Connection;
use crate::resp::RespValue;
use anyhow::anyhow;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;

type Map = HashMap<String, (String, Option<Instant>)>;
type Lists = HashMap<String, VecDeque<RespValue>>;

#[derive(Debug)]
struct BLPopListener {
    tx: oneshot::Sender<RespValue>,
    expires_at: Option<Instant>,
}

pub struct App {
    map: Mutex<Map>,
    lists: Mutex<Lists>,
    blpop_listeners: Mutex<HashMap<String, Vec<BLPopListener>>>,
}

impl App {
    pub fn new() -> Self {
        let map = Mutex::new(HashMap::new());
        let lists = Mutex::new(HashMap::new());
        let blpop_listeners = Mutex::new(HashMap::new());

        Self {
            map,
            lists,
            blpop_listeners,
        }
    }

    fn notify_blpop_listeners(&self, key: &String) -> anyhow::Result<()> {
        let mut listeners_guard = self
            .blpop_listeners
            .lock()
            .map_err(|_| anyhow!("unable to lock listeners"))?;
        match listeners_guard.remove(key) {
            Some(mut listeners) => {
                let mut lists_guard = self
                    .lists
                    .lock()
                    .map_err(|_| anyhow!("unable to lock lists"))?;
                listeners = listeners
                    .into_iter()
                    .filter_map(|listener| {
                        if let Some(expires_at) = listener.expires_at {
                            if expires_at < Instant::now() {
                                println!("blpop listener has expired");
                                return None;
                            }
                        }

                        if let Some(list) = lists_guard.get_mut(key) {
                            match list.pop_front() {
                                Some(elem) => {
                                    let to_send = RespValue::Array(vec![
                                        RespValue::BulkString(key.clone()),
                                        elem,
                                    ]);
                                    let _ = listener.tx.send(to_send);
                                    None
                                }
                                None => Some(listener),
                            }
                        } else {
                            None
                        }
                    })
                    .collect();

                listeners_guard.insert(key.clone(), listeners);
            }
            _ => {}
        }
        Ok(())
    }
}

pub async fn run(app: App, listener: TcpListener) -> anyhow::Result<()> {
    let app = Arc::new(app);
    loop {
        let app = Arc::clone(&app);
        let (stream, _) = listener.accept().await?;
        println!("accepted new connection");

        let _: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut connection = Connection::new(stream);
            loop {
                let value = connection.read_value().await;

                if let Some(value) = value? {
                    println!("read {:?}", value);

                    match Command::try_from(value) {
                        Ok(command) => {
                            let app_cloned = Arc::clone(&app);
                            handle_command(app_cloned, command, &mut connection).await?;
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

async fn handle_command(
    app: Arc<App>,
    command: Command,
    connection: &mut Connection,
) -> anyhow::Result<()> {
    match command {
        Command::Ping => {
            let to_send = RespValue::SimpleString(String::from("PONG"));
            connection.write_value(&to_send).await?;
        }
        Command::Echo(s) => {
            let to_send = RespValue::BulkString(s);
            connection.write_value(&to_send).await?;
        }
        Command::Set {
            key,
            value,
            expiry_duration,
        } => {
            let expiry_time = if let Some(duration) = expiry_duration {
                Some(Instant::now() + duration)
            } else {
                None
            };
            {
                let mut map_guard = app.map.lock().map_err(|_| anyhow!("unable to lock map"))?;
                map_guard.insert(key.to_string(), (value.to_string(), expiry_time));
            }

            if let Some(duration) = expiry_duration {
                let cloned_app = Arc::clone(&app);
                tokio::spawn(async move {
                    sleep(duration).await;

                    {
                        let mut map_guard = cloned_app
                            .map
                            .lock()
                            .map_err(|_| anyhow!("unable to lock map"))?;
                        map_guard.insert(key.to_string(), (value.to_string(), expiry_time));

                        if let Some((_, Some(key_expires_at))) = map_guard.get(&key) {
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
        Command::Get { key } => {
            let response = {
                let map_guard = app.map.lock().map_err(|_| anyhow!("unable to lock map"))?;
                match map_guard.get(&key) {
                    Some((value, _)) => RespValue::BulkString(value.clone()),
                    None => RespValue::NullBulkString,
                }
            };

            connection.write_value(&response).await?;
        }
        Command::RPush { key, elements } => {
            let len = {
                let mut lists_guard = app
                    .lists
                    .lock()
                    .map_err(|_| anyhow!("unable to lock lists"))?;
                let list = lists_guard
                    .entry(key.to_string())
                    .or_insert_with(|| VecDeque::new());
                list.extend(elements);

                list.len()
            };

            connection
                .write_value(&RespValue::Integer(len as i64))
                .await?;

            app.notify_blpop_listeners(&key)?;
        }
        Command::LPush { key, elements } => {
            let len = {
                let mut lists_guard = app
                    .lists
                    .lock()
                    .map_err(|_| anyhow!("unable to lock lists"))?;
                let list = lists_guard
                    .entry(key.to_string())
                    .or_insert_with(|| VecDeque::new());
                for elem in elements {
                    list.push_front(elem);
                }

                list.len()
            };

            connection
                .write_value(&RespValue::Integer(len as i64))
                .await?;

            app.notify_blpop_listeners(&key)?;
        }
        Command::LRange { key, start, stop } => {
            let value = {
                let lists_guard = app
                    .lists
                    .lock()
                    .map_err(|_| anyhow!("unable to lock lists"))?;
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
        Command::LLen { key } => {
            let response = {
                let lists_guard = app
                    .lists
                    .lock()
                    .map_err(|_| anyhow!("unable to lock map"))?;
                match lists_guard.get(&key) {
                    Some(list) => RespValue::Integer(list.len() as i64),
                    None => RespValue::Integer(0),
                }
            };

            connection.write_value(&response).await?;
        }
        Command::LPop { key, count } => {
            let response = lpop(
                &key,
                count,
                app.lists
                    .lock()
                    .map_err(|_| anyhow!("unable to lock map"))?,
            );

            connection.write_value(&response).await?;
        }
        Command::BLPop { key, timeout } => {
            let expires_at = timeout.map(|timeout| {
                let duration = Duration::from_secs_f64(timeout);
                Instant::now() + duration
            });

            let (tx, rx) = oneshot::channel();

            {
                let mut listeners_guard = app
                    .blpop_listeners
                    .lock()
                    .map_err(|_| anyhow!("unable to lock listeners"))?;

                let list = listeners_guard
                    .entry(key.to_string())
                    .or_insert_with(|| Vec::new());
                list.push(BLPopListener { tx, expires_at });
            }

            let response = rx.await?;
            connection.write_value(&response).await?;
        }
    }
    Ok(())
}

fn lpop(key: &String, count: Option<usize>, mut lists_guard: MutexGuard<Lists>) -> RespValue {
    let mut count = count.unwrap_or(1);

    match lists_guard.get_mut(key) {
        Some(list) => {
            let mut popped = vec![];

            count = std::cmp::min(count, list.len());
            match count {
                0 => RespValue::NullBulkString,
                1 => match list.pop_front() {
                    Some(elem) => elem,
                    None => RespValue::NullBulkString,
                },
                count => {
                    for _ in 0..count {
                        let value = match list.pop_front() {
                            Some(elem) => elem,
                            None => RespValue::NullBulkString,
                        };

                        popped.push(value);
                    }

                    RespValue::Array(popped)
                }
            }
        }

        None => RespValue::NullBulkString,
    }
}
