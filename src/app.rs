use crate::command::Command;
use crate::connection::Connection;
use crate::resp::RespValue;
use anyhow::anyhow;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

type Map = HashMap<String, RedisDataType>;

enum RedisDataType {
    String(Box<(String, Option<Instant>)>),
    List(Box<VecDeque<RespValue>>),
}

#[derive(Debug)]
struct BLPopListener {
    tx: oneshot::Sender<RespValue>,
    expires_at: Option<Instant>,
}

#[derive(Debug)]
enum CommandResponse {
    NonBlocking(RespValue),
    Blocking((oneshot::Receiver<RespValue>, Option<Duration>)),
}

pub async fn run(listener: TcpListener) -> anyhow::Result<()> {
    let map = HashMap::new();
    let blpop_listeners = HashMap::new();

    let (command_tx, command_rx) = mpsc::channel(100);
    let (events_tx, events_rx) = mpsc::channel(100);

    let mut app = App {
        map,
        blpop_listeners,
        command_rx,
        events_tx,
        events_rx,
    };

    tokio::spawn(async move {
        accept_listeners(listener, command_tx).await.unwrap();
    });

    app.run().await
}

enum AppEvent {
    KeyExpired { key: String },
}

struct App {
    map: Map,
    blpop_listeners: HashMap<String, Vec<BLPopListener>>,
    command_rx: mpsc::Receiver<(Command, oneshot::Sender<CommandResponse>)>,
    events_tx: mpsc::Sender<AppEvent>,
    events_rx: mpsc::Receiver<AppEvent>,
}

impl App {
    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            select![
                maybe_command = self.command_rx.recv() => {
                    match maybe_command {
                         Some((command, resp_tx))   => {
                             self.process_command(command, resp_tx).await?;
                          },
                         None  => {
                             return Err(anyhow!("command channel closed"));

                         },
                    }
                }
                maybe_event = self.events_rx.recv() => {
                    match maybe_event {
                         Some(event)   => {
                    self.process_event(event).await?;
                          },
                         None  => {
                             return Err(anyhow!("event channel closed"));

                         },
                    }
                }
            ];
        }
    }

    async fn process_event(&mut self, event: AppEvent) -> anyhow::Result<()> {
        match event {
            AppEvent::KeyExpired { key } => {
                if let Some(RedisDataType::String(b)) = self.map.get(&key) {
                    if let (_, Some(key_expires_at)) = **b {
                        if key_expires_at < Instant::now() {
                            self.map.remove(&key);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_command(
        &mut self,
        command: Command,
        resp_tx: oneshot::Sender<CommandResponse>,
    ) -> anyhow::Result<()> {
        match command {
            Command::Ping => {
                let response = RespValue::SimpleString(String::from("PONG"));
                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
            }
            Command::Echo(s) => {
                let response = RespValue::BulkString(s);
                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
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
                self.map.insert(
                    key.to_string(),
                    RedisDataType::String(Box::new((value.to_string(), expiry_time))),
                );

                let tx_cloned = self.events_tx.clone();

                if let Some(duration) = expiry_duration {
                    tokio::spawn(async move {
                        sleep(duration).await;
                        tx_cloned.send(AppEvent::KeyExpired { key }).await?;

                        Ok::<(), anyhow::Error>(())
                    });
                }

                let response = RespValue::SimpleString(String::from("OK"));
                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
            }
            Command::Get { key } => {
                let response = match self.map.get(&key) {
                    Some(RedisDataType::String(value)) => RespValue::BulkString(value.0.clone()),
                    None => RespValue::NullBulkString,
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
            }
            Command::RPush { key, elements } => {
                let response = match self.map.get_mut(&key) {
                    Some(RedisDataType::List(list)) => {
                        list.extend(elements);
                        RespValue::Integer(list.len() as i64)
                    }
                    None => {
                        let len = elements.len();
                        self.map.insert(
                            key.to_string(),
                            RedisDataType::List(Box::new(VecDeque::from(elements))),
                        );
                        RespValue::Integer(len as i64)
                    }
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;

                self.notify_blpop_listeners(&key)?;
            }
            Command::LPush { key, elements } => {
                let response = match self.map.get_mut(&key) {
                    Some(RedisDataType::List(list)) => {
                        for elem in elements {
                            list.push_front(elem);
                        }
                        RespValue::Integer(list.len() as i64)
                    }
                    None => {
                        let mut list = VecDeque::new();
                        for elem in elements {
                            list.push_front(elem);
                        }

                        let len = list.len();

                        self.map
                            .insert(key.to_string(), RedisDataType::List(Box::new(list)));

                        RespValue::Integer(len as i64)
                    }
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;

                self.notify_blpop_listeners(&key)?;
            }
            Command::LRange { key, start, stop } => {
                let value = match self.map.get(&key) {
                    Some(RedisDataType::List(entry)) => {
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
                };

                resp_tx
                    .send(CommandResponse::NonBlocking(RespValue::Array(value)))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
            }
            Command::LLen { key } => {
                let response = match self.map.get(&key) {
                    Some(RedisDataType::List(list)) => RespValue::Integer(list.len() as i64),
                    None => RespValue::Integer(0),
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
            }
            Command::LPop { key, count } => {
                let response = lpop(&key, count, &mut self.map);

                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
            }
            Command::BLPop { key, timeout } => {
                let elem = self
                    .map
                    .get_mut(&key)
                    .map(|data| match data {
                        RedisDataType::List(list) => list.pop_front(),
                        _ => None,
                    })
                    .flatten();

                let response = match elem {
                    Some(elem) => CommandResponse::NonBlocking(RespValue::Array(vec![
                        RespValue::BulkString(key.clone()),
                        elem,
                    ])),
                    None => {
                        let (tx, rx) = oneshot::channel();
                        let duration = timeout.map(|timeout| Duration::from_secs_f64(timeout));

                        let expires_at = duration.map(|duration| Instant::now() + duration);

                        let list = self
                            .blpop_listeners
                            .entry(key.to_string())
                            .or_insert_with(|| Vec::new());
                        list.push(BLPopListener { tx, expires_at });
                        CommandResponse::Blocking((rx, duration))
                    }
                };

                resp_tx
                    .send(response)
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
            }
            Command::Type { key } => {
                let response = match self.map.get(&key) {
                    Some(RedisDataType::String(_)) => RespValue::SimpleString("string".to_string()),
                    Some(RedisDataType::List(_)) => RespValue::SimpleString("list".to_string()),
                    None => RespValue::SimpleString("none".to_string()),
                };

                resp_tx
                    .send(CommandResponse::NonBlocking(response))
                    .map_err(|e| anyhow!("failed to send command response {:?}", e))?;
            }
        }
        Ok(())
    }

    fn notify_blpop_listeners(&mut self, key: &String) -> anyhow::Result<()> {
        match self.blpop_listeners.remove(key) {
            Some(mut listeners) => {
                listeners = listeners
                    .into_iter()
                    .filter_map(|listener| {
                        if let Some(expires_at) = listener.expires_at {
                            if expires_at < Instant::now() {
                                println!("blpop listener has expired");
                                let to_send = RespValue::NullArray;
                                let _ = listener.tx.send(to_send);
                                return None;
                            }
                        }

                        if let Some(RedisDataType::List(list)) = self.map.get_mut(key) {
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
                            Some(listener)
                        }
                    })
                    .collect();

                self.blpop_listeners.insert(key.clone(), listeners);
            }
            _ => {}
        }
        Ok(())
    }
}

async fn accept_listeners(
    listener: TcpListener,
    tx: mpsc::Sender<(Command, oneshot::Sender<CommandResponse>)>,
) -> anyhow::Result<()> {
    loop {
        let tx = tx.clone();
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
                            let (resp_tx, resp_rx) = oneshot::channel();
                            tx.send((command, resp_tx)).await?;

                            let response = resp_rx.await?;
                            let to_send = match response {
                                CommandResponse::NonBlocking(response) => response,
                                CommandResponse::Blocking((rx, duration)) => match duration {
                                    Some(duration) => match timeout(duration, rx).await {
                                        Ok(Ok(value)) => value,
                                        _ => RespValue::NullArray,
                                    },
                                    None => rx.await?,
                                },
                            };

                            connection.write_value(&to_send).await?;
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

fn lpop(key: &String, count: Option<usize>, map: &mut Map) -> RespValue {
    let mut count = count.unwrap_or(1);

    match map.get_mut(key) {
        Some(RedisDataType::List(list)) => {
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
        Some(RedisDataType::String(_)) => RespValue::SimpleError(String::from("Wrong type")),
        None => RespValue::NullBulkString,
    }
}
