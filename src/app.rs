use crate::client_connection::{ClientConnection, ConnCommand};
use crate::command::Command;
use crate::connection::Connection;
use crate::resp::RespValue;
use crate::stream::{Stream, StreamData, StreamIdInput};
use anyhow::anyhow;
use rand::distr::{Alphanumeric, SampleString};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tracing::{Instrument, info, info_span, instrument, warn};

type Map = HashMap<String, RedisDataType>;

enum RedisDataType {
    String(Box<(String, Option<Instant>)>),
    List(VecDeque<RespValue>),
    Stream(Box<Stream>),
}

#[derive(Debug)]
pub enum Role {
    Primary,
    ReplicaOf(SocketAddr),
}

#[derive(Debug)]
struct BLPopListener {
    tx: oneshot::Sender<RespValue>,
    expires_at: Option<Instant>,
}

#[derive(Debug)]
struct XReadListener {
    tx: mpsc::Sender<RespValue>,
    expires_at: Option<Instant>,
    last_seen_id: Option<String>,
}

#[derive(Debug)]
pub enum CommandResponse {
    NonBlocking(RespValue),
    BlockingOneshot((oneshot::Receiver<RespValue>, Option<Duration>)),
    BlockingMpsc((mpsc::Receiver<RespValue>, Option<Duration>)),
}

#[instrument]
pub async fn run(listener: TcpListener, role: Role) -> anyhow::Result<()> {
    info!("starting up redis instance");

    let addr = listener.local_addr()?;

    let (command_tx, command_rx) = mpsc::channel(100);

    let mut app = App::new(addr, command_rx, role);

    tokio::spawn(async move {
        accept_listeners(listener, command_tx).await.unwrap();
    });

    app.run().await
}

#[derive(Debug)]
enum AppEvent {
    KeyExpired {
        key: String,
    },
    ElementPushedToList {
        key: String,
    },
    EntryAddedToStream {
        key: String,
    },
    FullResyncWithReplica {
        repl_id: Option<String>,
        offset: i64,
        replica_addr: SocketAddr,
    },
    GotPropagatedCommand {
        command: Command,
    },
}

struct App {
    map: Map,
    blpop_listeners: HashMap<String, Vec<BLPopListener>>,
    xread_listeners: HashMap<String, VecDeque<XReadListener>>,
    replica_connections: HashMap<SocketAddr, mpsc::Sender<ConnCommand>>,
    command_rx: mpsc::Receiver<(Command, oneshot::Sender<CommandResponse>)>,
    events_tx: mpsc::Sender<AppEvent>,
    events_rx: mpsc::Receiver<AppEvent>,
    role: Role,
    addr: SocketAddr,
    replication_id: Option<String>,
    rdb_data: Vec<u8>,
}

impl App {
    fn new(
        addr: SocketAddr,
        command_rx: mpsc::Receiver<(Command, oneshot::Sender<CommandResponse>)>,
        role: Role,
    ) -> Self {
        let map = HashMap::new();
        let blpop_listeners = HashMap::new();
        let xread_listeners = HashMap::new();
        let replica_connections = HashMap::new();
        let (events_tx, events_rx) = mpsc::channel(100);

        let replication_id = if let Role::Primary = role {
            Some(Alphanumeric.sample_string(&mut rand::rng(), 40))
        } else {
            None
        };

        let empty_rdb_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let rdb_data = hex::decode(empty_rdb_file_hex).unwrap();

        Self {
            map,
            blpop_listeners,
            xread_listeners,
            replica_connections,
            command_rx,
            events_tx,
            events_rx,
            role,
            addr,
            replication_id,
            rdb_data,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        if let Role::ReplicaOf(primary) = self.role {
            let conn = perform_handshake_from_replica(primary, self.addr.port()).await?;

            let events_tx = self.events_tx.clone();
            tokio::spawn(
                async move {
                    if let Err(e) = listen_for_propagated_commands(conn, events_tx).await {
                        warn!("error getting propagated command, {}", e);
                    }
                }
                .instrument(info_span!("connection to primary", addr = ?primary)),
            );
        }

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

    #[instrument(skip(self))]
    async fn process_event(&mut self, event: AppEvent) -> anyhow::Result<()> {
        match event {
            AppEvent::KeyExpired { key } => {
                if let Some(RedisDataType::String(b)) = self.map.get(&key)
                    && let (_, Some(key_expires_at)) = **b
                    && key_expires_at < Instant::now()
                {
                    self.map.remove(&key);
                }
            }
            AppEvent::ElementPushedToList { key } => {
                self.notify_blpop_listeners(&key)?;
            }
            AppEvent::EntryAddedToStream { key } => {
                self.notify_xread_listeners(&key).await?;
            }
            AppEvent::FullResyncWithReplica {
                repl_id,
                offset,
                replica_addr,
            } => {
                if let Some(conn_tx) = self.replica_connections.get(&replica_addr) {
                    conn_tx
                        .send(ConnCommand::FullResync {
                            repl_id,
                            offset,
                            replica_addr,
                            data: self.rdb_data.clone(),
                        })
                        .await?;
                } else {
                    warn!("replica addr not in connections, {}", replica_addr);
                }
            }
            AppEvent::GotPropagatedCommand { command } => {
                self.process_propagated_command(command).await?;
            }
        }
        Ok(())
    }

    async fn process_command(
        &mut self,
        command: Command,
        resp_tx: oneshot::Sender<CommandResponse>,
    ) -> anyhow::Result<()> {
        let response = self.execute_command(&command).await?;

        if command.is_write() {
            for conn_tx in self.replica_connections.values() {
                tracing::info!(command = ?command, "propagating command");
                conn_tx
                    .send(ConnCommand::PropagateCommand {
                        command: command.clone(),
                    })
                    .await?;
            }
        }

        resp_tx
            .send(response)
            .map_err(|e| anyhow!("failed to send command response {:?}", e))
    }

    async fn process_propagated_command(&mut self, command: Command) -> anyhow::Result<()> {
        self.execute_command(&command).await?;
        Ok(())
    }

    #[instrument(skip(self), fields(role = ?self.role))]
    async fn execute_command(&mut self, command: &Command) -> anyhow::Result<CommandResponse> {
        info!("received command");
        let result = match command {
            Command::Ping => {
                let response = RespValue::SimpleString(String::from("PONG"));
                CommandResponse::NonBlocking(response)
            }
            Command::Echo(s) => {
                let response = RespValue::BulkString(s.clone());
                CommandResponse::NonBlocking(response)
            }
            Command::Set {
                key,
                value,
                expiry_duration,
            } => {
                let expiry_time = expiry_duration.map(|duration| Instant::now() + duration);

                self.map.insert(
                    key.to_string(),
                    RedisDataType::String(Box::new((value.to_string(), expiry_time))),
                );

                let tx_cloned = self.events_tx.clone();

                if let Some(duration) = expiry_duration {
                    let duration = *duration;
                    let key = key.clone();
                    tokio::spawn(async move {
                        sleep(duration).await;
                        tx_cloned.send(AppEvent::KeyExpired { key }).await?;

                        Ok::<(), anyhow::Error>(())
                    });
                }

                let response = RespValue::SimpleString(String::from("OK"));
                CommandResponse::NonBlocking(response)
            }
            Command::Get { key } => {
                let response = match self.map.get(key) {
                    Some(RedisDataType::String(value)) => RespValue::BulkString(value.0.clone()),
                    None => RespValue::NullBulkString,
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                CommandResponse::NonBlocking(response)
            }
            Command::RPush { key, elements } => {
                let response = match self.map.get_mut(key) {
                    Some(RedisDataType::List(list)) => {
                        list.extend(elements.clone());
                        RespValue::Integer(list.len() as i64)
                    }
                    None => {
                        let len = elements.len();
                        self.map.insert(
                            key.to_string(),
                            RedisDataType::List(VecDeque::from(elements.clone())),
                        );
                        RespValue::Integer(len as i64)
                    }
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                self.events_tx
                    .send(AppEvent::ElementPushedToList { key: key.clone() })
                    .await?;

                CommandResponse::NonBlocking(response)
            }
            Command::LPush { key, elements } => {
                let response = match self.map.get_mut(key) {
                    Some(RedisDataType::List(list)) => {
                        for elem in elements {
                            list.push_front(elem.clone());
                        }
                        RespValue::Integer(list.len() as i64)
                    }
                    None => {
                        let mut list = VecDeque::new();
                        for elem in elements {
                            list.push_front(elem.clone());
                        }

                        let len = list.len();

                        self.map.insert(key.to_string(), RedisDataType::List(list));

                        RespValue::Integer(len as i64)
                    }
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                self.events_tx
                    .send(AppEvent::ElementPushedToList { key: key.clone() })
                    .await?;
                CommandResponse::NonBlocking(response)
            }
            Command::LRange { key, start, stop } => {
                let value = match self.map.get(key) {
                    Some(RedisDataType::List(entry)) => {
                        let start: usize = if *start < 0 {
                            std::cmp::max(start + entry.len() as i64, 0) as usize
                        } else {
                            *start as usize
                        };

                        let mut stop: usize = if *stop < 0 {
                            std::cmp::max(stop + entry.len() as i64, 0) as usize
                        } else {
                            *stop as usize
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

                CommandResponse::NonBlocking(RespValue::Array(value))
            }
            Command::LLen { key } => {
                let response = match self.map.get(key) {
                    Some(RedisDataType::List(list)) => RespValue::Integer(list.len() as i64),
                    None => RespValue::Integer(0),
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                CommandResponse::NonBlocking(response)
            }
            Command::LPop { key, count } => {
                let response = lpop(key, *count, &mut self.map);

                CommandResponse::NonBlocking(response)
            }
            Command::BLPop { key, timeout } => {
                let elem = self.map.get_mut(key).and_then(|data| match data {
                    RedisDataType::List(list) => list.pop_front(),
                    _ => None,
                });

                match elem {
                    Some(elem) => CommandResponse::NonBlocking(RespValue::Array(vec![
                        RespValue::BulkString(key.clone()),
                        elem,
                    ])),
                    None => {
                        let (tx, rx) = oneshot::channel();
                        let duration = timeout.map(Duration::from_secs_f64);

                        let expires_at = duration.map(|duration| Instant::now() + duration);

                        let list = self.blpop_listeners.entry(key.to_string()).or_default();
                        list.push(BLPopListener { tx, expires_at });
                        CommandResponse::BlockingOneshot((rx, duration))
                    }
                }
            }
            Command::Type { key } => {
                let response = match self.map.get(key) {
                    Some(RedisDataType::String(..)) => {
                        RespValue::SimpleString("string".to_string())
                    }
                    Some(RedisDataType::List(_)) => RespValue::SimpleString("list".to_string()),
                    Some(RedisDataType::Stream(_)) => RespValue::SimpleString("stream".to_string()),
                    None => RespValue::SimpleString("none".to_string()),
                };

                CommandResponse::NonBlocking(response)
            }
            Command::XAdd { key, id, pairs } => {
                let response = match StreamIdInput::from_str(id) {
                    Ok(id) => {
                        let mut data = Vec::new();
                        for (field, value) in pairs {
                            data.push(StreamData {
                                field: field.clone(),
                                value: value.clone(),
                            });
                        }

                        match self.map.get_mut(key) {
                            Some(RedisDataType::Stream(stream)) => match stream.append(id, data) {
                                Ok(id) => RespValue::BulkString(id.to_string()),
                                Err(e) => RespValue::SimpleError(e.to_string()),
                            },
                            None => {
                                let mut stream = Stream::new();
                                match stream.append(id, data) {
                                    Ok(id) => {
                                        self.map.insert(
                                            key.clone(),
                                            RedisDataType::Stream(Box::new(stream)),
                                        );
                                        RespValue::BulkString(id.to_string())
                                    }
                                    Err(e) => RespValue::SimpleError(e.to_string()),
                                }
                            }
                            _ => RespValue::SimpleError(String::from("Wrong type")),
                        }
                    }
                    Err(e) => {
                        info!(reason = ?e, "invalid stream id");
                        RespValue::SimpleError(String::from("unable to parse stream id"))
                    }
                };

                self.events_tx
                    .send(AppEvent::EntryAddedToStream { key: key.clone() })
                    .await?;

                CommandResponse::NonBlocking(response)
            }
            Command::XRange { key, start, end } => {
                let response = {
                    match self.map.get_mut(key) {
                        Some(RedisDataType::Stream(stream)) => match stream.range(start, end) {
                            Ok(values) => values,
                            Err(e) => RespValue::SimpleError(e.to_string()),
                        },
                        None => RespValue::SimpleError(String::from("Stream not found")),
                        _ => RespValue::SimpleError(String::from("Wrong type")),
                    }
                };

                CommandResponse::NonBlocking(response)
            }
            Command::XRead {
                keys_and_ids,
                timeout,
            } => {
                let mut result = vec![];
                let mut pairs = vec![];
                for (key, last_id) in keys_and_ids {
                    let stream_data = {
                        match self.map.get_mut(key) {
                            Some(RedisDataType::Stream(stream)) => {
                                let last_id = match last_id.as_str() {
                                    "$" => stream.get_last_id(),
                                    id => Some(id.to_string()),
                                };

                                pairs.push((key.to_string(), last_id.clone()));

                                match stream.get_after_id(last_id.as_deref()) {
                                    Ok(values) => {
                                        if !values.is_empty() {
                                            RespValue::Array(values)
                                        } else {
                                            RespValue::NullArray
                                        }
                                    }
                                    Err(e) => RespValue::SimpleError(e.to_string()),
                                }
                            }
                            None => RespValue::SimpleError(String::from("Stream not found")),
                            _ => RespValue::SimpleError(String::from("Key is not a stream")),
                        }
                    };

                    if let RespValue::Array(_) = stream_data {
                        let stream_data =
                            RespValue::Array(vec![RespValue::BulkString(key.into()), stream_data]);
                        result.push(stream_data);
                    }
                }

                if result.is_empty() {
                    if let Some(timeout) = timeout {
                        let (tx, rx) = mpsc::channel(1);
                        let (duration, expires_at) = {
                            match timeout {
                                0 => (None, None),
                                timeout => {
                                    let duration = Duration::from_millis(*timeout);
                                    let expires_at = Instant::now() + duration;
                                    (Some(duration), Some(expires_at))
                                }
                            }
                        };

                        self.add_xread_listeners(&pairs, tx, expires_at)?;

                        CommandResponse::BlockingMpsc((rx, duration))
                    } else {
                        CommandResponse::NonBlocking(RespValue::NullArray)
                    }
                } else {
                    CommandResponse::NonBlocking(RespValue::Array(result))
                }
            }
            Command::Incr { key } => {
                let response = match self.map.get_mut(key) {
                    Some(RedisDataType::String(b)) => match b.0.parse::<i64>() {
                        Ok(int_value) => {
                            let new_value = int_value + 1;
                            b.0 = (new_value).to_string();
                            RespValue::Integer(new_value)
                        }
                        Err(_) => RespValue::SimpleError(String::from(
                            "ERR value is not an integer or out of range",
                        )),
                    },
                    None => {
                        let new_value = String::from("1");
                        self.map.insert(
                            key.to_string(),
                            RedisDataType::String(Box::new((new_value.clone(), None))),
                        );
                        RespValue::Integer(1)
                    }
                    _ => RespValue::SimpleError(String::from(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )),
                };

                CommandResponse::NonBlocking(response)
            }
            Command::Multi => {
                warn!("got multi in process_command");
                let response = RespValue::SimpleString(String::from("OK"));
                CommandResponse::NonBlocking(response)
            }
            Command::Exec => {
                warn!("got exec in process_command");
                let response = RespValue::SimpleError(String::from("ERR EXEC without MULTI"));
                CommandResponse::NonBlocking(response)
            }
            Command::Transaction { commands } => {
                let mut responses = vec![];
                for command in commands {
                    let response = match Box::pin(self.execute_command(command)).await {
                        Ok(CommandResponse::NonBlocking(response)) => response,
                        Ok(_) => RespValue::NullBulkString,
                        Err(e) => {
                            warn!(reason= ?e, "error during transaction");
                            RespValue::SimpleError(String::from("Unable to execute command"))
                        }
                    };

                    responses.push(response);
                }
                CommandResponse::NonBlocking(RespValue::Array(responses))
            }
            Command::Discard => {
                warn!("got Discard in process_command");
                let response = RespValue::SimpleError(String::from("ERR DISCARD without MULTI"));
                CommandResponse::NonBlocking(response)
            }
            Command::Info { categories } => {
                let mut info = String::new();
                for category in categories {
                    if category.as_str().to_lowercase().as_str() == "replication" {
                        let data = match (&self.role, &self.replication_id) {
                            (Role::Primary, Some(replication_id)) => {
                                format!(
                                    "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                                    replication_id, 0
                                )
                            }
                            (Role::Primary, None) => {
                                warn!("primary instance doesn't have a replication id");
                                format!(
                                    "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                                    "NULL", 0
                                )
                            }
                            (Role::ReplicaOf(_), _) => "role:slave".to_string(),
                        };
                        info.push_str(&data);
                    }
                }

                let response = RespValue::BulkString(info);

                CommandResponse::NonBlocking(response)
            }
            Command::ReplConf { replica_addr, tx } => {
                if let (Some(replica_addr), Some(tx)) = (replica_addr, tx) {
                    self.replica_connections.insert(*replica_addr, tx.clone());
                } else {
                    warn!(?replica_addr, "got replconf without tx and/or replica_addr");
                }

                let response = RespValue::SimpleString(String::from("OK"));
                CommandResponse::NonBlocking(response)
            }
            Command::PSync {
                repl_id,
                offset,
                replica_addr,
            } => {
                if !(repl_id.is_none() && *offset == -1) {
                    warn!(
                        ?repl_id,
                        ?offset,
                        "psync with repl_id and offset not implemented yet"
                    );

                    CommandResponse::NonBlocking(RespValue::SimpleError(String::from(
                        "psync not fully implemented yet",
                    )))
                } else {
                    let id = match &self.replication_id {
                        Some(id) => id,
                        None => "NULL",
                    };
                    if let Some(replica_addr) = replica_addr {
                        self.events_tx
                            .send(AppEvent::FullResyncWithReplica {
                                repl_id: repl_id.clone(),
                                offset: *offset,
                                replica_addr: *replica_addr,
                            })
                            .await?;
                    } else {
                        warn!(?replica_addr, "got PSync without replica_addr");
                    }

                    let data = format!("FULLRESYNC {} 0", id);
                    let response = RespValue::SimpleString(data);
                    CommandResponse::NonBlocking(response)
                }
            }
        };

        Ok(result)
    }

    fn add_xread_listeners(
        &mut self,
        pairs: &Vec<(String, Option<String>)>,
        tx: mpsc::Sender<RespValue>,
        expires_at: Option<Instant>,
    ) -> anyhow::Result<()> {
        for (key, last_id) in pairs {
            let listener = XReadListener {
                tx: tx.clone(),
                expires_at,
                last_seen_id: last_id.clone(),
            };

            let listeners = self.xread_listeners.entry(key.to_string()).or_default();
            listeners.push_back(listener);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn notify_xread_listeners(&mut self, key: &String) -> anyhow::Result<()> {
        if let Some(mut listeners) = self.xread_listeners.remove(key) {
            let mut new_listeners = VecDeque::new();

            while !listeners.is_empty() {
                let listener = listeners
                    .pop_front()
                    .ok_or_else(|| anyhow!("listeners is empty"))?;

                if listener.tx.is_closed() {
                    info!("xread channel has closed");
                    continue;
                }

                if let Some(expires_at) = listener.expires_at
                    && expires_at < Instant::now()
                {
                    info!("xread listener has expired");
                    let to_send = RespValue::NullArray;
                    let _ = listener.tx.send(to_send).await;
                    continue;
                }

                if let Some(RedisDataType::Stream(stream)) = self.map.get_mut(key) {
                    match stream.get_after_id(listener.last_seen_id.as_deref()) {
                        Ok(values) => {
                            // Only notify the listener if there are new values in stream
                            if !values.is_empty() {
                                let to_send = RespValue::Array(vec![RespValue::Array(vec![
                                    RespValue::BulkString(key.into()),
                                    RespValue::Array(values),
                                ])]);

                                let _ = listener.tx.send(to_send).await;
                                continue;
                            }
                        }
                        Err(e) => {
                            info!(reason=?e,"error getting stream");
                        }
                    }
                }

                new_listeners.push_back(listener);
            }

            self.xread_listeners.insert(key.clone(), new_listeners);
        }
        Ok(())
    }

    #[instrument(skip(self))]
    fn notify_blpop_listeners(&mut self, key: &String) -> anyhow::Result<()> {
        if let Some(mut listeners) = self.blpop_listeners.remove(key) {
            listeners = listeners
                .into_iter()
                .filter_map(|listener| {
                    if let Some(expires_at) = listener.expires_at
                        && expires_at < Instant::now()
                    {
                        info!("blpop listener has expired");
                        let to_send = RespValue::NullArray;
                        let _ = listener.tx.send(to_send);
                        return None;
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
        Ok(())
    }
}

async fn accept_listeners(
    listener: TcpListener,
    tx: mpsc::Sender<(Command, oneshot::Sender<CommandResponse>)>,
) -> anyhow::Result<()> {
    loop {
        let command_tx = tx.clone();
        let (stream, socket) = listener.accept().await?;

        tokio::spawn(
            async move {
                info!("accepted new connection");
                let mut client_connection = ClientConnection::new(stream, command_tx);
                client_connection.run().await
            }
            .instrument(info_span!("client connection", addr = ?socket)),
        );
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
        Some(RedisDataType::String(..)) => RespValue::SimpleError(String::from("Wrong type")),
        _ => RespValue::NullBulkString,
    }
}

#[instrument]
async fn perform_handshake_from_replica(
    primary: SocketAddr,
    port: u16,
) -> anyhow::Result<Connection> {
    let stream = TcpStream::connect(primary).await?;
    let mut conn = Connection::new(stream);
    let result = send_command_from_replica(
        RespValue::Array(vec![RespValue::BulkString(String::from("PING"))]),
        &mut conn,
    )
    .await?;

    if result != RespValue::SimpleString(String::from("PONG")) {
        warn!(?primary, ?result, "unable to handshake with primary");
        return Err(anyhow!("unable to handshake with primary redis instance"));
    } else {
        info!(?primary, "got pong from primary");
    }

    let result = send_command_from_replica(
        RespValue::Array(vec![
            RespValue::BulkString(String::from("REPLCONF")),
            RespValue::BulkString(String::from("listening-port")),
            RespValue::BulkString(port.to_string()),
        ]),
        &mut conn,
    )
    .await?;

    if result != RespValue::SimpleString(String::from("OK")) {
        warn!(?primary, ?result, "unable to handshake with primary");
        return Err(anyhow!("unable to handshake with primary redis instance"));
    } else {
        info!(?primary, "got OK from primary");
    }

    let result = send_command_from_replica(
        RespValue::Array(vec![
            RespValue::BulkString(String::from("REPLCONF")),
            RespValue::BulkString(String::from("capa")),
            RespValue::BulkString(String::from("psync2")),
        ]),
        &mut conn,
    )
    .await?;

    if result != RespValue::SimpleString(String::from("OK")) {
        warn!(?primary, ?result, "unable to handshake with primary");
        return Err(anyhow!("unable to handshake with primary redis instance"));
    } else {
        info!(?primary, "got OK from primary");
    }

    let result = send_command_from_replica(
        RespValue::Array(vec![
            RespValue::BulkString(String::from("PSYNC")),
            RespValue::BulkString(String::from("?")),
            RespValue::BulkString(String::from("-1")),
        ]),
        &mut conn,
    )
    .await?;

    if let RespValue::SimpleString(_) = result {
        info!(?primary, ?result, "got response to psync");
    } else {
        warn!(?primary, ?result, "unable to handshake with primary");
        return Err(anyhow!("unable to handshake with primary redis instance"));
    }

    let _ = conn.read_rdb_data().await;

    Ok(conn)
}

async fn send_command_from_replica(
    command: RespValue,
    conn: &mut Connection,
) -> anyhow::Result<RespValue> {
    conn.write_value(&command)
        .await
        .map_err(|e| anyhow!("unable to send command {}", e))?;

    conn.read_value()
        .await?
        .ok_or_else(|| anyhow!("no value read from connection"))
}

async fn listen_for_propagated_commands(
    mut conn: Connection,
    events_tx: mpsc::Sender<AppEvent>,
) -> anyhow::Result<()> {
    info!("starting to listen for propagated commands");
    loop {
        let command = conn
            .read_value()
            .await?
            .ok_or_else(|| anyhow!("no value read from connection"))?
            .try_into()
            .map_err(|e| anyhow!("{}", e))?;

        let _ = events_tx
            .send(AppEvent::GotPropagatedCommand { command })
            .await;
    }
}
