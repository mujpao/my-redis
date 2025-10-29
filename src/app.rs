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
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing::{Instrument, info, info_span, instrument, warn};

type Map = HashMap<String, RedisDataType>;

enum RedisDataType {
    String(Box<(String, Option<Instant>)>),
    List(Box<VecDeque<RespValue>>),
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
enum CommandResponse {
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

enum AppEvent {
    KeyExpired { key: String },
    ElementPushedToList { key: String },
    EntryAddedToStream { key: String },
}

struct App {
    map: Map,
    blpop_listeners: HashMap<String, Vec<BLPopListener>>,
    xread_listeners: HashMap<String, VecDeque<XReadListener>>,
    command_rx: mpsc::Receiver<(Command, oneshot::Sender<CommandResponse>)>,
    events_tx: mpsc::Sender<AppEvent>,
    events_rx: mpsc::Receiver<AppEvent>,
    role: Role,
    addr: SocketAddr,
    replication_id: Option<String>,
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
        let (events_tx, events_rx) = mpsc::channel(100);

        let replication_id = if let Role::Primary = role {
            Some(Alphanumeric.sample_string(&mut rand::rng(), 40))
        } else {
            None
        };

        Self {
            map,
            blpop_listeners,
            xread_listeners,
            command_rx,
            events_tx,
            events_rx,
            role,
            addr,
            replication_id,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        self.perform_handshake().await?;

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
    async fn perform_handshake(&mut self) -> anyhow::Result<()> {
        match self.role {
            Role::Primary => Ok(()),
            Role::ReplicaOf(primary) => {
                let stream = TcpStream::connect(primary).await?;
                let mut conn = Connection::new(stream);
                let result = App::send_command(
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

                let port = self.addr.port().to_string();

                let result = App::send_command(
                    RespValue::Array(vec![
                        RespValue::BulkString(String::from("REPLCONF")),
                        RespValue::BulkString(String::from("listening-port")),
                        RespValue::BulkString(String::from(port)),
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

                let result = App::send_command(
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

                let result = App::send_command(
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

                Ok(())
            }
        }
    }

    async fn send_command(command: RespValue, conn: &mut Connection) -> anyhow::Result<RespValue> {
        conn.write_value(&command).await.map_err(|e| {
            warn!(?command, reason = ?e, "Unable to send command to primary");
            anyhow!("unable to send command")
        })?;

        conn.read_value()
            .await?
            .ok_or_else(|| anyhow!("no value read from connection"))
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
            AppEvent::ElementPushedToList { key } => {
                self.notify_blpop_listeners(&key)?;
            }
            AppEvent::EntryAddedToStream { key } => {
                self.notify_xread_listeners(&key).await?;
            }
        }
        Ok(())
    }

    async fn process_command(
        &mut self,
        command: Command,
        resp_tx: oneshot::Sender<CommandResponse>,
    ) -> anyhow::Result<()> {
        let response = self.execute_command(command).await?;
        resp_tx
            .send(response)
            .map_err(|e| anyhow!("failed to send command response {:?}", e))
    }

    #[instrument(skip(self))]
    async fn execute_command(&mut self, command: Command) -> anyhow::Result<CommandResponse> {
        info!("received command");
        Ok(match command {
            Command::Ping => {
                let response = RespValue::SimpleString(String::from("PONG"));
                CommandResponse::NonBlocking(response)
            }
            Command::Echo(s) => {
                let response = RespValue::BulkString(s);
                CommandResponse::NonBlocking(response)
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
                CommandResponse::NonBlocking(response)
            }
            Command::Get { key } => {
                let response = match self.map.get(&key) {
                    Some(RedisDataType::String(value)) => RespValue::BulkString(value.0.clone()),
                    None => RespValue::NullBulkString,
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                CommandResponse::NonBlocking(response)
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

                self.events_tx
                    .send(AppEvent::ElementPushedToList { key })
                    .await?;

                CommandResponse::NonBlocking(response)
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

                self.events_tx
                    .send(AppEvent::ElementPushedToList { key })
                    .await?;
                CommandResponse::NonBlocking(response)
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

                CommandResponse::NonBlocking(RespValue::Array(value))
            }
            Command::LLen { key } => {
                let response = match self.map.get(&key) {
                    Some(RedisDataType::List(list)) => RespValue::Integer(list.len() as i64),
                    None => RespValue::Integer(0),
                    _ => RespValue::SimpleError(String::from("Wrong type")),
                };

                CommandResponse::NonBlocking(response)
            }
            Command::LPop { key, count } => {
                let response = lpop(&key, count, &mut self.map);

                CommandResponse::NonBlocking(response)
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

                match elem {
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
                        CommandResponse::BlockingOneshot((rx, duration))
                    }
                }
            }
            Command::Type { key } => {
                let response = match self.map.get(&key) {
                    Some(RedisDataType::String(_)) => RespValue::SimpleString("string".to_string()),
                    Some(RedisDataType::List(_)) => RespValue::SimpleString("list".to_string()),
                    Some(RedisDataType::Stream(_)) => RespValue::SimpleString("stream".to_string()),
                    None => RespValue::SimpleString("none".to_string()),
                };

                CommandResponse::NonBlocking(response)
            }
            Command::XAdd { key, id, pairs } => {
                let response = match StreamIdInput::from_str(&id) {
                    Ok(id) => {
                        let mut data = Vec::new();
                        for (field, value) in pairs {
                            data.push(StreamData { field, value });
                        }

                        match self.map.get_mut(&key) {
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
                    .send(AppEvent::EntryAddedToStream { key })
                    .await?;

                CommandResponse::NonBlocking(response)
            }
            Command::XRange { key, start, end } => {
                let response = {
                    match self.map.get_mut(&key) {
                        Some(RedisDataType::Stream(stream)) => match stream.range(&start, &end) {
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
                let response = {
                    let mut result = vec![];
                    let mut pairs = vec![];
                    for (key, last_id) in &keys_and_ids {
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
                                            if values.len() > 0 {
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
                            let stream_data = RespValue::Array(vec![
                                RespValue::BulkString(key.into()),
                                stream_data,
                            ]);
                            result.push(stream_data);
                        }
                    }

                    if result.len() == 0 {
                        if let Some(timeout) = timeout {
                            let (tx, rx) = mpsc::channel(1);
                            let (duration, expires_at) = {
                                match timeout {
                                    0 => (None, None),
                                    timeout => {
                                        let duration = Duration::from_millis(timeout);
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
                };

                response
            }
            Command::Incr { key } => {
                let response = match self.map.get_mut(&key) {
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
                    match category.as_str().to_lowercase().as_str() {
                        "replication" => {
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
                        _ => {}
                    }
                }

                let response = RespValue::BulkString(info);

                CommandResponse::NonBlocking(response)
            }
            Command::ReplConf => {
                let response = RespValue::SimpleString(String::from("OK"));
                CommandResponse::NonBlocking(response)
            }
            Command::PSync => {
                let id = match &self.replication_id {
                    Some(id) => id,
                    None => "NULL",
                };
                let data = format!("FULLRESYNC {} 0", id);
                let response = RespValue::SimpleString(data);
                CommandResponse::NonBlocking(response)
            }
        })
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
                expires_at: expires_at,
                last_seen_id: last_id.clone(),
            };

            let listeners = self
                .xread_listeners
                .entry(key.to_string())
                .or_insert_with(|| VecDeque::new());
            listeners.push_back(listener);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn notify_xread_listeners(&mut self, key: &String) -> anyhow::Result<()> {
        match self.xread_listeners.remove(key) {
            Some(mut listeners) => {
                let mut new_listeners = VecDeque::new();

                while !listeners.is_empty() {
                    let listener = listeners
                        .pop_front()
                        .ok_or_else(|| anyhow!("listeners is empty"))?;

                    if listener.tx.is_closed() {
                        info!("xread channel has closed");
                        continue;
                    }

                    if let Some(expires_at) = listener.expires_at {
                        if expires_at < Instant::now() {
                            info!("xread listener has expired");
                            let to_send = RespValue::NullArray;
                            let _ = listener.tx.send(to_send).await;
                            continue;
                        }
                    }

                    if let Some(RedisDataType::Stream(stream)) = self.map.get_mut(key) {
                        match stream.get_after_id(listener.last_seen_id.as_deref()) {
                            Ok(values) => {
                                // Only notify the listener if there are new values in stream
                                if values.len() > 0 {
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
            _ => {}
        }
        Ok(())
    }

    #[instrument(skip(self))]
    fn notify_blpop_listeners(&mut self, key: &String) -> anyhow::Result<()> {
        match self.blpop_listeners.remove(key) {
            Some(mut listeners) => {
                listeners = listeners
                    .into_iter()
                    .filter_map(|listener| {
                        if let Some(expires_at) = listener.expires_at {
                            if expires_at < Instant::now() {
                                info!("blpop listener has expired");
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
        let (stream, socket) = listener.accept().await?;

        let _: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        info!("accepted new connection");
        let mut connection = Connection::new(stream);
            let mut transaction_queue: Option<Vec<Command>> = None;
            loop {
                let value = connection.read_value().await;


                if let Some(value) = value? {
                info!(?value, "got value on connection");

                    match (&mut transaction_queue, Command::try_from(value)) {
                        (Some(_), Ok(Command::Discard)) => {
                            info!("discarding transaction");
                            transaction_queue = None;
                            let response = RespValue::SimpleString(String::from("OK"));
                            connection.write_value(&response).await?;
                        }
                        (None, Ok(Command::Discard)) => {
                            info!("ERR DISCARD without MULTI");

                            let response =
                                RespValue::SimpleError(String::from("ERR DISCARD without MULTI"));
                            connection.write_value(&response).await?;
                        }
                        (Some(queue), Ok(Command::Exec)) => {
                            info!(commands = ?queue, "executing transaction");
                            let command = Command::Transaction {
                                commands: std::mem::take(queue),
                            };
                            transaction_queue = None;
                            let (resp_tx, resp_rx) = oneshot::channel();
                            tx.send((command, resp_tx)).await?;

                            let response = resp_rx.await?;
                            let response = match response {
                                CommandResponse::NonBlocking(response) => response,
                                _ => RespValue::SimpleError(String::from("transaction failed")),
                            };
                            connection.write_value(&response).await?;
                        }
                        (None, Ok(Command::Exec)) => {
                            info!("ERR EXEC without MULTI");
                            let response =
                                RespValue::SimpleError(String::from("ERR EXEC without MULTI"));
                            connection.write_value(&response).await?;
                        }
                        (Some(_), Ok(Command::Multi)) => {
                            info!("ERR MULTI calls can not be nested");
                            let response = RespValue::SimpleError(String::from(
                                "ERR MULTI calls can not be nested",
                            ));
                            connection.write_value(&response).await?;
                        }
                        (Some(queue), Ok(command)) => {
                            info!(?command, "adding command to transaction");
                            queue.push(command);
                            let response = RespValue::SimpleString(String::from("QUEUED"));
                            connection.write_value(&response).await?;
                        }
                        (None, Ok(Command::Multi)) => {
                            info!("starting transaction");
                            transaction_queue = Some(Vec::new());
                            let response = RespValue::SimpleString(String::from("OK"));
                            connection.write_value(&response).await?;
                        }
                        (None, Ok(command)) => {
                            let (resp_tx, resp_rx) = oneshot::channel();
                            tx.send((command, resp_tx)).await?;

                            let response = resp_rx.await?;
                            let to_send = match response {
                                CommandResponse::NonBlocking(response) => response,
                                CommandResponse::BlockingOneshot((rx, duration)) => {
                                    match duration {
                                        Some(duration) => match timeout(duration, rx).await {
                                            Ok(Ok(value)) => value,
                                            _ => RespValue::NullArray,
                                        },
                                        None => rx.await?,
                                    }
                                }
                                CommandResponse::BlockingMpsc((mut rx, duration)) => match duration
                                {
                                    Some(duration) => match timeout(duration, rx.recv()).await {
                                        Ok(Some(value)) => {
                                            info!("got {:?} from mpsc channel", value);
                                            rx.close();
                                            value
                                        }
                                        e => {
                                            info!(
                                                "timeout reached for blocking mpsc receive, {:?}",
                                                e
                                            );
                                            RespValue::NullArray
                                        }
                                    },
                                    None => {
                                        let value = rx
                                            .recv()
                                            .await
                                            .ok_or_else(|| anyhow!("error on blocking receiver"))?;
                                        rx.close();
                                        value
                                    }
                                },
                            };

                            connection.write_value(&to_send).await?;
                        }
                        (_, Err(e)) => {
                            let s = format!("{}", e);
                            let to_send = RespValue::SimpleError(s);
                            connection.write_value(&to_send).await?;
                        }
                    }
                }
            }
        }.instrument(info_span!("client connection", addr = ?socket)));
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
        _ => RespValue::NullBulkString,
    }
}
