use crate::app::CommandResponse;
use crate::command::Command;
use crate::connection::Connection;
use crate::resp::RespValue;
use anyhow::anyhow;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::{info, warn};

pub enum ConnCommand {
    FullResync {
        repl_id: Option<String>,
        offset: i64,
        replica_addr: SocketAddr,
        data: Vec<u8>,
    },
}

/// Handles communication from the redis instance to a client or replica instance
pub struct ClientConnection {
    connection: Connection,
    transaction_queue: Option<Vec<Command>>,
    client_is_replica: bool,
    command_tx: mpsc::Sender<(Command, oneshot::Sender<CommandResponse>)>,
    events_tx: mpsc::Sender<ConnCommand>,
    events_rx: mpsc::Receiver<ConnCommand>,
}

impl ClientConnection {
    pub fn new(
        stream: TcpStream,
        command_tx: mpsc::Sender<(Command, oneshot::Sender<CommandResponse>)>,
    ) -> Self {
        let connection = Connection::new(stream);
        let transaction_queue: Option<Vec<Command>> = None;

        let (events_tx, events_rx) = mpsc::channel(100);

        Self {
            connection,
            transaction_queue,
            client_is_replica: false,
            command_tx,
            events_tx,
            events_rx,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            select![
                maybe_value = self.connection.read_value() => {
                    if let Some(value) = maybe_value? {
                        self.handle_resp_value(value).await?;
                    }
                }
                maybe_event = self.events_rx.recv() => {
                    match maybe_event {
                         Some(ConnCommand::FullResync { replica_addr, data,..})   => {
                            if self.connection.get_client_addr()? != replica_addr {
                                warn!(?replica_addr, "replica addr doesn't match");
                                continue;
                            }

                            self.connection.write_rdb_data(&data).await?;
                          },
                         None  => {
                             return Err(anyhow!("event channel closed"));
                         },
                    }
                }
            ];
        }
    }

    fn pre_process_command(&mut self, command: Command) -> Command {
        match command {
            Command::ReplConf { .. } => {
                self.client_is_replica = true;
                let addr = match self.connection.get_client_addr() {
                    Ok(addr) => Some(addr),
                    Err(e) => {
                        warn!(reason = ?e, "unable to get replconf client addr");
                        None
                    }
                };
                Command::ReplConf {
                    replica_addr: addr,
                    tx: Some(self.events_tx.clone()),
                }
            }
            Command::PSync {
                repl_id, offset, ..
            } => {
                self.client_is_replica = true;
                let addr = match self.connection.get_client_addr() {
                    Ok(addr) => Some(addr),
                    Err(e) => {
                        warn!(reason = ?e, "unable to get client addr");
                        None
                    }
                };

                Command::PSync {
                    repl_id,
                    offset,
                    replica_addr: addr,
                }
            }
            command => command,
        }
    }

    async fn handle_resp_value(&mut self, value: RespValue) -> anyhow::Result<()> {
        info!(?value, "got value on connection");

        let command = match Command::try_from(value) {
            Ok(command) => self.pre_process_command(command),
            Err(e) => {
                let s = format!("{}", e);
                let to_send = RespValue::SimpleError(s);
                self.connection.write_value(&to_send).await?;
                return Ok(());
            }
        };

        match (&mut self.transaction_queue, command) {
            (Some(_), Command::Discard) => {
                info!("discarding transaction");
                self.transaction_queue = None;
                let response = RespValue::SimpleString(String::from("OK"));
                self.connection.write_value(&response).await?;
            }
            (None, Command::Discard) => {
                info!("ERR DISCARD without MULTI");

                let response = RespValue::SimpleError(String::from("ERR DISCARD without MULTI"));
                self.connection.write_value(&response).await?;
            }
            (Some(queue), Command::Exec) => {
                info!(commands = ?queue, "executing transaction");
                let command = Command::Transaction {
                    commands: std::mem::take(queue),
                };
                self.transaction_queue = None;
                let (resp_tx, resp_rx) = oneshot::channel();
                self.command_tx.send((command, resp_tx)).await?;

                let response = resp_rx.await?;
                let response = match response {
                    CommandResponse::NonBlocking(response) => response,
                    _ => RespValue::SimpleError(String::from("transaction failed")),
                };
                self.connection.write_value(&response).await?;
            }
            (None, Command::Exec) => {
                info!("ERR EXEC without MULTI");
                let response = RespValue::SimpleError(String::from("ERR EXEC without MULTI"));
                self.connection.write_value(&response).await?;
            }
            (Some(_), Command::Multi) => {
                info!("ERR MULTI calls can not be nested");
                let response =
                    RespValue::SimpleError(String::from("ERR MULTI calls can not be nested"));
                self.connection.write_value(&response).await?;
            }
            (Some(queue), command) => {
                info!(?command, "adding command to transaction");
                queue.push(command);
                let response = RespValue::SimpleString(String::from("QUEUED"));
                self.connection.write_value(&response).await?;
            }
            (None, Command::Multi) => {
                info!("starting transaction");
                self.transaction_queue = Some(Vec::new());
                let response = RespValue::SimpleString(String::from("OK"));
                self.connection.write_value(&response).await?;
            }

            (None, command) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                self.command_tx.send((command, resp_tx)).await?;

                let response = resp_rx.await?;
                let to_send = match response {
                    CommandResponse::NonBlocking(response) => response,
                    CommandResponse::BlockingOneshot((rx, duration)) => match duration {
                        Some(duration) => match timeout(duration, rx).await {
                            Ok(Ok(value)) => value,
                            _ => RespValue::NullArray,
                        },
                        None => rx.await?,
                    },
                    CommandResponse::BlockingMpsc((mut rx, duration)) => match duration {
                        Some(duration) => match timeout(duration, rx.recv()).await {
                            Ok(Some(value)) => {
                                info!("got {:?} from mpsc channel", value);
                                rx.close();
                                value
                            }
                            e => {
                                info!("timeout reached for blocking mpsc receive, {:?}", e);
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

                self.connection.write_value(&to_send).await?;
            }
        }

        Ok(())
    }
}
