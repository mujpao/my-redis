use crate::resp::RespValue;
use anyhow::anyhow;
use connection::Connection;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

mod connection;
mod resp;

pub async fn run(listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        println!("accepted new connection");

        let _: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut connection = Connection::new(stream);
            loop {
                let value = connection.read_value().await;

                println!("value: {:?}", value);
                if let Some(value) = value? {
                    println!("read {:?}", value);

                    match Command::try_from(value)? {
                        Command::Ping => {
                            let to_send = RespValue::SimpleString(String::from("PONG"));
                            connection.write_value(&to_send).await?;
                        } // Command::Echo(s) => {
                        //     let to_send = RespValue::BulkString(s);
                        //     connection.write_value(&to_send).await?;
                        // }
                        Command::Unknown => {
                            let to_send = RespValue::SimpleString(String::from("HELLO"));
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
    // Echo(String),
    Unknown,
}

impl TryFrom<RespValue> for Command {
    type Error = anyhow::Error;

    fn try_from(value: RespValue) -> Result<Self, Self::Error> {
        match value {
            RespValue::Array(ref data) => {
                if data.len() == 0 {
                    return Err(anyhow!("invalid command {:?}", value));
                }
                if let RespValue::BulkString(command_name) = &data[0] {
                    match command_name.as_str().to_uppercase().as_str() {
                        "PING" => Ok(Command::Ping),
                        _ => {
                            println!("unknown command {:?}", value);
                            Ok(Command::Unknown)
                        }
                    }
                } else {
                    return Err(anyhow!("invalid command {:?}", value));
                }
            }
            _ => Err(anyhow!("respvalue not an array {:?}", value)),
        }
    }
}
