#![allow(unused_imports)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        println!("accepted new connection");
        loop {
            let mut buffer = [0; 20];

            stream.read(&mut buffer).await?;
            println!("read {:?}", buffer);

            stream.write_all(b"+PONG\r\n").await?;
        }
    }
}
