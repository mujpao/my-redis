use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

mod parse;

pub async fn run(listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        println!("accepted new connection");

        let _: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            loop {
                let mut buffer = [0; 20];
                stream.read(&mut buffer).await?;
                println!("read {:?}", buffer);

                stream.write_all(b"+PONG\r\n").await?;
            }
        });
    }
}
