use codecrafters_redis::app::{App, run};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let app = App::new();

    run(app, listener).await
}
