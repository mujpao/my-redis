use codecrafters_redis::app::run;
use tokio::net::TcpListener;
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    run(listener).await
}
