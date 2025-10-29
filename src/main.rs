use codecrafters_redis::app::run;
use tokio::net::TcpListener;
use tracing_subscriber;

use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind(("127.0.0.1", args.port)).await?;

    run(listener).await
}
