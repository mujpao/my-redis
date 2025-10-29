use codecrafters_redis::app::{Role, run};
use redis::aio::MultiplexedConnection;
use tokio::net::TcpListener;
use tracing_subscriber::{filter::EnvFilter, filter::LevelFilter, fmt, layer::SubscriberExt};

fn setup_tracing() {
    let subscriber = tracing_subscriber::registry().with(fmt::layer()).with(
        EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy(),
    );

    let _ = tracing::subscriber::set_global_default(subscriber);
}

pub async fn setup() -> u16 {
    setup_tracing();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let _ = tokio::spawn(async { run(listener, Role::Primary).await });

    port
}

pub async fn setup_replica(primary_port: u16) -> MultiplexedConnection {
    setup_tracing();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_port = listener.local_addr().unwrap().port();

    let primary_addr = format!("127.0.0.1:{}", primary_port).parse().unwrap();
    let _ = tokio::spawn(async move { run(listener, Role::ReplicaOf(primary_addr)).await });

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", replica_port)).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}
