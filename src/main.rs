use clap::Parser;
use codecrafters_redis::app::{Role, run};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
    #[arg(short, long = "replicaof", value_name = "\"MASTER_HOST MASTER_PORT\"", value_parser = validate_primary_address)]
    replica_of: Option<SocketAddr>,
}

fn validate_primary_address(s: &str) -> Result<SocketAddr, String> {
    let s: Vec<&str> = s.split(' ').collect();
    if s.len() != 2 {
        return Err("Unable to parse ip address of primary".to_string());
    }
    let ip_addr: Ipv4Addr = match s[0] {
        "localhost" => Ipv4Addr::LOCALHOST,
        addr => addr
            .parse()
            .map_err(|_| format!("Unable to parse ip address`{}`", s[0]))?,
    };

    let port: u16 = s[1]
        .parse()
        .map_err(|_| format!("Unable to parse port`{}`", s[1]))?;

    Ok(SocketAddr::new(IpAddr::V4(ip_addr), port))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let role = {
        match args.replica_of {
            Some(replica_addr) => Role::ReplicaOf(replica_addr),
            None => Role::Primary,
        }
    };

    let listener = TcpListener::bind(("127.0.0.1", args.port)).await?;

    run(listener, role).await
}
