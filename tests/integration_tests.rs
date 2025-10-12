use codecrafters_redis::run;
use tokio::net::TcpListener;

#[tokio::test]
async fn handle_pings_from_multiple_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async {
        run(listener).await.unwrap();
    });

    let client1 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn1 = client1.get_connection().unwrap();

    let client2 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn2 = client2.get_connection().unwrap();

    let data: String = redis::cmd("PING")
        .query(&mut conn1)
        .expect("failed to execute PING");
    assert_eq!(data, "PONG");

    let data: String = redis::cmd("PING")
        .query(&mut conn2)
        .expect("failed to execute PING");
    assert_eq!(data, "PONG");

    let data: String = redis::cmd("PING")
        .query(&mut conn1)
        .expect("failed to execute PING");
    assert_eq!(data, "PONG");
}
