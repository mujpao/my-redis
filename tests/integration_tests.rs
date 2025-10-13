use codecrafters_redis::run;
use tokio::net::TcpListener;

async fn setup() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _ = tokio::spawn(async {
        run(listener).await.unwrap();
    });

    port
}

#[tokio::test]
async fn handle_pings_from_multiple_connections() {
    let port = setup().await;
    let connection_str = format!("redis://127.0.0.1:{}/", port);
    let client1 = redis::Client::open(connection_str.clone()).unwrap();

    let mut conn1 = client1.get_multiplexed_async_connection().await.unwrap();

    let client2 = redis::Client::open(connection_str).unwrap();
    let mut conn2 = client2.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("PING")
        .query_async(&mut conn1)
        .await
        .expect("failed to execute PING");
    assert_eq!(data, "PONG");

    let data: String = redis::cmd("PING")
        .query_async(&mut conn2)
        .await
        .expect("failed to execute PING");
    assert_eq!(data, "PONG");

    let data: String = redis::cmd("PING")
        .query_async(&mut conn1)
        .await
        .expect("failed to execute PING");
    assert_eq!(data, "PONG");
}

#[tokio::test]
async fn handle_echo() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("ECHO")
        .arg("")
        .query_async(&mut conn)
        .await
        .expect("failed to execute ECHO");
    assert_eq!(data, "");

    let data: String = redis::cmd("echo")
        .arg("hello world")
        .query_async(&mut conn)
        .await
        .expect("failed to execute ECHO");
    assert_eq!(data, "hello world");

    let data: String = redis::cmd("eCho")
        .arg("here is some data")
        .query_async(&mut conn)
        .await
        .expect("failed to execute ECHO");
    assert_eq!(data, "here is some data");

    let err: Result<String, redis::RedisError> = redis::cmd("echo").query_async(&mut conn).await;
    assert!(err.is_err());
}
