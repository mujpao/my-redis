use codecrafters_redis::app::{App, run};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;

async fn setup() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let _ = tokio::spawn(async {
        let app = App::new();

        run(app, listener).await
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

#[tokio::test]
async fn handle_set_and_get() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .expect("failed to execute SET");
    assert_eq!(data, "OK");

    let data: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .expect("failed to execute GET");
    assert_eq!(data, "bar");

    let data: redis::Value = redis::cmd("GET")
        .arg("keydoesn'texist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute GET");
    assert_eq!(data, redis::Value::Nil);
}

#[tokio::test]
async fn handle_set_with_expiry() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let client2 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn2 = client2.get_multiplexed_async_connection().await.unwrap();

    let handle = tokio::spawn(async move {
        let data: String = redis::cmd("SET")
            .arg("mykey1")
            .arg("myvalue")
            .arg("px")
            .arg(100)
            .query_async(&mut conn2)
            .await
            .expect("failed to execute SET");
        assert_eq!(data, "OK");

        let data: String = redis::cmd("GET")
            .arg("mykey1")
            .query_async(&mut conn2)
            .await
            .expect("failed to execute GET");
        assert_eq!(data, "myvalue");

        sleep(Duration::from_millis(100)).await;

        let data: redis::Value = redis::cmd("GET")
            .arg("mykey1")
            .query_async(&mut conn2)
            .await
            .expect("failed to execute GET");
        assert_eq!(data, redis::Value::Nil);

        let data: String = redis::cmd("SET")
            .arg("foo1")
            .arg("bar")
            .arg("ex")
            .arg(1)
            .query_async(&mut conn2)
            .await
            .expect("failed to execute SET");
        assert_eq!(data, "OK");

        let data: String = redis::cmd("GET")
            .arg("foo1")
            .query_async(&mut conn2)
            .await
            .expect("failed to execute GET");
        assert_eq!(data, "bar");

        sleep(Duration::from_secs(1)).await;

        let data: redis::Value = redis::cmd("GET")
            .arg("foo1")
            .query_async(&mut conn2)
            .await
            .expect("failed to execute GET");
        assert_eq!(data, redis::Value::Nil);
    });

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .arg("EX")
        .arg(1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute SET");
    assert_eq!(data, "OK");

    let data: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .expect("failed to execute GET");
    assert_eq!(data, "bar");

    sleep(Duration::from_secs(1)).await;

    let data: redis::Value = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .expect("failed to execute GET");
    assert_eq!(data, redis::Value::Nil);

    let data: String = redis::cmd("SET")
        .arg("mykey")
        .arg("myvalue")
        .arg("PX")
        .arg(100)
        .query_async(&mut conn)
        .await
        .expect("failed to execute SET");
    assert_eq!(data, "OK");

    let data: String = redis::cmd("GET")
        .arg("mykey")
        .query_async(&mut conn)
        .await
        .expect("failed to execute GET");
    assert_eq!(data, "myvalue");

    sleep(Duration::from_millis(100)).await;

    let data: redis::Value = redis::cmd("GET")
        .arg("mykey")
        .query_async(&mut conn)
        .await
        .expect("failed to execute GET");
    assert_eq!(data, redis::Value::Nil);

    handle.await.unwrap();
}

#[tokio::test]
async fn rpush_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: i64 = redis::cmd("LLEN")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LLEN");
    assert_eq!(data, 0);

    let data: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("hello")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 1);

    let data: i64 = redis::cmd("LLEN")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LLEN");
    assert_eq!(data, 1);

    let data: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("world")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 2);

    let data: i64 = redis::cmd("LLEN")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LLEN");
    assert_eq!(data, 2);

    let data: i64 = redis::cmd("RPUSH")
        .arg("another_list")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 2);

    let data: i64 = redis::cmd("LLEN")
        .arg("another_list")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LLEN");
    assert_eq!(data, 2);

    let data: i64 = redis::cmd("RPUSH")
        .arg("another_list")
        .arg("foo")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 5);

    let data: i64 = redis::cmd("LLEN")
        .arg("another_list")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LLEN");
    assert_eq!(data, 5);
}

#[tokio::test]
async fn lrange_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: i64 = redis::cmd("RPUSH")
        .arg("list_key")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 5);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(0)
        .arg(1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["a", "b"]);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(2)
        .arg(4)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["c", "d", "e"]);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(5)
        .arg(6)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, Vec::<String>::new());

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(2)
        .arg(6)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["c", "d", "e"]);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(3)
        .arg(2)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, Vec::<String>::new());

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("arraydoesn'texist")
        .arg(3)
        .arg(2)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, Vec::<String>::new());

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(-2)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["d", "e"]);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(0)
        .arg(-3)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["a", "b", "c"]);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(-6)
        .arg(-3)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["a", "b", "c"]);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(-6)
        .arg(-6)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["a"]);
}

#[tokio::test]
async fn lpush_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: i64 = redis::cmd("LPUSH")
        .arg("list_key")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 3);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["c", "b", "a"]);

    let data: i64 = redis::cmd("LPUSH")
        .arg("list_key")
        .arg("d")
        .arg("e")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 5);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("list_key")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["e", "d", "c", "b", "a"]);

    let data: i64 = redis::cmd("LLEN")
        .arg("list_key")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LLEN");
    assert_eq!(data, 5);
}

#[tokio::test]
async fn lpop_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: redis::Value = redis::cmd("LPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, redis::Value::Nil);

    let data: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 4);

    let data: String = redis::cmd("LPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, "a");

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["b", "c", "d"]);

    let data: String = redis::cmd("LPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, "b");

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["c", "d"]);

    let data: String = redis::cmd("LPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, "c");

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["d"]);

    let data: String = redis::cmd("LPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, "d");

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, Vec::<String>::new());

    let data: redis::Value = redis::cmd("LPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, redis::Value::Nil);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, Vec::<String>::new());
}

#[tokio::test]
async fn lpop_multiple_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: redis::Value = redis::cmd("LPOP")
        .arg("mylist")
        .arg(3)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, redis::Value::Nil);

    let data: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut conn)
        .await
        .expect("failed to execute RPUSH");
    assert_eq!(data, 4);

    let data: Vec<String> = redis::cmd("LPOP")
        .arg("mylist")
        .arg(2)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, vec!["a", "b"]);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, vec!["c", "d"]);

    let data: Vec<String> = redis::cmd("LPOP")
        .arg("mylist")
        .arg(4)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, vec!["c", "d"]);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, Vec::<String>::new());

    let data: redis::Value = redis::cmd("LPOP")
        .arg("mylist")
        .arg(2)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LPOP");
    assert_eq!(data, redis::Value::Nil);

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .expect("failed to execute LRANGE");
    assert_eq!(result, Vec::<String>::new());
}

#[tokio::test]
async fn blpop_without_timeout_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let client2 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn2 = client2.get_multiplexed_async_connection().await.unwrap();

    let client3 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn3 = client3.get_multiplexed_async_connection().await.unwrap();

    let handle1 = tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        let data: i64 = redis::cmd("RPUSH")
            .arg("mylist")
            .arg("foo")
            .query_async(&mut conn2)
            .await
            .unwrap();
        assert_eq!(data, 1);

        let data: i64 = redis::cmd("LPUSH")
            .arg("mylist")
            .arg("bar")
            .query_async(&mut conn2)
            .await
            .unwrap();
        assert_eq!(data, 1);
    });

    let handle2 = tokio::spawn(async move {
        sleep(Duration::from_millis(5)).await;
        let data: Vec<String> = redis::cmd("BLPOP")
            .arg("mylist")
            .arg(0)
            .query_async(&mut conn3)
            .await
            .unwrap();
        assert_eq!(data, vec!["mylist", "bar"]);
    });

    let data: Vec<String> = redis::cmd("BLPOP")
        .arg("mylist")
        .arg(0)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, vec!["mylist", "foo"]);

    handle1.await.unwrap();
    handle2.await.unwrap();

    let result: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, Vec::<String>::new());
}
