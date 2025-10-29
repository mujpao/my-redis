use crate::common::setup;
use chrono::DateTime;
use std::time::Duration;
use tokio::time::sleep;

mod common;

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

#[tokio::test]
async fn blpop_with_timeout_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let client2 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn2 = client2.get_multiplexed_async_connection().await.unwrap();

    let handle1 = tokio::spawn(async move {
        sleep(Duration::from_millis(115)).await;
        let data: i64 = redis::cmd("RPUSH")
            .arg("mylist")
            .arg("foo")
            .query_async(&mut conn2)
            .await
            .unwrap();
        assert_eq!(data, 1);
    });

    let data: redis::Value = redis::cmd("BLPOP")
        .arg("mylist")
        .arg(0.1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::Nil);

    let data: Vec<String> = redis::cmd("BLPOP")
        .arg("mylist")
        .arg(0.1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, vec!["mylist", "foo"]);

    handle1.await.unwrap();

    let client3 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn3 = client3.get_multiplexed_async_connection().await.unwrap();

    let handle2 = tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;
        let data: i64 = redis::cmd("LPUSH")
            .arg("mylist")
            .arg("bar")
            .query_async(&mut conn3)
            .await
            .unwrap();
        assert_eq!(data, 1);
    });

    let data: Vec<String> = redis::cmd("BLPOP")
        .arg("mylist")
        .arg(0.1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, vec!["mylist", "bar"]);

    handle2.await.unwrap();

    let data: redis::Value = redis::cmd("BLPOP")
        .arg("mylist")
        .arg(0.1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::Nil);
}

#[tokio::test]
async fn type_works() {
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
        .unwrap();
    assert_eq!(data, 3);

    let result: String = redis::cmd("TYPE")
        .arg("list_key")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, String::from("list"));

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "OK");

    let result: String = redis::cmd("TYPE")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, String::from("string"));

    let result: String = redis::cmd("TYPE")
        .arg("keydoesn'texist")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, String::from("none"));
}

#[tokio::test]
async fn create_stream_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("XADD")
        .arg("stream_key")
        .arg("1526919030474-0")
        .arg("temperature")
        .arg(36)
        .arg("humidity")
        .arg(95)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1526919030474-0");

    let result: String = redis::cmd("TYPE")
        .arg("stream_key")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, String::from("stream"));
}

#[tokio::test]
async fn validate_stream_id() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("1-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1-1");

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("1-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1-2");

    let err: Result<String, redis::RedisError> = redis::cmd("XADD")
        .arg("some_key")
        .arg("1-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await;

    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "The ID specified in XADD is equal or smaller than the target stream top item"
    );

    let err: Result<String, redis::RedisError> = redis::cmd("XADD")
        .arg("some_key")
        .arg("1-0")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await;
    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "The ID specified in XADD is equal or smaller than the target stream top item"
    );

    let err: Result<String, redis::RedisError> = redis::cmd("XADD")
        .arg("some_key")
        .arg("0-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await;
    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "The ID specified in XADD is equal or smaller than the target stream top item"
    );

    let err: Result<String, redis::RedisError> = redis::cmd("XADD")
        .arg("some_key")
        .arg("0-3")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await;
    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "The ID specified in XADD is equal or smaller than the target stream top item"
    );

    let err: Result<String, redis::RedisError> = redis::cmd("XADD")
        .arg("some_key")
        .arg("0-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await;
    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "The ID specified in XADD is equal or smaller than the target stream top item"
    );

    let err: Result<String, redis::RedisError> = redis::cmd("XADD")
        .arg("some_key")
        .arg("0-0")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await;
    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "The ID specified in XADD must be greater than 0-0"
    );

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("2-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "2-2");
}

#[tokio::test]
async fn auto_generate_stream_seq_no() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("1-*")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1-0");

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("1-*")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1-1");

    let data: String = redis::cmd("XADD")
        .arg("some_key2")
        .arg("0-*")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: String = redis::cmd("XADD")
        .arg("some_key2")
        .arg("5-*")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "5-0");

    let data: String = redis::cmd("XADD")
        .arg("some_key3")
        .arg("*")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    let split_data: Vec<_> = data.split("-").collect();
    assert_eq!(split_data[1], "0");
    let millis: i64 = split_data[0].parse().unwrap();
    DateTime::from_timestamp_millis(millis).expect("timestamp should be valid");
}

#[tokio::test]
async fn xrange_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("1526985054069-0")
        .arg("temperature")
        .arg(36)
        .arg("humidity")
        .arg(95)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1526985054069-0");

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("1526985054079-0")
        .arg("temperature")
        .arg(37)
        .arg("humidity")
        .arg(94)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1526985054079-0");

    let expected = redis::Value::Array(vec![
        redis::Value::Array(vec![
            redis::Value::BulkString("1526985054069-0".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("temperature".into()),
                redis::Value::BulkString("36".into()),
                redis::Value::BulkString("humidity".into()),
                redis::Value::BulkString("95".into()),
            ]),
        ]),
        redis::Value::Array(vec![
            redis::Value::BulkString("1526985054079-0".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("temperature".into()),
                redis::Value::BulkString("37".into()),
                redis::Value::BulkString("humidity".into()),
                redis::Value::BulkString("94".into()),
            ]),
        ]),
    ]);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("some_key")
        .arg("1526985054069")
        .arg("1526985054079")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let expected = redis::Value::Array(vec![redis::Value::Array(vec![
        redis::Value::BulkString("1526985054069-0".into()),
        redis::Value::Array(vec![
            redis::Value::BulkString("temperature".into()),
            redis::Value::BulkString("36".into()),
            redis::Value::BulkString("humidity".into()),
            redis::Value::BulkString("95".into()),
        ]),
    ])]);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("some_key")
        .arg("1526985054069-0")
        .arg("1526985054069-0")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("some_key")
        .arg("1")
        .arg("2")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::Array(vec![]));

    let data: String = redis::cmd("XADD")
        .arg("stream_key")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: String = redis::cmd("XADD")
        .arg("stream_key")
        .arg("0-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-2");

    let data: String = redis::cmd("XADD")
        .arg("stream_key")
        .arg("0-3")
        .arg("baz")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-3");

    let expected = redis::Value::Array(vec![
        redis::Value::Array(vec![
            redis::Value::BulkString("0-1".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("foo".into()),
                redis::Value::BulkString("bar".into()),
            ]),
        ]),
        redis::Value::Array(vec![
            redis::Value::BulkString("0-2".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("bar".into()),
                redis::Value::BulkString("baz".into()),
            ]),
        ]),
        redis::Value::Array(vec![
            redis::Value::BulkString("0-3".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("baz".into()),
                redis::Value::BulkString("foo".into()),
            ]),
        ]),
    ]);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("0")
        .arg("1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("-")
        .arg("0-3")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("0-1")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let expected = redis::Value::Array(vec![
        redis::Value::Array(vec![
            redis::Value::BulkString("0-2".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("bar".into()),
                redis::Value::BulkString("baz".into()),
            ]),
        ]),
        redis::Value::Array(vec![
            redis::Value::BulkString("0-3".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("baz".into()),
                redis::Value::BulkString("foo".into()),
            ]),
        ]),
    ]);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("0-2")
        .arg("0-3")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("0-2")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let expected = redis::Value::Array(vec![
        redis::Value::Array(vec![
            redis::Value::BulkString("0-1".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("foo".into()),
                redis::Value::BulkString("bar".into()),
            ]),
        ]),
        redis::Value::Array(vec![
            redis::Value::BulkString("0-2".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("bar".into()),
                redis::Value::BulkString("baz".into()),
            ]),
        ]),
        redis::Value::Array(vec![
            redis::Value::BulkString("0-3".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("baz".into()),
                redis::Value::BulkString("foo".into()),
            ]),
        ]),
    ]);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("0-1")
        .arg("0-3")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("-")
        .arg("0-3")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let expected = redis::Value::Array(vec![
        redis::Value::Array(vec![
            redis::Value::BulkString("0-1".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("foo".into()),
                redis::Value::BulkString("bar".into()),
            ]),
        ]),
        redis::Value::Array(vec![
            redis::Value::BulkString("0-2".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("bar".into()),
                redis::Value::BulkString("baz".into()),
            ]),
        ]),
    ]);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("0-1")
        .arg("0-2")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XRANGE")
        .arg("stream_key")
        .arg("-")
        .arg("0-2")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);
}

#[tokio::test]
async fn xread_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("XADD")
        .arg("stream_key")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: String = redis::cmd("XADD")
        .arg("stream_key")
        .arg("0-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-2");

    let data: String = redis::cmd("XADD")
        .arg("stream_key")
        .arg("0-3")
        .arg("baz")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-3");

    let stream_key_entries = redis::Value::Array(vec![
        redis::Value::Array(vec![
            redis::Value::BulkString("0-2".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("bar".into()),
                redis::Value::BulkString("baz".into()),
            ]),
        ]),
        redis::Value::Array(vec![
            redis::Value::BulkString("0-3".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString("baz".into()),
                redis::Value::BulkString("foo".into()),
            ]),
        ]),
    ]);

    let stream_key = redis::Value::Array(vec![
        redis::Value::BulkString("stream_key".into()),
        stream_key_entries,
    ]);

    let expected = redis::Value::Array(vec![stream_key.clone()]);

    let data: redis::Value = redis::cmd("XREAD")
        .arg("STREAMS")
        .arg("stream_key")
        .arg("0-1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("1526985054069-0")
        .arg("temperature")
        .arg(36)
        .arg("humidity")
        .arg(95)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1526985054069-0");

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("1526985054079-0")
        .arg("temperature")
        .arg(37)
        .arg("humidity")
        .arg(94)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "1526985054079-0");

    let some_key_entries = redis::Value::Array(vec![redis::Value::Array(vec![
        redis::Value::BulkString("1526985054079-0".into()),
        redis::Value::Array(vec![
            redis::Value::BulkString("temperature".into()),
            redis::Value::BulkString("37".into()),
            redis::Value::BulkString("humidity".into()),
            redis::Value::BulkString("94".into()),
        ]),
    ])]);

    let some_key = redis::Value::Array(vec![
        redis::Value::BulkString("some_key".into()),
        some_key_entries,
    ]);

    let expected = redis::Value::Array(vec![stream_key, some_key]);
    let data: redis::Value = redis::cmd("XREAD")
        .arg("STREAMS")
        .arg("stream_key")
        .arg("some_key")
        .arg("0-1")
        .arg("1526985054069-0")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);
}

#[tokio::test]
async fn xread_blocking_works_when_data_available_on_multiple_streams() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("XADD")
        .arg("stream_key1")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: String = redis::cmd("XADD")
        .arg("stream_key1")
        .arg("0-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-2");

    let data: String = redis::cmd("XADD")
        .arg("stream_key2")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: String = redis::cmd("XADD")
        .arg("stream_key3")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: String = redis::cmd("XADD")
        .arg("stream_key3")
        .arg("0-2")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-2");

    let stream_key_entries = redis::Value::Array(vec![redis::Value::Array(vec![
        redis::Value::BulkString("0-2".into()),
        redis::Value::Array(vec![
            redis::Value::BulkString("bar".into()),
            redis::Value::BulkString("baz".into()),
        ]),
    ])]);

    let stream_key1 = redis::Value::Array(vec![
        redis::Value::BulkString("stream_key1".into()),
        stream_key_entries.clone(),
    ]);

    let stream_key3 = redis::Value::Array(vec![
        redis::Value::BulkString("stream_key3".into()),
        stream_key_entries,
    ]);

    let expected = redis::Value::Array(vec![stream_key1, stream_key3]);

    let data: redis::Value = redis::cmd("XREAD")
        .arg("STREAMS")
        .arg("stream_key1")
        .arg("stream_key2")
        .arg("stream_key3")
        .arg("0-1")
        .arg("0-1")
        .arg("0-1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    let data: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(1000)
        .arg("STREAMS")
        .arg("stream_key1")
        .arg("stream_key2")
        .arg("stream_key3")
        .arg("0-1")
        .arg("0-1")
        .arg("0-1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);
}

#[tokio::test]
async fn xread_blocking_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("XADD")
        .arg("stream_key1")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: String = redis::cmd("XADD")
        .arg("stream_key2")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: String = redis::cmd("XADD")
        .arg("stream_key3")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(100)
        .arg("STREAMS")
        .arg("stream_key1")
        .arg("stream_key2")
        .arg("stream_key3")
        .arg("0-1")
        .arg("0-1")
        .arg("0-1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::Nil);

    let client2 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn2 = client2.get_multiplexed_async_connection().await.unwrap();

    let handle2 = tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;

        let data: String = redis::cmd("XADD")
            .arg("stream_key1")
            .arg("0-2")
            .arg("bar")
            .arg("baz")
            .query_async(&mut conn2)
            .await
            .unwrap();
        assert_eq!(data, "0-2");
    });

    let stream_key_entries = redis::Value::Array(vec![redis::Value::Array(vec![
        redis::Value::BulkString("0-2".into()),
        redis::Value::Array(vec![
            redis::Value::BulkString("bar".into()),
            redis::Value::BulkString("baz".into()),
        ]),
    ])]);

    let stream_key1 = redis::Value::Array(vec![
        redis::Value::BulkString("stream_key1".into()),
        stream_key_entries.clone(),
    ]);

    let expected = redis::Value::Array(vec![stream_key1]);

    let data: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(500)
        .arg("STREAMS")
        .arg("stream_key1")
        .arg("stream_key2")
        .arg("stream_key3")
        .arg("0-1")
        .arg("0-1")
        .arg("0-1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    handle2.await.unwrap();

    let client3 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn3 = client3.get_multiplexed_async_connection().await.unwrap();

    let handle2 = tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;

        let data: String = redis::cmd("XADD")
            .arg("stream_key2")
            .arg("0-2")
            .arg("bar")
            .arg("baz")
            .query_async(&mut conn3)
            .await
            .unwrap();
        assert_eq!(data, "0-2");
    });

    let stream_key2 = redis::Value::Array(vec![
        redis::Value::BulkString("stream_key2".into()),
        stream_key_entries.clone(),
    ]);

    let expected = redis::Value::Array(vec![stream_key2]);

    let data: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(0)
        .arg("STREAMS")
        .arg("stream_key1")
        .arg("stream_key2")
        .arg("stream_key3")
        .arg("0-2")
        .arg("0-1")
        .arg("0-1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    handle2.await.unwrap();
}

#[tokio::test]
async fn xread_blocking_with_dollar_sign_id() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("XADD")
        .arg("stream_key")
        .arg("0-1")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let data: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(100)
        .arg("STREAMS")
        .arg("stream_key")
        .arg("$")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::Nil);

    let client2 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn2 = client2.get_multiplexed_async_connection().await.unwrap();

    let handle = tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;

        let data: String = redis::cmd("XADD")
            .arg("stream_key")
            .arg("0-2")
            .arg("bar")
            .arg("baz")
            .query_async(&mut conn2)
            .await
            .unwrap();
        assert_eq!(data, "0-2");
    });

    let stream_key_entries = redis::Value::Array(vec![redis::Value::Array(vec![
        redis::Value::BulkString("0-2".into()),
        redis::Value::Array(vec![
            redis::Value::BulkString("bar".into()),
            redis::Value::BulkString("baz".into()),
        ]),
    ])]);

    let stream_key1 = redis::Value::Array(vec![
        redis::Value::BulkString("stream_key".into()),
        stream_key_entries,
    ]);

    let expected = redis::Value::Array(vec![stream_key1]);

    let data: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(500)
        .arg("STREAMS")
        .arg("stream_key")
        .arg("$")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, expected);

    handle.await.unwrap();

    let data: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(100)
        .arg("STREAMS")
        .arg("stream_key")
        .arg("$")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::Nil);
}

#[tokio::test]
async fn incr_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg(5)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "OK");

    let data: i64 = redis::cmd("INCR")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, 6);

    let data: i64 = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, 6);

    let data: i64 = redis::cmd("INCR")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, 7);

    let data: i64 = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, 7);

    let data: String = redis::cmd("XADD")
        .arg("some_key")
        .arg("0-1")
        .arg("bar")
        .arg("baz")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "0-1");

    let err: Result<String, redis::RedisError> = redis::cmd("INCR")
        .arg("some_key")
        .query_async(&mut conn)
        .await;
    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "Operation against a key holding the wrong kind of value"
    );

    let data: String = redis::cmd("SET")
        .arg("bar")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "OK");

    let err: Result<String, redis::RedisError> =
        redis::cmd("INCR").arg("bar").query_async(&mut conn).await;
    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "value is not an integer or out of range"
    );

    let data: i64 = redis::cmd("INCR")
        .arg("a")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, 1);

    let data: i64 = redis::cmd("GET")
        .arg("a")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, 1);
}

#[tokio::test]
async fn transaction_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let client2 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn2 = client2.get_multiplexed_async_connection().await.unwrap();

    let err: Result<String, redis::RedisError> = redis::cmd("EXEC").query_async(&mut conn).await;
    assert_eq!(err.unwrap_err().detail().unwrap(), "EXEC without MULTI");

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "OK");

    let data: String = redis::cmd("MULTI").query_async(&mut conn).await.unwrap();
    assert_eq!(data, "OK");

    let mut expected = vec![];

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg(5)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "QUEUED");
    expected.push(redis::Value::Okay);

    let data: String = redis::cmd("INCR")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "QUEUED");
    expected.push(redis::Value::Int(6));

    let data: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "QUEUED");
    expected.push(redis::Value::BulkString("6".into()));

    let data: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn2)
        .await
        .unwrap();
    assert_eq!(data, "bar");

    let data: redis::Value = redis::cmd("EXEC").query_async(&mut conn).await.unwrap();
    assert_eq!(data, redis::Value::Array(expected));

    let data: redis::Value = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn2)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::BulkString("6".into()));

    let data: String = redis::cmd("MULTI").query_async(&mut conn).await.unwrap();
    assert_eq!(data, "OK");
    let err: Result<String, redis::RedisError> = redis::cmd("MULTI").query_async(&mut conn).await;
    assert_eq!(
        err.unwrap_err().detail().unwrap(),
        "MULTI calls can not be nested"
    );
}

#[tokio::test]
async fn transaction_can_be_discarded() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "OK");

    let data: String = redis::cmd("MULTI").query_async(&mut conn).await.unwrap();
    assert_eq!(data, "OK");

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg(5)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "QUEUED");

    let data: String = redis::cmd("DISCARD").query_async(&mut conn).await.unwrap();
    assert_eq!(data, "OK");

    let err: Result<String, redis::RedisError> = redis::cmd("EXEC").query_async(&mut conn).await;
    assert_eq!(err.unwrap_err().detail().unwrap(), "EXEC without MULTI");

    let err: Result<String, redis::RedisError> = redis::cmd("DISCARD").query_async(&mut conn).await;
    assert_eq!(err.unwrap_err().detail().unwrap(), "DISCARD without MULTI");

    let data: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(data, "bar");
}
