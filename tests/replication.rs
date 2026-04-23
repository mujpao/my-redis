use crate::common::{setup, setup_replica};
use codecrafters_redis::command::Command;
use codecrafters_redis::connection::Connection;
use codecrafters_redis::resp::RespValue;
use rand::distr::Alphanumeric;
use rand::distr::SampleString;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::time::sleep;

mod common;

#[tokio::test]
async fn info_replication_works() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let data: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(data.contains("role:master\r\n"));
    assert!(data.contains("master_replid:"));
    assert!(data.contains("master_repl_offset:0"));

    let mut conn2 = setup_replica(port).await;

    let data: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut conn2)
        .await
        .unwrap();
    assert_eq!(data, "role:slave");
}

#[tokio::test]
async fn primary_responds_to_replication_handshake() {
    let port = setup().await;

    let handle = tokio::spawn(async move {
        let addr = format!("127.0.0.1:{}", port);
        let stream = TcpStream::connect(addr).await.unwrap();

        let mut conn = Connection::new(stream);

        conn.write_value(&RespValue::Array(vec![RespValue::BulkString(
            String::from("PING"),
        )]))
        .await
        .unwrap();
        let response = conn.read_value().await.unwrap();
        assert_eq!(
            response,
            Some(RespValue::SimpleString(String::from("PONG")))
        );

        conn.write_value(&RespValue::Array(vec![
            RespValue::BulkString(String::from("REPLCONF")),
            RespValue::BulkString(String::from("listening-port")),
            RespValue::BulkString(String::from("1234")),
        ]))
        .await
        .unwrap();

        let response = conn.read_value().await.unwrap();
        assert_eq!(response, Some(RespValue::SimpleString(String::from("OK"))));

        conn.write_value(&RespValue::Array(vec![
            RespValue::BulkString(String::from("REPLCONF")),
            RespValue::BulkString(String::from("capa")),
            RespValue::BulkString(String::from("psync2")),
        ]))
        .await
        .unwrap();

        let response = conn.read_value().await.unwrap();
        assert_eq!(response, Some(RespValue::SimpleString(String::from("OK"))));

        conn.write_value(&RespValue::Array(vec![
            RespValue::BulkString(String::from("PSYNC")),
            RespValue::BulkString(String::from("?")),
            RespValue::BulkString(String::from("-1")),
        ]))
        .await
        .unwrap();

        let response = conn.read_value().await.unwrap();

        match response {
            Some(RespValue::SimpleString(s)) => {
                assert!(s.contains("FULLRESYNC"));
            }
            _ => {
                assert!(false);
            }
        }

        let empty_rdb_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let rdb_data = hex::decode(empty_rdb_file_hex).unwrap();

        let response = conn.read_rdb_data().await.unwrap();
        assert_eq!(response, rdb_data);
    });

    handle.await.unwrap();
}

#[tokio::test]
async fn set_and_get_commands_propagate_to_single_replica() {
    let port = setup().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn_primary = client.get_multiplexed_async_connection().await.unwrap();

    let mut conn_replica = setup_replica(port).await;

    sleep(Duration::from_millis(300)).await;

    let data: String = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query_async(&mut conn_primary)
        .await
        .unwrap();
    assert_eq!(data, "OK");

    let data: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn_primary)
        .await
        .unwrap();
    assert_eq!(data, "bar");

    let data: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn_replica)
        .await
        .expect("failed to execute GET");
    assert_eq!(data, "bar");

    let data: redis::Value = redis::cmd("GET")
        .arg("keydoesn'texist")
        .query_async(&mut conn_primary)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::Nil);

    let data: redis::Value = redis::cmd("GET")
        .arg("keydoesn'texist")
        .query_async(&mut conn_replica)
        .await
        .unwrap();
    assert_eq!(data, redis::Value::Nil);
}

#[tokio::test]
async fn replica_can_process_commands() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();

        let mut conn = Connection::new(stream);
        let ping = conn.read_value().await.unwrap();
        assert_eq!(
            ping,
            Some(RespValue::Array(vec![RespValue::BulkString(String::from(
                "PING"
            ),)]))
        );

        conn.write_value(&RespValue::SimpleString(String::from("PONG")))
            .await
            .unwrap();

        // replconf 1
        let _ = conn.read_value().await.unwrap();

        conn.write_value(&RespValue::SimpleString(String::from("OK")))
            .await
            .unwrap();

        // replconf 2
        let _ = conn.read_value().await.unwrap();
        conn.write_value(&RespValue::SimpleString(String::from("OK")))
            .await
            .unwrap();

        let replication_id = Alphanumeric.sample_string(&mut rand::rng(), 40);

        // psync
        let _ = conn.read_value().await.unwrap();
        let data = format!("FULLRESYNC {} 0", replication_id);
        let response = RespValue::SimpleString(data);
        conn.write_value(&response).await.unwrap();

        let empty_rdb_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let rdb_data = hex::decode(empty_rdb_file_hex).unwrap();
        conn.write_rdb_data(&rdb_data).await.unwrap();

        let command = Command::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
            expiry_duration: None,
        };

        conn.write_value(&command.try_into().unwrap())
            .await
            .unwrap();

        conn
    });

    let mut conn_replica = setup_replica(port).await;

    sleep(Duration::from_millis(300)).await;

    handle.await.unwrap();

    let data: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut conn_replica)
        .await
        .expect("failed to execute GET");
    assert_eq!(data, "bar");
}
