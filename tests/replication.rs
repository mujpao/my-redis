use crate::common::{setup, setup_replica};
use codecrafters_redis::connection::Connection;
use codecrafters_redis::resp::RespValue;
use tokio::net::TcpStream;

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
