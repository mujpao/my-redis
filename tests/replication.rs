use crate::common::{setup, setup_replica};

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
    assert_eq!(data, "role:master");

    let mut conn2 = setup_replica(port).await;

    let data: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut conn2)
        .await
        .unwrap();
    assert_eq!(data, "role:slave");
}
