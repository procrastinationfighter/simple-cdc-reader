use std::env;
use scylla::{Session, SessionBuilder, IntoTypedRows};
use std::time::Duration;

const DB_URI: &str = "172.17.0.2:9042";
const WAIT_TIME_IN_SECONDS: usize = 5;
const CONNECTION_TIMEOUT_IN_SECONDS: u64 = 3;

// Assumption of the program:
// in the database already exists table ks.t(pk, ck, v) with enabled cdc.

fn parse_arguments() -> Vec<u8> {
    let args: Vec<String> = env::args().collect();
    let stream_str: &str = args[1].strip_prefix("0x")
        .expect("Expected stream id format: hex number starting with 0x");
    let stream_id: Vec<u8> = hex::decode(stream_str)
        .expect("Decoding stream id failed.");

    stream_id
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let stream_id = parse_arguments();

    let session = SessionBuilder::new()
        .known_node(DB_URI)
        .connection_timeout(Duration::from_secs(CONNECTION_TIMEOUT_IN_SECONDS))
        .build()
        .await?;

    let result = session
        .query("SELECT \"ck\", \"pk\", \"v\" FROM ks.t_scylla_cdc_log WHERE \"cdc$stream_id\" = ?", (&stream_id,))
        .await?;

    for row in result.rows.expect("No rows found") {
        let (ck, pk, v): (i32, i32, i32,) = row.into_typed::<(i32, i32, i32,)>()?;
        println!("ck: {}, pk: {}, v: {}", ck, pk, v);
    }

    Ok(())
}
