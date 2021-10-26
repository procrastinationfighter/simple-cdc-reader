use std::env;
use scylla::{SessionBuilder, Session};
use scylla::frame::value::Timestamp;
use futures_util::StreamExt;

// Assumption of the program:
// in the database already exists table ks.t(pk, ck, v) with enabled cdc.
const DB_URI: &str = "172.17.0.2:9042";

const STARTING_TIME_DELAY_IN_MS: i64 = 7_200_000; // Default: 2 hours backwards.
const WAIT_TIME_IN_MS: u64 = 3000;
const NEW_DATA_DELAY_IN_MS: i64 = 300;
const DURATION_TIME_IN_MS: i64 = 1_800_000;     // Default: 30 minutes durations
                                                // (used for test purposes, it is recommended to change it)

// I tried to query this by using toTimestamp("cdc$time"), but utterly failed.
const QUERY_STR: &str = "SELECT \"ck\", \"pk\", \"v\" FROM ks.t_scylla_cdc_log \
                            WHERE \"cdc$stream_id\" = ? \
                                AND \"cdc$time\" >= maxTimeuuid(?)\
                                AND \"cdc$time\" < minTimeuuid(?)";

fn parse_arguments() -> Vec<u8> {
    let args: Vec<String> = env::args().collect();
    let stream_str: &str = args[1].strip_prefix("0x")
        .expect("Expected stream id format: hex number starting with 0x");
    let stream_id: Vec<u8> = hex::decode(stream_str)
        .expect("Decoding stream id failed.");

    stream_id
}

async fn query_data(session: &Session,
                    stream_id: &Vec<u8>,
                    next_starting_point: chrono::Duration) -> Result<i64, Box<dyn std::error::Error>> {
    // The function assumes that the program slept,
    // so that next_starting_point in ms is smaller than
    // now - NEW_DATA_DELAY_IN_MS
    let right_timestamp_ms = {
        let now = chrono::Local::now().timestamp_millis();
        let new_time = next_starting_point.num_milliseconds() + DURATION_TIME_IN_MS;

        if new_time >= now - NEW_DATA_DELAY_IN_MS {
            now - NEW_DATA_DELAY_IN_MS
        } else {
            new_time
        }
    };
    let right_timestamp = chrono::Duration::milliseconds(right_timestamp_ms);

    let prepared_query = session
        .prepare(QUERY_STR)
        .await?;

    let mut rows_stream = session
        .execute_iter(prepared_query, (stream_id, Timestamp(next_starting_point), Timestamp(right_timestamp)))
        .await?
        .into_typed::<(Option<i32>, Option<i32>, Option<i32>,)>();

    while let Some(next_row_res) = rows_stream.next().await {
        let (ck, pk, v) = next_row_res?;
        println!("ck: {:?}, pk: {:?}, v: {:?}", ck, pk, v);
    }

    Ok(right_timestamp_ms)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream_id = parse_arguments();
    let session = SessionBuilder::new()
        .known_node(DB_URI)
        .build()
        .await?;
    let mut starting_point = chrono::Duration::milliseconds(chrono::Local::now().timestamp_millis() - STARTING_TIME_DELAY_IN_MS);

    loop {
        let starting_point_ms = query_data(&session, &stream_id, starting_point).await?;
        starting_point = chrono::Duration::milliseconds(starting_point_ms);
        tokio::time::sleep(tokio::time::Duration::from_millis(WAIT_TIME_IN_MS)).await;
    }
}
