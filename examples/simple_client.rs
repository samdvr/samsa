use samsa::proto::*;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to router
    let mut account_client =
        account_service_client::AccountServiceClient::connect("http://127.0.0.1:50051").await?;
    let mut bucket_client =
        bucket_service_client::BucketServiceClient::connect("http://127.0.0.1:50051").await?;
    let mut stream_client =
        stream_service_client::StreamServiceClient::connect("http://127.0.0.1:50051").await?;

    println!("Connected to Samsa router");

    // Create a bucket
    let bucket_name = "test-bucket";
    let create_bucket_request = CreateBucketRequest {
        bucket: bucket_name.to_string(),
        config: Some(BucketConfig {
            default_stream_config: Some(StreamConfig {
                storage_class: StorageClass::Standard as i32,
                retention_age_seconds: Some(86400), // 1 day
                timestamping_mode: TimestampingMode::Arrival as i32,
                allow_future_timestamps: false,
            }),
            create_stream_on_append: true,
            create_stream_on_read: false,
        }),
    };

    match account_client
        .create_bucket(Request::new(create_bucket_request))
        .await
    {
        Ok(response) => {
            println!("Created bucket: {:?}", response.into_inner().info);
        }
        Err(e) => {
            println!("Failed to create bucket (might already exist): {}", e);
        }
    }

    // List buckets
    let list_buckets_request = ListBucketsRequest {
        prefix: "".to_string(),
        start_after: "".to_string(),
        limit: Some(10),
    };

    let response = account_client
        .list_buckets(Request::new(list_buckets_request))
        .await?;
    println!("Buckets: {:?}", response.into_inner().buckets);

    // Create a stream
    let stream_name = "test-stream";
    let create_stream_request = CreateStreamRequest {
        stream: stream_name.to_string(),
        config: Some(StreamConfig {
            storage_class: StorageClass::Standard as i32,
            retention_age_seconds: Some(86400),
            timestamping_mode: TimestampingMode::Arrival as i32,
            allow_future_timestamps: false,
        }),
    };

    match bucket_client
        .create_stream(Request::new(create_stream_request))
        .await
    {
        Ok(response) => {
            println!("Created stream: {:?}", response.into_inner().info);
        }
        Err(e) => {
            println!("Failed to create stream (might already exist): {}", e);
        }
    }

    // Append some records
    let append_request = AppendRequest {
        bucket: bucket_name.to_string(),
        stream: stream_name.to_string(),
        records: vec![
            AppendRecord {
                timestamp: None, // Let server assign timestamp
                headers: vec![Header {
                    name: b"content-type".to_vec(),
                    value: b"application/json".to_vec(),
                }],
                body: b"{\"message\": \"Hello, Samsa!\"}".to_vec(),
            },
            AppendRecord {
                timestamp: None,
                headers: vec![],
                body: b"{\"message\": \"Second record\"}".to_vec(),
            },
        ],
        match_seq_id: None,
        fencing_token: None,
    };

    let response = stream_client.append(Request::new(append_request)).await?;
    let append_response = response.into_inner();
    println!(
        "Appended records: start_seq={}, end_seq={}",
        append_response.start_seq_id, append_response.end_seq_id
    );

    // Check tail
    let check_tail_request = CheckTailRequest {
        bucket: bucket_name.to_string(),
        stream: stream_name.to_string(),
    };

    let response = stream_client
        .check_tail(Request::new(check_tail_request))
        .await?;
    let tail_response = response.into_inner();
    println!(
        "Stream tail: next_seq={}, last_timestamp={}",
        tail_response.next_seq_id, tail_response.last_timestamp
    );

    // Read records
    let read_request = ReadRequest {
        bucket: bucket_name.to_string(),
        stream: stream_name.to_string(),
        start: Some(read_request::Start::SeqId(uuid::Uuid::nil().to_string())),
        limit: Some(ReadLimit {
            count: Some(10),
            bytes: None,
        }),
    };

    let response = stream_client.read(Request::new(read_request)).await?;
    let read_response = response.into_inner();

    match read_response.output {
        Some(read_response::Output::Batch(batch)) => {
            println!("Read {} records:", batch.records.len());
            for (i, record) in batch.records.iter().enumerate() {
                println!(
                    "  Record {}: seq={}, body={}",
                    i,
                    record.seq_id,
                    String::from_utf8_lossy(&record.body)
                );
            }
        }
        Some(read_response::Output::NextSeqId(seq)) => {
            println!("No records found, next sequence ID: {}", seq);
        }
        None => {
            println!("No output in read response");
        }
    }

    println!("Example completed successfully!");
    Ok(())
}
