use clap::{Parser, Subcommand};
use samsa::proto::*;
use std::io::{self, BufRead};
use tonic::Request;
use tonic::transport::Channel;

/// Samsa Streaming System CLI
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Node service address
    #[arg(short, long, default_value = "http://127.0.0.1:50052")]
    address: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Bucket management commands
    Bucket {
        #[command(subcommand)]
        action: BucketCommands,
    },
    /// Stream management commands
    Stream {
        /// Bucket name
        #[arg(short, long)]
        bucket: String,
        #[command(subcommand)]
        action: StreamCommands,
    },
    /// Data operations
    Data {
        /// Bucket name
        #[arg(short, long)]
        bucket: String,
        /// Stream name
        #[arg(short, long)]
        stream: String,
        #[command(subcommand)]
        action: DataCommands,
    },
    /// Access token management
    Token {
        #[command(subcommand)]
        action: TokenCommands,
    },
}

#[derive(Subcommand)]
enum BucketCommands {
    /// List buckets
    List {
        /// Prefix filter
        #[arg(short, long, default_value = "")]
        prefix: String,
        /// Start after this bucket for pagination
        #[arg(long, default_value = "")]
        start_after: String,
        /// Limit number of results
        #[arg(short, long)]
        limit: Option<u64>,
    },
    /// Create a new bucket
    Create {
        /// Bucket name
        name: String,
        /// Create streams automatically on append
        #[arg(long)]
        auto_create_on_append: bool,
        /// Create streams automatically on read
        #[arg(long)]
        auto_create_on_read: bool,
    },
    /// Delete a bucket
    Delete {
        /// Bucket name
        name: String,
    },
    /// Get bucket configuration
    Config {
        /// Bucket name
        name: String,
    },
}

#[derive(Subcommand)]
enum StreamCommands {
    /// List streams in a bucket
    List {
        /// Prefix filter
        #[arg(short, long, default_value = "")]
        prefix: String,
        /// Start after this stream for pagination
        #[arg(long, default_value = "")]
        start_after: String,
        /// Limit number of results
        #[arg(short, long)]
        limit: Option<u64>,
    },
    /// Create a new stream
    Create {
        /// Stream name
        name: String,
        /// Storage class (0=unspecified, 1=standard, 2=cold)
        #[arg(long, default_value = "1")]
        storage_class: i32,
        /// Retention age in seconds
        #[arg(long)]
        retention_age: Option<u64>,
        /// Timestamping mode (0=unspecified, 1=client, 2=server, 3=arrival)
        #[arg(long, default_value = "3")]
        timestamping_mode: i32,
        /// Allow future timestamps
        #[arg(long)]
        allow_future: bool,
    },
    /// Delete a stream
    Delete {
        /// Stream name
        name: String,
    },
    /// Get stream configuration
    Config {
        /// Stream name
        name: String,
    },
    /// Check stream tail
    Tail {
        /// Stream name
        name: String,
    },
}

#[derive(Subcommand)]
enum DataCommands {
    /// Append records to a stream
    Append {
        /// Read records from stdin (JSON format)
        #[arg(long)]
        from_stdin: bool,
        /// Record body (if not reading from stdin)
        #[arg(short, long)]
        body: Option<String>,
        /// Custom timestamp (milliseconds since epoch)
        #[arg(short, long)]
        timestamp: Option<u64>,
    },
    /// Read records from a stream
    Read {
        /// Start sequence ID
        #[arg(long)]
        start_seq_id: Option<String>,
        /// Start from timestamp (milliseconds since epoch)
        #[arg(long)]
        start_timestamp: Option<u64>,
        /// Start from tail offset
        #[arg(long)]
        tail_offset: Option<u64>,
        /// Maximum number of records to read
        #[arg(short, long)]
        limit: Option<u64>,
        /// Maximum bytes to read
        #[arg(long)]
        max_bytes: Option<u64>,
        /// Output format (json, raw)
        #[arg(short, long, default_value = "json")]
        format: String,
    },
    /// Continuously read from a stream
    Subscribe {
        /// Start sequence ID
        #[arg(long)]
        start_seq_id: Option<String>,
        /// Start from timestamp (milliseconds since epoch)
        #[arg(long)]
        start_timestamp: Option<u64>,
        /// Start from tail offset
        #[arg(long)]
        tail_offset: Option<u64>,
        /// Maximum number of records per batch
        #[arg(short, long)]
        limit: Option<u64>,
        /// Output format (json, raw)
        #[arg(short, long, default_value = "json")]
        format: String,
        /// Send heartbeats when no data
        #[arg(long)]
        heartbeats: bool,
    },
}

#[derive(Subcommand)]
enum TokenCommands {
    /// Issue a new access token
    Issue {
        /// Expiration time in seconds from now
        #[arg(short, long)]
        expires_in: Option<u64>,
    },
    /// Revoke an access token
    Revoke {
        /// Token ID
        id: String,
    },
    /// List access tokens
    List {
        /// Prefix filter
        #[arg(short, long, default_value = "")]
        prefix: String,
        /// Start after this token for pagination
        #[arg(long, default_value = "")]
        start_after: String,
        /// Limit number of results
        #[arg(short, long)]
        limit: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let channel = Channel::from_shared(cli.address)?.connect().await?;

    match cli.command {
        Commands::Bucket { action } => handle_bucket_commands(channel, action).await?,
        Commands::Stream { bucket, action } => {
            handle_stream_commands(channel, bucket, action).await?
        }
        Commands::Data {
            bucket,
            stream,
            action,
        } => handle_data_commands(channel, bucket, stream, action).await?,
        Commands::Token { action } => handle_token_commands(channel, action).await?,
    }

    Ok(())
}

async fn handle_bucket_commands(
    channel: Channel,
    action: BucketCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = account_service_client::AccountServiceClient::new(channel);

    match action {
        BucketCommands::List {
            prefix,
            start_after,
            limit,
        } => {
            let request = Request::new(ListBucketsRequest {
                prefix,
                start_after,
                limit,
            });

            let response = client.list_buckets(request).await?;
            let buckets = response.into_inner();

            println!("Buckets:");
            for bucket in buckets.buckets {
                println!("  Name: {}", bucket.name);
                println!("  State: {:?}", bucket.state);
                println!("  Created: {}", bucket.created_at);
                if let Some(deleted_at) = bucket.deleted_at {
                    println!("  Deleted: {}", deleted_at);
                }
                println!();
            }

            if buckets.has_more {
                println!("(More buckets available - use pagination)");
            }
        }
        BucketCommands::Create {
            name,
            auto_create_on_append,
            auto_create_on_read,
        } => {
            let config = BucketConfig {
                default_stream_config: Some(StreamConfig {
                    storage_class: 1,                   // Standard
                    retention_age_seconds: Some(86400), // 1 day
                    timestamping_mode: 3,               // Arrival
                    allow_future_timestamps: false,
                }),
                create_stream_on_append: auto_create_on_append,
                create_stream_on_read: auto_create_on_read,
            };

            let request = Request::new(CreateBucketRequest {
                bucket: name.clone(),
                config: Some(config),
            });

            let response = client.create_bucket(request).await?;
            let bucket_info = response.into_inner().info.unwrap();

            println!("Created bucket: {}", bucket_info.name);
            println!("State: {:?}", bucket_info.state);
        }
        BucketCommands::Delete { name } => {
            let request = Request::new(DeleteBucketRequest {
                bucket: name.clone(),
            });
            client.delete_bucket(request).await?;
            println!("Deleted bucket: {}", name);
        }
        BucketCommands::Config { name } => {
            let request = Request::new(GetBucketConfigRequest { bucket: name });
            let response = client.get_bucket_config(request).await?;
            let config = response.into_inner().config.unwrap();

            println!("Bucket Configuration:");
            println!(
                "  Auto-create on append: {}",
                config.create_stream_on_append
            );
            println!("  Auto-create on read: {}", config.create_stream_on_read);
            if let Some(default_config) = config.default_stream_config {
                println!("  Default stream config:");
                println!("    Storage class: {}", default_config.storage_class);
                println!("    Retention: {:?}", default_config.retention_age_seconds);
                println!(
                    "    Timestamping mode: {}",
                    default_config.timestamping_mode
                );
                println!(
                    "    Allow future timestamps: {}",
                    default_config.allow_future_timestamps
                );
            }
        }
    }

    Ok(())
}

async fn handle_stream_commands(
    channel: Channel,
    bucket: String,
    action: StreamCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bucket_client = bucket_service_client::BucketServiceClient::new(channel.clone());
    let mut stream_client = stream_service_client::StreamServiceClient::new(channel);

    match action {
        StreamCommands::List {
            prefix,
            start_after,
            limit,
        } => {
            let mut request = Request::new(ListStreamsRequest {
                prefix,
                start_after,
                limit,
            });
            request
                .metadata_mut()
                .insert("bucket", bucket.parse().unwrap());

            let response = bucket_client.list_streams(request).await?;
            let streams = response.into_inner();

            println!("Streams in bucket '{}':", bucket);
            for stream in streams.streams {
                println!("  Name: {}", stream.name);
                println!("  Created: {}", stream.created_at);
                if let Some(deleted_at) = stream.deleted_at {
                    println!("  Deleted: {}", deleted_at);
                }
                println!();
            }

            if streams.has_more {
                println!("(More streams available - use pagination)");
            }
        }
        StreamCommands::Create {
            name,
            storage_class,
            retention_age,
            timestamping_mode,
            allow_future,
        } => {
            let config = StreamConfig {
                storage_class,
                retention_age_seconds: retention_age,
                timestamping_mode,
                allow_future_timestamps: allow_future,
            };

            let mut request = Request::new(CreateStreamRequest {
                stream: name.clone(),
                config: Some(config),
            });
            request
                .metadata_mut()
                .insert("bucket", bucket.parse().unwrap());

            let response = bucket_client.create_stream(request).await?;
            let stream_info = response.into_inner().info.unwrap();

            println!("Created stream: {}", stream_info.name);
        }
        StreamCommands::Delete { name } => {
            let mut request = Request::new(DeleteStreamRequest {
                stream: name.clone(),
            });
            request
                .metadata_mut()
                .insert("bucket", bucket.parse().unwrap());

            bucket_client.delete_stream(request).await?;
            println!("Deleted stream: {}", name);
        }
        StreamCommands::Config { name } => {
            let mut request = Request::new(GetStreamConfigRequest { stream: name });
            request
                .metadata_mut()
                .insert("bucket", bucket.parse().unwrap());

            let response = bucket_client.get_stream_config(request).await?;
            let config = response.into_inner().config.unwrap();

            println!("Stream Configuration:");
            println!("  Storage class: {}", config.storage_class);
            println!("  Retention: {:?}", config.retention_age_seconds);
            println!("  Timestamping mode: {}", config.timestamping_mode);
            println!(
                "  Allow future timestamps: {}",
                config.allow_future_timestamps
            );
        }
        StreamCommands::Tail { name } => {
            let request = Request::new(CheckTailRequest {
                bucket: bucket.clone(),
                stream: name,
            });

            let response = stream_client.check_tail(request).await?;
            let tail = response.into_inner();

            println!("Stream tail:");
            println!("  Next sequence ID: {}", tail.next_seq_id);
            println!("  Last timestamp: {}", tail.last_timestamp);
        }
    }

    Ok(())
}

async fn handle_data_commands(
    channel: Channel,
    bucket: String,
    stream: String,
    action: DataCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = stream_service_client::StreamServiceClient::new(channel);

    match action {
        DataCommands::Append {
            from_stdin,
            body,
            timestamp,
        } => {
            let mut records = Vec::new();

            if from_stdin {
                println!("Reading records from stdin (JSON format, one record per line)...");
                let stdin = io::stdin();
                for line in stdin.lock().lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }

                    // Try to parse as JSON
                    match serde_json::from_str::<serde_json::Value>(&line) {
                        Ok(json_record) => {
                            let record_body = if let Some(body_field) = json_record.get("body") {
                                body_field.to_string().into_bytes()
                            } else {
                                line.into_bytes()
                            };

                            let record_timestamp =
                                json_record.get("timestamp").and_then(|v| v.as_u64());

                            records.push(AppendRecord {
                                timestamp: record_timestamp,
                                headers: vec![],
                                body: record_body,
                            });
                        }
                        Err(_) => {
                            // Treat as raw text if not valid JSON
                            records.push(AppendRecord {
                                timestamp,
                                headers: vec![],
                                body: line.into_bytes(),
                            });
                        }
                    }
                }
            } else if let Some(body_text) = body {
                records.push(AppendRecord {
                    timestamp,
                    headers: vec![],
                    body: body_text.into_bytes(),
                });
            } else {
                return Err("Either --from-stdin or --body must be specified".into());
            }

            if records.is_empty() {
                println!("No records to append");
                return Ok(());
            }

            let request = Request::new(AppendRequest {
                bucket: bucket.clone(),
                stream: stream.clone(),
                records,
                match_seq_id: None,
                fencing_token: None,
            });

            let response = client.append(request).await?;
            let result = response.into_inner();

            println!("Appended records:");
            println!("  Start sequence ID: {}", result.start_seq_id);
            println!("  End sequence ID: {}", result.end_seq_id);
            println!("  Start timestamp: {}", result.start_timestamp);
            println!("  End timestamp: {}", result.end_timestamp);
        }
        DataCommands::Read {
            start_seq_id,
            start_timestamp,
            tail_offset,
            limit,
            max_bytes,
            format,
        } => {
            let start = if let Some(seq_id) = start_seq_id {
                Some(read_request::Start::SeqId(seq_id))
            } else if let Some(timestamp) = start_timestamp {
                Some(read_request::Start::Timestamp(timestamp))
            } else {
                tail_offset.map(read_request::Start::TailOffset)
            };

            let read_limit = if limit.is_some() || max_bytes.is_some() {
                Some(ReadLimit {
                    count: limit,
                    bytes: max_bytes,
                })
            } else {
                None
            };

            let request = Request::new(ReadRequest {
                bucket: bucket.clone(),
                stream: stream.clone(),
                start,
                limit: read_limit,
            });

            let response = client.read(request).await?;
            let result = response.into_inner();

            match result.output {
                Some(read_response::Output::Batch(batch)) => {
                    println!("Read {} records:", batch.records.len());
                    for (i, record) in batch.records.iter().enumerate() {
                        if format == "json" {
                            let json_record = serde_json::json!({
                                "seq_id": record.seq_id,
                                "timestamp": record.timestamp,
                                "body": String::from_utf8_lossy(&record.body),
                                "headers": record.headers.iter().map(|h| {
                                    serde_json::json!({
                                        "name": String::from_utf8_lossy(&h.name),
                                        "value": String::from_utf8_lossy(&h.value)
                                    })
                                }).collect::<Vec<_>>()
                            });
                            println!("{}", serde_json::to_string_pretty(&json_record)?);
                        } else {
                            println!("Record {}:", i + 1);
                            println!("  Seq ID: {}", record.seq_id);
                            println!("  Timestamp: {}", record.timestamp);
                            println!("  Body: {}", String::from_utf8_lossy(&record.body));
                            if !record.headers.is_empty() {
                                println!("  Headers:");
                                for header in &record.headers {
                                    println!(
                                        "    {}: {}",
                                        String::from_utf8_lossy(&header.name),
                                        String::from_utf8_lossy(&header.value)
                                    );
                                }
                            }
                            println!();
                        }
                    }
                }
                Some(read_response::Output::NextSeqId(next_seq_id)) => {
                    println!("No records found. Next sequence ID: {}", next_seq_id);
                }
                None => {
                    println!("No output from read request");
                }
            }
        }
        DataCommands::Subscribe {
            start_seq_id,
            start_timestamp,
            tail_offset,
            limit,
            format,
            heartbeats,
        } => {
            let start = if let Some(seq_id) = start_seq_id {
                Some(read_session_request::Start::SeqId(seq_id))
            } else if let Some(timestamp) = start_timestamp {
                Some(read_session_request::Start::Timestamp(timestamp))
            } else {
                tail_offset.map(read_session_request::Start::TailOffset)
            };

            let read_limit = if limit.is_some() {
                Some(ReadLimit {
                    count: limit,
                    bytes: None,
                })
            } else {
                None
            };

            let request = Request::new(ReadSessionRequest {
                bucket: bucket.clone(),
                stream: stream.clone(),
                start,
                limit: read_limit,
                heartbeats,
            });

            let mut response_stream = client.read_session(request).await?.into_inner();

            println!("Subscribing to stream (press Ctrl+C to exit)...");

            while let Some(response) = response_stream.message().await? {
                if let Some(output) = response.output {
                    if let Some(read_output::Output::Batch(batch)) = output.output {
                        for record in batch.records {
                            if format == "json" {
                                let json_record = serde_json::json!({
                                    "seq_id": record.seq_id,
                                    "timestamp": record.timestamp,
                                    "body": String::from_utf8_lossy(&record.body),
                                    "headers": record.headers.iter().map(|h| {
                                        serde_json::json!({
                                            "name": String::from_utf8_lossy(&h.name),
                                            "value": String::from_utf8_lossy(&h.value)
                                        })
                                    }).collect::<Vec<_>>()
                                });
                                println!("{}", serde_json::to_string(&json_record)?);
                            } else {
                                println!(
                                    "[{}] {}: {}",
                                    record.timestamp,
                                    record.seq_id,
                                    String::from_utf8_lossy(&record.body)
                                );
                            }
                        }
                    }
                } else if heartbeats {
                    println!("(heartbeat)");
                }
            }
        }
    }

    Ok(())
}

async fn handle_token_commands(
    channel: Channel,
    action: TokenCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = account_service_client::AccountServiceClient::new(channel);

    match action {
        TokenCommands::Issue { expires_in } => {
            let expires_at = expires_in.map(|seconds| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + seconds
            });

            let info = AccessTokenInfo {
                id: String::new(), // Will be generated by server
                expires_at,
                auto_prefix_streams: false,
                scope: Some(AccessTokenScope {
                    buckets: Some(ResourceSet {
                        matching: Some(resource_set::Matching::Prefix("".to_string())),
                    }),
                    streams: Some(ResourceSet {
                        matching: Some(resource_set::Matching::Prefix("".to_string())),
                    }),
                    access_tokens: Some(ResourceSet {
                        matching: Some(resource_set::Matching::Prefix("".to_string())),
                    }),
                    ops: vec![],
                }),
            };

            let request = Request::new(IssueAccessTokenRequest { info: Some(info) });
            let response = client.issue_access_token(request).await?;
            let token = response.into_inner().access_token;

            println!("Issued access token: {}", token);
        }
        TokenCommands::Revoke { id } => {
            let request = Request::new(RevokeAccessTokenRequest { id: id.clone() });
            let response = client.revoke_access_token(request).await?;
            let info = response.into_inner().info.unwrap();

            println!("Revoked token: {}", id);
            println!("ID: {}", info.id);
        }
        TokenCommands::List {
            prefix,
            start_after,
            limit,
        } => {
            let request = Request::new(ListAccessTokensRequest {
                prefix,
                start_after,
                limit,
            });

            let response = client.list_access_tokens(request).await?;
            let tokens = response.into_inner();

            println!("Access Tokens:");
            for token in tokens.access_tokens {
                println!("  ID: {}", token.id);
                println!("  Expires: {:?}", token.expires_at);
                println!("  Auto prefix streams: {}", token.auto_prefix_streams);
                println!();
            }

            if tokens.has_more {
                println!("(More tokens available - use pagination)");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bucket_list_defaults() {
        let cli = Cli::parse_from(["samsa", "bucket", "list"]);
        match cli.command {
            Commands::Bucket { action } => match action {
                BucketCommands::List {
                    prefix,
                    start_after,
                    limit,
                } => {
                    assert_eq!(prefix, "");
                    assert_eq!(start_after, "");
                    assert_eq!(limit, None);
                }
                _ => panic!("Unexpected bucket command"),
            },
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_bucket_list_with_args() {
        let cli = Cli::parse_from([
            "samsa",
            "bucket",
            "list",
            "--prefix",
            "test-prefix",
            "--start-after",
            "last-bucket",
            "--limit",
            "10",
        ]);
        match cli.command {
            Commands::Bucket { action } => match action {
                BucketCommands::List {
                    prefix,
                    start_after,
                    limit,
                } => {
                    assert_eq!(prefix, "test-prefix");
                    assert_eq!(start_after, "last-bucket");
                    assert_eq!(limit, Some(10));
                }
                _ => panic!("Unexpected bucket command"),
            },
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_bucket_create_minimal() {
        let cli = Cli::parse_from(["samsa", "bucket", "create", "my-bucket"]);
        match cli.command {
            Commands::Bucket { action } => match action {
                BucketCommands::Create {
                    name,
                    auto_create_on_append,
                    auto_create_on_read,
                } => {
                    assert_eq!(name, "my-bucket");
                    assert!(!auto_create_on_append);
                    assert!(!auto_create_on_read);
                }
                _ => panic!("Unexpected bucket command"),
            },
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_bucket_create_with_flags() {
        let cli = Cli::parse_from([
            "samsa",
            "bucket",
            "create",
            "another-bucket",
            "--auto-create-on-append",
            "--auto-create-on-read",
        ]);
        match cli.command {
            Commands::Bucket { action } => match action {
                BucketCommands::Create {
                    name,
                    auto_create_on_append,
                    auto_create_on_read,
                } => {
                    assert_eq!(name, "another-bucket");
                    assert!(auto_create_on_append);
                    assert!(auto_create_on_read);
                }
                _ => panic!("Unexpected bucket command"),
            },
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_bucket_delete() {
        let cli = Cli::parse_from(["samsa", "bucket", "delete", "bucket-to-delete"]);
        match cli.command {
            Commands::Bucket { action } => match action {
                BucketCommands::Delete { name } => {
                    assert_eq!(name, "bucket-to-delete");
                }
                _ => panic!("Unexpected bucket command"),
            },
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_bucket_config() {
        let cli = Cli::parse_from(["samsa", "bucket", "config", "bucket-for-config"]);
        match cli.command {
            Commands::Bucket { action } => match action {
                BucketCommands::Config { name } => {
                    assert_eq!(name, "bucket-for-config");
                }
                _ => panic!("Unexpected bucket command"),
            },
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_stream_list_defaults() {
        let cli = Cli::parse_from(["samsa", "stream", "--bucket", "b", "list"]);
        match cli.command {
            Commands::Stream { bucket, action } => {
                assert_eq!(bucket, "b");
                match action {
                    StreamCommands::List {
                        prefix,
                        start_after,
                        limit,
                    } => {
                        assert_eq!(prefix, "");
                        assert_eq!(start_after, "");
                        assert_eq!(limit, None);
                    }
                    _ => panic!("Unexpected stream command"),
                }
            }
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_stream_list_with_args() {
        let cli = Cli::parse_from([
            "samsa",
            "stream",
            "--bucket",
            "b",
            "list",
            "--prefix",
            "test-prefix",
            "--start-after",
            "last-stream",
            "--limit",
            "5",
        ]);
        match cli.command {
            Commands::Stream { bucket, action } => {
                assert_eq!(bucket, "b");
                match action {
                    StreamCommands::List {
                        prefix,
                        start_after,
                        limit,
                    } => {
                        assert_eq!(prefix, "test-prefix");
                        assert_eq!(start_after, "last-stream");
                        assert_eq!(limit, Some(5));
                    }
                    _ => panic!("Unexpected stream command"),
                }
            }
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_data_append_from_stdin() {
        let cli = Cli::parse_from([
            "samsa",
            "data",
            "--bucket",
            "b",
            "--stream",
            "s",
            "append",
            "--from-stdin",
        ]);
        match cli.command {
            Commands::Data {
                bucket,
                stream,
                action,
            } => {
                assert_eq!(bucket, "b");
                assert_eq!(stream, "s");
                match action {
                    DataCommands::Append {
                        from_stdin,
                        body,
                        timestamp,
                    } => {
                        assert!(from_stdin);
                        assert_eq!(body, None);
                        assert_eq!(timestamp, None);
                    }
                    _ => panic!("Unexpected data command"),
                }
            }
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_data_append_with_body_and_timestamp() {
        let cli = Cli::parse_from([
            "samsa",
            "data",
            "--bucket",
            "b",
            "--stream",
            "s",
            "append",
            "--body",
            "record data",
            "--timestamp",
            "1234567890",
        ]);
        match cli.command {
            Commands::Data {
                bucket,
                stream,
                action,
            } => {
                assert_eq!(bucket, "b");
                assert_eq!(stream, "s");
                match action {
                    DataCommands::Append {
                        from_stdin,
                        body,
                        timestamp,
                    } => {
                        assert!(!from_stdin);
                        assert_eq!(body, Some("record data".to_string()));
                        assert_eq!(timestamp, Some(1234567890));
                    }
                    _ => panic!("Unexpected data command"),
                }
            }
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_token_issue() {
        let cli = Cli::parse_from(["samsa", "token", "issue", "--expires-in", "3600"]);
        match cli.command {
            Commands::Token { action } => match action {
                TokenCommands::Issue { expires_in } => {
                    assert_eq!(expires_in, Some(3600));
                }
                _ => panic!("Unexpected token command"),
            },
            _ => panic!("Unexpected command"),
        }
    }

    #[test]
    fn test_parse_token_issue_no_args() {
        let cli = Cli::parse_from(["samsa", "token", "issue"]);
        match cli.command {
            Commands::Token { action } => match action {
                TokenCommands::Issue { expires_in } => {
                    assert_eq!(expires_in, None);
                }
                _ => panic!("Unexpected token command"),
            },
            _ => panic!("Unexpected command"),
        }
    }
}
