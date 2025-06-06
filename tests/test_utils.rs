//! Test utilities for Samsa  integration tests
//!
//! This module provides  helper functions and infrastructure
//! for running integration tests against the Samsa system.

use std::process::{Command as StdCommand, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::process::{Child as TokioChild, Command};
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;
use tonic::{Request, Status};

use samsa::proto::*;

// Fixed ports for consistent testing
pub const ETCD_PORT: u16 = 2379;
pub const SERVER_PORT: u16 = 52000;
pub const METRICS_PORT: u16 = 9600;
pub const ETCD_ENDPOINTS: &str = "http://localhost:2379";

//  shared infrastructure
static SHARED_INFRA: std::sync::LazyLock<Arc<Mutex<Option<SharedInfrastructure>>>> =
    std::sync::LazyLock::new(|| Arc::new(Mutex::new(None)));

///  shared infrastructure that manages both etcd and server
struct SharedInfrastructure {
    etcd_process: Option<TokioChild>,
    server_process: Option<TokioChild>,
    is_etcd_external: bool,
}

impl SharedInfrastructure {
    async fn initialize() -> Result<(), Box<dyn std::error::Error>> {
        // Try external etcd first
        let is_etcd_external = Self::check_etcd_available().await;
        let etcd_process = if is_etcd_external {
            println!("✅ Using external etcd instance");
            None
        } else {
            Some(Self::start_etcd().await?)
        };

        // Start server
        let server_process = Self::start_server().await?;

        // Store shared infrastructure
        let mut shared = SHARED_INFRA.lock().unwrap();
        *shared = Some(SharedInfrastructure {
            etcd_process,
            server_process: Some(server_process),
            is_etcd_external,
        });

        println!("✅ Shared infrastructure ready");
        Ok(())
    }

    async fn check_etcd_available() -> bool {
        // Use a longer timeout and test a real operation
        timeout(Duration::from_millis(2000), async {
            match etcd_client::Client::connect([ETCD_ENDPOINTS], None).await {
                Ok(mut client) => {
                    // Test a real operation to ensure etcd is actually working
                    (client.get("test", None).await).is_ok()
                }
                Err(_) => false, // Failed to connect
            }
        })
        .await
        .unwrap_or(false)
    }

    async fn start_etcd() -> Result<TokioChild, Box<dyn std::error::Error>> {
        if !Self::check_docker_available().await {
            return Err("Docker not available to start etcd".into());
        }

        // Cleanup existing container more thoroughly
        let _ = Command::new("docker")
            .args(["stop", "etcd-test"])
            .output()
            .await;
        let _ = Command::new("docker")
            .args(["rm", "-f", "etcd-test"])
            .output()
            .await;

        println!("🚀 Starting etcd with Docker...");
        let child = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--name",
                "etcd-test",
                "-p",
                &format!("{}:2379", ETCD_PORT),
                "quay.io/coreos/etcd:v3.5.0",
                "/usr/local/bin/etcd",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://0.0.0.0:2379",
                "--listen-peer-urls=http://0.0.0.0:2380",
                "--initial-advertise-peer-urls=http://0.0.0.0:2380",
                "--initial-cluster=default=http://0.0.0.0:2380",
                "--initial-cluster-state=new",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        // Wait for etcd to be ready with better feedback
        for attempt in 0..30 {
            if attempt % 5 == 0 && attempt > 0 {
                println!("⏱️  Still waiting for etcd... (attempt {}/30)", attempt);
            }

            if Self::check_etcd_available().await {
                println!("✅ Etcd ready after {} attempts", attempt + 1);
                return Ok(child);
            }
            sleep(Duration::from_millis(300)).await;
        }

        Err("Etcd failed to start within timeout".into())
    }

    async fn start_server() -> Result<TokioChild, Box<dyn std::error::Error>> {
        Self::cleanup_ports().await;

        println!("🚀 Starting server on port {}...", SERVER_PORT);

        let mut child = Command::new("cargo")
            .args(["run", "--bin", "server"])
            .env("SERVER_PORT", SERVER_PORT.to_string())
            .env("METRICS_PORT", METRICS_PORT.to_string())
            .env("ETCD_ENDPOINTS", ETCD_ENDPOINTS)
            .env("RUST_LOG", "samsa=info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Wait for server to be ready with progress indicators
        for attempt in 0..30 {
            if attempt % 5 == 0 && attempt > 0 {
                println!("⏱️  Still waiting for server... (attempt {}/30)", attempt);

                // Check if the child process is still running
                if let Ok(Some(exit_status)) = child.try_wait() {
                    // Process exited, capture stderr for debugging
                    let stderr = child.stderr.take();
                    if let Some(mut stderr) = stderr {
                        let mut error_output = String::new();
                        use tokio::io::AsyncReadExt;
                        let _ = stderr.read_to_string(&mut error_output).await;
                        return Err(format!(
                            "Server process exited with status: {}. Error output: {}",
                            exit_status, error_output
                        )
                        .into());
                    }
                    return Err(
                        format!("Server process exited with status: {}", exit_status).into(),
                    );
                }
            }

            if Self::check_server_ready().await {
                println!("✅ Server ready after {} attempts", attempt + 1);
                return Ok(child);
            }
            sleep(Duration::from_millis(200)).await;
        }

        // Server failed to start - try to get error output
        let stderr = child.stderr.take();
        if let Some(mut stderr) = stderr {
            let mut error_output = String::new();
            use tokio::io::AsyncReadExt;
            let _ = stderr.read_to_string(&mut error_output).await;
            return Err(format!(
                "Server failed to start within timeout. Error output: {}",
                error_output
            )
            .into());
        }

        Err("Server failed to start within timeout".into())
    }

    async fn check_docker_available() -> bool {
        Command::new("docker")
            .args(["ps"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false)
    }

    async fn check_server_ready() -> bool {
        // Check multiple times with retries to ensure server is truly ready
        for attempt in 0..10 {
            // Increased from 8 to 10 attempts
            let is_ready = timeout(Duration::from_millis(8000), async {
                // Increased timeout to 8 seconds
                // Wait a bit for the server to fully start
                sleep(Duration::from_millis(500)).await;

                let channel = Channel::from_static("http://127.0.0.1:52000")
                    .connect_timeout(Duration::from_secs(15))
                    .timeout(Duration::from_secs(120))
                    .tcp_keepalive(Some(Duration::from_secs(60)))
                    .tcp_nodelay(true)
                    .http2_keep_alive_interval(Duration::from_secs(60))
                    .keep_alive_timeout(Duration::from_secs(10))
                    .keep_alive_while_idle(true)
                    .http2_adaptive_window(true)
                    .connect()
                    .await
                    .ok()?;

                let mut client = account_service_client::AccountServiceClient::new(channel);
                let request = Request::new(ListBucketsRequest {
                    prefix: String::new(),
                    start_after: String::new(),
                    limit: Some(1),
                });

                // Try the request and make sure it succeeds
                client.list_buckets(request).await.ok()
            })
            .await;

            if is_ready.is_ok() && is_ready.unwrap().is_some() {
                // Additional verification - wait a bit more and try again with multiple tests
                sleep(Duration::from_millis(1000)).await; // Increased verification wait

                let mut verification_success = 0;
                for verify_round in 0..5 {
                    // Increased from 3 to 5 verification rounds
                    let verify_check = timeout(Duration::from_millis(5000), async {
                        // Increased timeout
                        let channel = Channel::from_static("http://127.0.0.1:52000")
                            .connect_timeout(Duration::from_secs(15))
                            .timeout(Duration::from_secs(120))
                            .tcp_keepalive(Some(Duration::from_secs(60)))
                            .tcp_nodelay(true)
                            .http2_keep_alive_interval(Duration::from_secs(60))
                            .keep_alive_timeout(Duration::from_secs(10))
                            .keep_alive_while_idle(true)
                            .http2_adaptive_window(true)
                            .connect()
                            .await
                            .ok()?;

                        let mut client = account_service_client::AccountServiceClient::new(channel);
                        let request = Request::new(ListBucketsRequest {
                            prefix: String::new(),
                            start_after: String::new(),
                            limit: Some(1),
                        });
                        client.list_buckets(request).await.ok()
                    })
                    .await;

                    if verify_check.is_ok() && verify_check.unwrap().is_some() {
                        verification_success += 1;
                    }

                    if verify_round < 4 {
                        // Don't sleep after the last attempt
                        sleep(Duration::from_millis(200)).await;
                    }
                }

                // Require at least 2 out of 5 verification attempts to succeed
                if verification_success >= 2 {
                    return true;
                }
            }

            // Progressive backoff: wait longer between attempts
            sleep(Duration::from_millis(800 + (300 * attempt as u64))).await;
        }
        false
    }

    async fn cleanup_ports() {
        for port in [SERVER_PORT, METRICS_PORT] {
            if let Ok(output) = StdCommand::new("lsof")
                .args([&format!("-ti:{}", port)])
                .output()
            {
                if !output.stdout.is_empty() {
                    let pids = String::from_utf8_lossy(&output.stdout);
                    for pid in pids.split_whitespace() {
                        let _ = StdCommand::new("kill").args(["-9", pid]).output();
                    }
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }

    fn cleanup() {
        let mut shared = SHARED_INFRA.lock().unwrap();
        if let Some(mut infra) = shared.take() {
            if let Some(mut server) = infra.server_process.take() {
                let _ = server.kill();
            }
            if let Some(mut etcd) = infra.etcd_process.take() {
                let _ = etcd.kill();
            }
            if !infra.is_etcd_external {
                let _ = StdCommand::new("docker")
                    .args(["rm", "-f", "etcd-test"])
                    .output();
            }
        }
    }
}

/// Initialize shared test infrastructure
pub async fn init_shared_infrastructure() -> Result<(), Box<dyn std::error::Error>> {
    // Check if already initialized
    {
        let shared = SHARED_INFRA.lock().unwrap();
        if shared.is_some() {
            return Ok(());
        }
    }

    SharedInfrastructure::initialize().await
}

/// Clean up shared infrastructure
pub fn cleanup_shared_infrastructure() {
    SharedInfrastructure::cleanup();
}

/// Clean etcd data between tests
pub async fn clean_etcd_data() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = etcd_client::Client::connect([ETCD_ENDPOINTS], None).await?;
    client
        .delete("", Some(etcd_client::DeleteOptions::new().with_prefix()))
        .await?;

    sleep(Duration::from_millis(100)).await;
    Ok(())
}

/// Comprehensive cleanup that removes both etcd data and any leftover buckets
pub async fn cleanup_test_data() -> Result<(), Box<dyn std::error::Error>> {
    // First clean etcd
    clean_etcd_data().await?;

    // Try to clean any leftover buckets through the API
    if let Ok(mut client) = SamsaTestClient::new().await {
        if let Ok(buckets) = client.list_buckets().await {
            for bucket in buckets {
                // Delete all test-related buckets
                if bucket.name.contains("test") || bucket.name.contains("bucket") {
                    let _ = client.delete_bucket(&bucket.name).await;
                }
            }
        }
    }

    // Clean etcd again to ensure everything is gone
    clean_etcd_data().await?;
    sleep(Duration::from_millis(100)).await;
    Ok(())
}

///  test environment
pub struct TestEnvironment {
    pub node_address: String,
}

impl TestEnvironment {
    pub async fn setup() -> Result<Self, Box<dyn std::error::Error>> {
        init_shared_infrastructure().await?;
        cleanup_test_data().await?;
        Ok(TestEnvironment {
            node_address: format!("http://127.0.0.1:{}", SERVER_PORT),
        })
    }

    pub async fn setup_minimal() -> Result<Self, Box<dyn std::error::Error>> {
        Self::setup().await
    }

    pub async fn cleanup_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        cleanup_test_data().await
    }
}

///  Samsa test client
pub struct SamsaTestClient {
    pub account_client: account_service_client::AccountServiceClient<Channel>,
    pub bucket_client: bucket_service_client::BucketServiceClient<Channel>,
    pub stream_client: stream_service_client::StreamServiceClient<Channel>,
}

impl SamsaTestClient {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Try multiple times to connect to handle any timing issues
        let mut last_error = None;

        for attempt in 0..10 {
            // Increased from 5 to 10 attempts
            if attempt > 0 {
                // Progressive backoff: wait longer between attempts
                sleep(Duration::from_millis(500 + (1000 * attempt as u64))).await;
            }

            match Channel::from_static("http://127.0.0.1:52000")
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(120))
                .tcp_keepalive(Some(Duration::from_secs(60)))
                .tcp_nodelay(true)
                .http2_keep_alive_interval(Duration::from_secs(60))
                .keep_alive_timeout(Duration::from_secs(10))
                .keep_alive_while_idle(true)
                .http2_adaptive_window(true)
                .connect()
                .await
            {
                Ok(channel) => {
                    // Test the connection works with multiple verification attempts
                    let mut test_client =
                        account_service_client::AccountServiceClient::new(channel.clone());

                    // Try multiple times to ensure connection is stable
                    let mut connection_verified = false;
                    for verify_attempt in 0..5 {
                        if verify_attempt > 0 {
                            sleep(Duration::from_millis(300)).await;
                        }

                        match timeout(
                            Duration::from_secs(10),
                            test_client.list_buckets(Request::new(ListBucketsRequest {
                                prefix: String::new(),
                                start_after: String::new(),
                                limit: Some(1),
                            })),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                connection_verified = true;
                                break;
                            }
                            Ok(Err(e)) => {
                                println!(
                                    "Connection test failed on verify attempt {}: {}",
                                    verify_attempt + 1,
                                    e
                                );
                                if verify_attempt == 4 {
                                    last_error = Some(Box::new(e) as Box<dyn std::error::Error>);
                                }
                            }
                            Err(_) => {
                                println!(
                                    "Connection test timed out on verify attempt {}",
                                    verify_attempt + 1
                                );
                                if verify_attempt == 4 {
                                    last_error = Some("Connection verification timeout".into());
                                }
                            }
                        }
                    }

                    if connection_verified {
                        // Create clients with the same improved channel settings
                        let account_channel = Channel::from_static("http://127.0.0.1:52000")
                            .connect_timeout(Duration::from_secs(15))
                            .timeout(Duration::from_secs(120))
                            .tcp_keepalive(Some(Duration::from_secs(60)))
                            .tcp_nodelay(true)
                            .http2_keep_alive_interval(Duration::from_secs(60))
                            .keep_alive_timeout(Duration::from_secs(10))
                            .keep_alive_while_idle(true)
                            .http2_adaptive_window(true)
                            .connect()
                            .await?;

                        let bucket_channel = Channel::from_static("http://127.0.0.1:52000")
                            .connect_timeout(Duration::from_secs(15))
                            .timeout(Duration::from_secs(120))
                            .tcp_keepalive(Some(Duration::from_secs(60)))
                            .tcp_nodelay(true)
                            .http2_keep_alive_interval(Duration::from_secs(60))
                            .keep_alive_timeout(Duration::from_secs(10))
                            .keep_alive_while_idle(true)
                            .http2_adaptive_window(true)
                            .connect()
                            .await?;

                        let stream_channel = Channel::from_static("http://127.0.0.1:52000")
                            .connect_timeout(Duration::from_secs(15))
                            .timeout(Duration::from_secs(120))
                            .tcp_keepalive(Some(Duration::from_secs(60)))
                            .tcp_nodelay(true)
                            .http2_keep_alive_interval(Duration::from_secs(60))
                            .keep_alive_timeout(Duration::from_secs(10))
                            .keep_alive_while_idle(true)
                            .http2_adaptive_window(true)
                            .connect()
                            .await?;

                        return Ok(SamsaTestClient {
                            account_client: account_service_client::AccountServiceClient::new(
                                account_channel,
                            ),
                            bucket_client: bucket_service_client::BucketServiceClient::new(
                                bucket_channel,
                            ),
                            stream_client: stream_service_client::StreamServiceClient::new(
                                stream_channel,
                            ),
                        });
                    }
                }
                Err(e) => {
                    println!("Connection attempt {} failed: {}", attempt + 1, e);
                    last_error = Some(e.into());
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "Failed to connect after all attempts".into()))
    }

    /// Create a bucket with default configuration
    pub async fn create_bucket(&mut self, bucket_name: &str) -> Result<BucketInfo, Status> {
        let request = CreateBucketRequest {
            bucket: bucket_name.to_string(),
            config: Some(default_bucket_config()),
        };

        let response = self
            .account_client
            .create_bucket(Request::new(request))
            .await?;
        Ok(response.into_inner().info.unwrap())
    }

    /// Create a stream with default configuration
    pub async fn create_stream(
        &mut self,
        bucket: &str,
        stream_name: &str,
    ) -> Result<StreamInfo, Status> {
        let request = CreateStreamRequest {
            stream: stream_name.to_string(),
            config: Some(default_stream_config()),
        };

        let mut request = Request::new(request);
        self.add_bucket_metadata(&mut request, bucket);

        let response = self.bucket_client.create_stream(request).await?;
        Ok(response.into_inner().info.unwrap())
    }

    /// Append text to a stream
    pub async fn append_text(
        &mut self,
        bucket: &str,
        stream: &str,
        text: &str,
    ) -> Result<AppendResponse, Status> {
        let records = vec![AppendRecord {
            timestamp: None,
            headers: vec![],
            body: text.as_bytes().to_vec(),
        }];

        let request = AppendRequest {
            bucket: bucket.to_string(),
            stream: stream.to_string(),
            records,
            match_seq_id: None,
            fencing_token: None,
        };

        // Simple retry logic for HTTP2 connection issues
        for attempt in 0..3 {
            if attempt > 0 {
                sleep(Duration::from_millis(200 + (attempt as u64 * 300))).await;
                println!("Retrying append_text attempt {}", attempt + 1);
            }

            match timeout(
                Duration::from_secs(30),
                self.stream_client.append(Request::new(request.clone())),
            )
            .await
            {
                Ok(Ok(response)) => return Ok(response.into_inner()),
                Ok(Err(e)) => {
                    println!("append_text failed on attempt {}: {}", attempt + 1, e);
                    if attempt == 2 {
                        return Err(e);
                    }
                }
                Err(_) => {
                    println!("append_text timed out on attempt {}", attempt + 1);
                    if attempt == 2 {
                        return Err(Status::deadline_exceeded("Request timed out"));
                    }
                }
            }
        }

        Err(Status::internal("All retry attempts failed"))
    }

    /// Read all records from a stream
    pub async fn read_all(
        &mut self,
        bucket: &str,
        stream: &str,
    ) -> Result<Vec<SequencedRecord>, Status> {
        let request = ReadRequest {
            bucket: bucket.to_string(),
            stream: stream.to_string(),
            start: Some(read_request::Start::SeqId(uuid::Uuid::nil().to_string())),
            limit: Some(ReadLimit {
                count: Some(1000),
                bytes: None,
            }),
        };

        let response = self.stream_client.read(request).await?;
        let read_response = response.into_inner();

        match read_response.output {
            Some(read_response::Output::Batch(batch)) => Ok(batch.records),
            _ => Ok(Vec::new()),
        }
    }

    /// List all buckets
    pub async fn list_buckets(&mut self) -> Result<Vec<BucketInfo>, Status> {
        let request = ListBucketsRequest {
            prefix: String::new(),
            start_after: String::new(),
            limit: Some(100),
        };

        let response = self
            .account_client
            .list_buckets(Request::new(request))
            .await?;
        Ok(response.into_inner().buckets)
    }

    /// List streams in a bucket
    pub async fn list_streams(&mut self, bucket: &str) -> Result<Vec<StreamInfo>, Status> {
        let request = ListStreamsRequest {
            prefix: String::new(),
            start_after: String::new(),
            limit: None,
        };

        let mut request = Request::new(request);
        self.add_bucket_metadata(&mut request, bucket);

        let response = self.bucket_client.list_streams(request).await?;
        Ok(response.into_inner().streams)
    }

    /// Delete a bucket
    pub async fn delete_bucket(&mut self, bucket_name: &str) -> Result<(), Status> {
        let request = DeleteBucketRequest {
            bucket: bucket_name.to_string(),
        };

        self.account_client
            .delete_bucket(Request::new(request))
            .await?;
        Ok(())
    }

    /// Delete a stream
    pub async fn delete_stream(&mut self, bucket: &str, stream_name: &str) -> Result<(), Status> {
        let request = DeleteStreamRequest {
            stream: stream_name.to_string(),
        };

        let mut request = Request::new(request);
        self.add_bucket_metadata(&mut request, bucket);

        self.bucket_client.delete_stream(request).await?;
        Ok(())
    }

    /// Create a bucket with custom configuration
    pub async fn create_bucket_with_config(
        &mut self,
        bucket_name: &str,
        config: BucketConfig,
    ) -> Result<BucketInfo, Status> {
        let request = CreateBucketRequest {
            bucket: bucket_name.to_string(),
            config: Some(config),
        };

        let response = self
            .account_client
            .create_bucket(Request::new(request))
            .await?;
        Ok(response.into_inner().info.unwrap())
    }

    /// Create a stream with custom configuration
    pub async fn create_stream_with_config(
        &mut self,
        bucket: &str,
        stream_name: &str,
        config: StreamConfig,
    ) -> Result<StreamInfo, Status> {
        let request = CreateStreamRequest {
            stream: stream_name.to_string(),
            config: Some(config),
        };

        let mut request = Request::new(request);
        self.add_bucket_metadata(&mut request, bucket);

        let response = self.bucket_client.create_stream(request).await?;
        Ok(response.into_inner().info.unwrap())
    }

    /// Append multiple records to a stream
    pub async fn append_records(
        &mut self,
        bucket: &str,
        stream: &str,
        records: Vec<AppendRecord>,
    ) -> Result<AppendResponse, Status> {
        let request = AppendRequest {
            bucket: bucket.to_string(),
            stream: stream.to_string(),
            records,
            match_seq_id: None,
            fencing_token: None,
        };

        let response = self.stream_client.append(Request::new(request)).await?;
        Ok(response.into_inner())
    }

    /// Read from a specific sequence ID
    pub async fn read_from_seq_id(
        &mut self,
        bucket: &str,
        stream: &str,
        seq_id: &str,
        count: Option<u64>,
    ) -> Result<ReadResponse, Status> {
        let request = ReadRequest {
            bucket: bucket.to_string(),
            stream: stream.to_string(),
            start: Some(read_request::Start::SeqId(seq_id.to_string())),
            limit: count.map(|count| ReadLimit {
                count: Some(count),
                bytes: None,
            }),
        };

        // Simple retry logic for HTTP2 connection issues
        for attempt in 0..3 {
            if attempt > 0 {
                sleep(Duration::from_millis(200 + (attempt as u64 * 300))).await;
                println!("Retrying read_from_seq_id attempt {}", attempt + 1);
            }

            match timeout(
                Duration::from_secs(30),
                self.stream_client.read(Request::new(request.clone())),
            )
            .await
            {
                Ok(Ok(response)) => return Ok(response.into_inner()),
                Ok(Err(e)) => {
                    println!("read_from_seq_id failed on attempt {}: {}", attempt + 1, e);
                    if attempt == 2 {
                        return Err(e);
                    }
                }
                Err(_) => {
                    println!("read_from_seq_id timed out on attempt {}", attempt + 1);
                    if attempt == 2 {
                        return Err(Status::deadline_exceeded("Request timed out"));
                    }
                }
            }
        }

        Err(Status::internal("All retry attempts failed"))
    }

    /// Get the tail information for a stream
    pub async fn get_tail(
        &mut self,
        bucket: &str,
        stream: &str,
    ) -> Result<CheckTailResponse, Status> {
        let request = CheckTailRequest {
            bucket: bucket.to_string(),
            stream: stream.to_string(),
        };

        // Simple retry logic for HTTP2 connection issues
        for attempt in 0..3 {
            if attempt > 0 {
                sleep(Duration::from_millis(200 + (attempt as u64 * 300))).await;
                println!("Retrying get_tail attempt {}", attempt + 1);
            }

            match timeout(
                Duration::from_secs(30),
                self.stream_client.check_tail(Request::new(request.clone())),
            )
            .await
            {
                Ok(Ok(response)) => return Ok(response.into_inner()),
                Ok(Err(e)) => {
                    println!("get_tail failed on attempt {}: {}", attempt + 1, e);
                    if attempt == 2 {
                        return Err(e);
                    }
                }
                Err(_) => {
                    println!("get_tail timed out on attempt {}", attempt + 1);
                    if attempt == 2 {
                        return Err(Status::deadline_exceeded("Request timed out"));
                    }
                }
            }
        }

        Err(Status::internal("All retry attempts failed"))
    }

    /// Issue an access token
    pub async fn issue_access_token(&mut self, info: AccessTokenInfo) -> Result<String, Status> {
        let request = IssueAccessTokenRequest { info: Some(info) };

        let response = self
            .account_client
            .issue_access_token(Request::new(request))
            .await?;
        Ok(response.into_inner().access_token)
    }

    /// Revoke an access token
    pub async fn revoke_access_token(&mut self, id: &str) -> Result<AccessTokenInfo, Status> {
        let request = RevokeAccessTokenRequest { id: id.to_string() };

        let response = self
            .account_client
            .revoke_access_token(Request::new(request))
            .await?;
        Ok(response.into_inner().info.unwrap())
    }

    /// List access tokens
    pub async fn list_access_tokens(&mut self) -> Result<Vec<AccessTokenInfo>, Status> {
        let request = ListAccessTokensRequest {
            prefix: String::new(),
            start_after: String::new(),
            limit: Some(1000),
        };

        let response = self
            .account_client
            .list_access_tokens(Request::new(request))
            .await?;
        Ok(response.into_inner().access_tokens)
    }

    /// Get bucket configuration
    pub async fn get_bucket_config(&mut self, bucket_name: &str) -> Result<BucketConfig, Status> {
        let request = GetBucketConfigRequest {
            bucket: bucket_name.to_string(),
        };

        let response = self
            .account_client
            .get_bucket_config(Request::new(request))
            .await?;
        Ok(response.into_inner().config.unwrap())
    }

    /// Get stream configuration
    pub async fn get_stream_config(
        &mut self,
        bucket: &str,
        stream_name: &str,
    ) -> Result<StreamConfig, Status> {
        let request = GetStreamConfigRequest {
            stream: stream_name.to_string(),
        };

        let mut request = Request::new(request);
        self.add_bucket_metadata(&mut request, bucket);

        let response = self.bucket_client.get_stream_config(request).await?;
        Ok(response.into_inner().config.unwrap())
    }

    /// Create a new test client with the specified address
    pub async fn new_with_address(address: &str) -> Result<Self, String> {
        // Create separate channels for each client to avoid sharing issues
        let account_channel = Channel::from_shared(address.to_string())
            .map_err(|e| format!("Invalid address: {}", e))?
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(120))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(60))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .connect()
            .await
            .map_err(|e| format!("Account connection failed: {}", e))?;

        let bucket_channel = Channel::from_shared(address.to_string())
            .map_err(|e| format!("Invalid address: {}", e))?
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(120))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(60))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .connect()
            .await
            .map_err(|e| format!("Bucket connection failed: {}", e))?;

        let stream_channel = Channel::from_shared(address.to_string())
            .map_err(|e| format!("Invalid address: {}", e))?
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(120))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(60))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .connect()
            .await
            .map_err(|e| format!("Stream connection failed: {}", e))?;

        Ok(SamsaTestClient {
            account_client: account_service_client::AccountServiceClient::new(account_channel),
            bucket_client: bucket_service_client::BucketServiceClient::new(bucket_channel),
            stream_client: stream_service_client::StreamServiceClient::new(stream_channel),
        })
    }

    /// Helper to add bucket metadata to requests
    fn add_bucket_metadata<T>(&self, request: &mut Request<T>, bucket: &str) {
        request
            .metadata_mut()
            .insert("bucket", bucket.parse().unwrap());
    }
}

/// Default configurations
pub fn default_bucket_config() -> BucketConfig {
    BucketConfig {
        default_stream_config: Some(default_stream_config()),
        create_stream_on_append: true,
        create_stream_on_read: false,
    }
}

pub fn default_stream_config() -> StreamConfig {
    StreamConfig {
        storage_class: StorageClass::Standard as i32,
        retention_age_seconds: Some(86400), // 1 day
        timestamping_mode: TimestampingMode::Arrival as i32,
        allow_future_timestamps: false,
    }
}

/// Helper to extract text from a record
pub fn record_to_text(record: &SequencedRecord) -> String {
    String::from_utf8_lossy(&record.body).to_string()
}

/// Generate unique test names
pub fn unique_name(prefix: &str) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{}", prefix, timestamp)
}

/// Helper function to create test records
pub fn create_test_record(content: &str) -> AppendRecord {
    AppendRecord {
        timestamp: None,
        headers: vec![],
        body: content.as_bytes().to_vec(),
    }
}

/// Helper function to create test records with headers
pub fn create_test_record_with_headers(content: &str, headers: Vec<(&str, &str)>) -> AppendRecord {
    let headers = headers
        .into_iter()
        .map(|(name, value)| Header {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        })
        .collect();

    AppendRecord {
        timestamp: None,
        headers,
        body: content.as_bytes().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_infrastructure_setup() {
        let _env = TestEnvironment::setup().await.unwrap();
        let mut client = SamsaTestClient::new().await.unwrap();

        // Basic smoke test
        let buckets = client.list_buckets().await.unwrap();
        if !buckets.is_empty() {
            println!("Found {} buckets after cleanup:", buckets.len());
            for bucket in &buckets {
                println!("  - {}", bucket.name);
            }
        }

        // Create and delete a test bucket
        client.create_bucket("test-bucket").await.unwrap();
        let buckets = client.list_buckets().await.unwrap();
        assert!(!buckets.is_empty());

        client.delete_bucket("test-bucket").await.unwrap();
    }
}
