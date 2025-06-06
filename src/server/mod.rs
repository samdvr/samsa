pub mod handlers;

use clap::Parser;
use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use tracing::{error, info, warn};

use crate::common::{
    config::{ObservabilityConfig, ServerConfig},
    error::{Result, SamsaError},
    etcd_client::{EtcdClient, EtcdConfig, EtcdEvent, NodeInfo, NodeStatus},
    observability::init_observability,
    storage::Storage,
};
use crate::proto::{
    account_service_server::AccountServiceServer, bucket_service_server::BucketServiceServer,
    stream_service_server::StreamServiceServer,
};
use handlers::{AccountHandler, BucketHandler, StreamHandler};

#[derive(Parser, Debug)]
#[command(name = "server")]
#[command(about = "A unified server for the Samsa distributed storage system")]
#[command(version)]
pub struct Args {
    /// Node ID for this server instance
    #[arg(long, value_name = "UUID")]
    pub node_id: Option<String>,
}

pub async fn run_server() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize observability first (metrics + enhanced tracing)
    let observability_config = ObservabilityConfig::from_env();
    if let Err(e) = init_observability(observability_config.clone()).await {
        eprintln!("Failed to initialize observability: {}", e);
        // Continue without observability rather than failing completely
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("samsa=info".parse().unwrap()),
            )
            .init();
        warn!(
            "Observability initialization failed, using basic logging: {}",
            e
        );
    }

    info!("Starting Samsa Unified Server (account, bucket, and stream services)...");
    info!(
        "Observability configured - metrics port: {}, service: {}, node: {}",
        observability_config.metrics_port,
        observability_config.service_name,
        observability_config.node_id
    );

    // Load configuration
    let mut config = ServerConfig::from_env()?;

    // Override node_id from command line if provided
    if let Some(ref node_id_str) = args.node_id {
        let parsed_node_id = uuid::Uuid::parse_str(node_id_str)
            .map_err(|e| SamsaError::Config(format!("Invalid node ID format: {}", e)))?;
        config.node_id = parsed_node_id;
        info!("Using node ID from command line: {}", parsed_node_id);
    }

    info!("Server configuration loaded: {:?}", config);

    // Initialize storage
    let object_store = Arc::new(InMemory::new());
    let storage = Arc::new(Storage::new_with_memory(
        object_store,
        config.node_id.to_string(),
    ));
    info!("Storage initialized");

    // Initialize etcd client for service discovery
    let node_info = NodeInfo::new(config.node_id, config.address.clone(), config.port);

    let etcd_config = EtcdConfig {
        endpoints: config.etcd_endpoints.clone(),
        heartbeat_interval: Duration::from_secs(config.heartbeat_interval_secs),
        lease_ttl: Duration::from_secs(config.lease_ttl_secs),
        max_retry_attempts: 5,
        retry_backoff_base: Duration::from_millis(500),
        max_retry_backoff: Duration::from_secs(30),
        recovery_timeout: Duration::from_secs(300),
        max_consecutive_heartbeat_failures: 10,
    };

    let (mut etcd_client, mut event_receiver) = EtcdClient::new(etcd_config, node_info).await?;

    // Start a task to handle etcd events
    tokio::spawn(async move {
        while let Some(event) = event_receiver.recv().await {
            match event {
                EtcdEvent::NodeRegistered => {
                    info!("Successfully registered with etcd cluster");
                }
                EtcdEvent::ConnectionLost(error) => {
                    warn!("Lost connection to etcd: {}", error);
                    // Here you could implement additional logic like:
                    // - Marking the node as read-only
                    // - Stopping accepting new requests
                    // - Alerting monitoring systems
                }
                EtcdEvent::ConnectionRecovered => {
                    info!("Etcd connection recovered successfully");
                    // Here you could implement recovery logic like:
                    // - Resuming full operations
                    // - Clearing read-only mode
                }
                EtcdEvent::RegistrationFailed(error) => {
                    error!("Failed to register with etcd: {}", error);
                    // Here you could implement failure handling like:
                    // - Graceful shutdown
                    // - Fallback to standalone mode
                    // - Retry with different etcd endpoints
                }
                EtcdEvent::StatusUpdated(status) => {
                    info!("Node status updated to: {:?}", status);
                }
            }
        }
    });

    // Register with etcd and start heartbeat
    etcd_client.register_and_start_heartbeat().await?;
    etcd_client.update_status(NodeStatus::Ready).await?;
    info!("Node registered with etcd and marked as ready");

    // Create gRPC service handlers - all services are hosted on this unified server
    let etcd_client_arc = Arc::new(etcd_client);
    let account_handler = AccountHandler::new(storage.clone(), etcd_client_arc.clone());
    let bucket_handler = BucketHandler::new(storage.clone(), etcd_client_arc.clone());
    let stream_handler = StreamHandler::new(storage.clone());

    // Build and start the server with all services
    let addr = config
        .endpoint()
        .parse()
        .map_err(|e| SamsaError::Config(format!("Invalid address: {}", e)))?;

    info!("Starting unified gRPC server on {} with all services", addr);

    let server = Server::builder()
        // Configure HTTP/2 settings for better connection stability
        .timeout(Duration::from_secs(60))
        .concurrency_limit_per_connection(256)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .tcp_nodelay(true)
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(5)))
        .http2_adaptive_window(Some(true))
        .max_concurrent_streams(Some(100))
        .add_service(AccountServiceServer::new(account_handler))
        .add_service(BucketServiceServer::new(bucket_handler))
        .add_service(StreamServiceServer::new(stream_handler))
        .serve(addr);

    // Handle graceful shutdown
    tokio::select! {
        result = server => {
            if let Err(e) = result {
                error!("Server error: {}", e);
                return Err(SamsaError::Network(e.to_string()));
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }

    // Graceful shutdown: update status to unhealthy and unregister
    info!("Initiating graceful shutdown...");

    // Try to get mutable reference and unregister properly
    if let Ok(mut etcd_client_mut) = Arc::try_unwrap(etcd_client_arc) {
        if let Err(e) = etcd_client_mut.update_status(NodeStatus::Unhealthy).await {
            warn!(
                "Failed to update status to unhealthy during shutdown: {}",
                e
            );
        }
        if let Err(e) = etcd_client_mut.unregister_node().await {
            warn!("Failed to unregister node during shutdown: {}", e);
        }
    } else {
        // Fallback: create a new client to unregister (though this is less reliable)
        let node_info = NodeInfo::new(config.node_id, config.address.clone(), config.port);
        let etcd_config = EtcdConfig {
            endpoints: config.etcd_endpoints.clone(),
            heartbeat_interval: Duration::from_secs(config.heartbeat_interval_secs),
            lease_ttl: Duration::from_secs(config.lease_ttl_secs),
            ..Default::default()
        };

        if let Ok((mut cleanup_client, _)) = EtcdClient::new(etcd_config, node_info).await {
            if let Err(e) = cleanup_client.unregister_node().await {
                warn!("Failed to unregister node during cleanup: {}", e);
            }
        }
    }

    info!("Node unregistered from etcd");
    info!("Samsa Unified Server shutdown complete");
    Ok(())
}
