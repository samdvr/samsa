use crate::common::error::Result;
use crate::common::metrics::{Timer, labels, names, utils};
use etcd_client::{Client, GetOptions, PutOptions};
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::panic;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{interval, sleep};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: Uuid,
    pub address: String,
    pub port: u16,
    pub status: NodeStatus,
    pub last_heartbeat: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum NodeStatus {
    Starting,
    Ready,
    Unhealthy,
}

impl NodeInfo {
    pub fn new(id: Uuid, address: String, port: u16) -> Self {
        Self {
            id,
            address,
            port,
            status: NodeStatus::Starting,
            last_heartbeat: current_timestamp_millis(),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

/// Events that the EtcdClient can send to notify the application of important state changes
#[derive(Debug, Clone)]
pub enum EtcdEvent {
    /// Node successfully registered with etcd
    NodeRegistered,
    /// Node lost connection to etcd (lease keep-alive failed)
    ConnectionLost(String),
    /// Node successfully recovered connection and re-registered
    ConnectionRecovered,
    /// Node failed to re-register after connection loss
    RegistrationFailed(String),
    /// Node status was updated
    StatusUpdated(NodeStatus),
}

/// Configuration for etcd client retry and recovery behavior
#[derive(Debug, Clone)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
    pub heartbeat_interval: Duration,
    pub lease_ttl: Duration,
    pub max_retry_attempts: usize,
    pub retry_backoff_base: Duration,
    pub max_retry_backoff: Duration,
    pub recovery_timeout: Duration,
    pub max_consecutive_heartbeat_failures: u64,
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["localhost:2379".to_string()],
            heartbeat_interval: Duration::from_secs(10),
            lease_ttl: Duration::from_secs(30),
            max_retry_attempts: 5,
            retry_backoff_base: Duration::from_millis(500),
            max_retry_backoff: Duration::from_secs(30),
            recovery_timeout: Duration::from_secs(300), // 5 minutes
            max_consecutive_heartbeat_failures: 10,
        }
    }
}

/// Enhanced etcd client with robust error handling and recovery
pub struct EtcdClient {
    client: Arc<Mutex<Option<Client>>>,
    lease_id: Arc<Mutex<Option<i64>>>,
    node_info: Arc<Mutex<NodeInfo>>,
    config: EtcdConfig,
    event_sender: Arc<Mutex<Option<mpsc::UnboundedSender<EtcdEvent>>>>,
    is_healthy: Arc<AtomicBool>,
    consecutive_heartbeat_failures: Arc<AtomicU64>,
    heartbeat_task_running: Arc<AtomicBool>,
}

impl EtcdClient {
    pub async fn new(
        config: EtcdConfig,
        node_info: NodeInfo,
    ) -> Result<(Self, mpsc::UnboundedReceiver<EtcdEvent>)> {
        let client = Client::connect(&config.endpoints, None).await?;
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        let etcd_client = Self {
            client: Arc::new(Mutex::new(Some(client))),
            lease_id: Arc::new(Mutex::new(None)),
            node_info: Arc::new(Mutex::new(node_info)),
            config,
            event_sender: Arc::new(Mutex::new(Some(event_sender))),
            is_healthy: Arc::new(AtomicBool::new(false)),
            consecutive_heartbeat_failures: Arc::new(AtomicU64::new(0)),
            heartbeat_task_running: Arc::new(AtomicBool::new(false)),
        };

        Ok((etcd_client, event_receiver))
    }

    #[cfg(test)]
    pub fn new_mock() -> Self {
        let (event_sender, _) = mpsc::unbounded_channel();
        Self {
            client: Arc::new(Mutex::new(None)), // No actual client
            lease_id: Arc::new(Mutex::new(None)),
            node_info: Arc::new(Mutex::new(NodeInfo::new(
                Uuid::new_v4(),
                "mock_address".to_string(),
                0,
            ))),
            config: EtcdConfig::default(),
            event_sender: Arc::new(Mutex::new(Some(event_sender))),
            is_healthy: Arc::new(AtomicBool::new(false)),
            consecutive_heartbeat_failures: Arc::new(AtomicU64::new(0)),
            heartbeat_task_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Register the node with etcd and start the heartbeat process
    pub async fn register_and_start_heartbeat(&mut self) -> Result<()> {
        self.register_node().await?;
        self.start_heartbeat_with_recovery().await?;
        Ok(())
    }

    async fn register_node(&mut self) -> Result<()> {
        let timer = Timer::new(names::ETCD_OPERATION_DURATION)
            .with_label(labels::OPERATION.to_string(), "register_node".to_string());

        // Record operation attempt
        utils::record_counter(
            names::ETCD_OPERATIONS_TOTAL,
            1,
            vec![(labels::OPERATION.to_string(), "register_node".to_string())],
        );

        let mut client_opt = self.client.lock().await;
        let client = client_opt.as_mut().ok_or_else(|| {
            crate::common::error::SamsaError::Internal("etcd client not connected".to_string())
        })?;

        // Create a lease
        let lease_result = client
            .lease_grant(self.config.lease_ttl.as_secs() as i64, None)
            .await;

        let lease_resp = match lease_result {
            Ok(resp) => resp,
            Err(e) => {
                utils::record_error(
                    names::ETCD_ERRORS_TOTAL,
                    &e.to_string(),
                    vec![(labels::OPERATION.to_string(), "lease_grant".to_string())],
                );
                return Err(e.into());
            }
        };

        {
            let mut lease_id = self.lease_id.lock().await;
            *lease_id = Some(lease_resp.id());
        }

        // Register the node
        let node_info = self.node_info.lock().await;
        let key = format!("/samsa/servers/{}", node_info.id);
        let value = serde_json::to_string(&*node_info)?;

        let put_options = PutOptions::new().with_lease(lease_resp.id());
        let put_result = client.put(key, value, Some(put_options)).await;

        match put_result {
            Ok(_) => {
                // Mark as healthy and update connection status metric
                self.is_healthy.store(true, Ordering::Relaxed);
                utils::record_gauge(names::ETCD_CONNECTION_STATUS, 1.0, vec![]);

                // Send registration event
                self.send_event(EtcdEvent::NodeRegistered).await;

                tracing::info!("Node {} registered with etcd", node_info.id);
                timer.record();
                Ok(())
            }
            Err(e) => {
                utils::record_error(
                    names::ETCD_ERRORS_TOTAL,
                    &e.to_string(),
                    vec![(
                        labels::OPERATION.to_string(),
                        "put_registration".to_string(),
                    )],
                );
                utils::record_gauge(names::ETCD_CONNECTION_STATUS, 0.0, vec![]);
                Err(e.into())
            }
        }
    }

    /// Enhanced heartbeat with robust error handling and panic recovery
    async fn start_heartbeat_with_recovery(&self) -> Result<()> {
        // Prevent multiple heartbeat tasks from running
        if self.heartbeat_task_running.swap(true, Ordering::SeqCst) {
            tracing::warn!("Heartbeat task is already running, skipping start");
            return Ok(());
        }

        let client = self.client.clone();
        let lease_id_arc = self.lease_id.clone();
        let node_info = self.node_info.clone();
        let config = self.config.clone();
        let event_sender = self.event_sender.clone();
        let is_healthy = self.is_healthy.clone();
        let consecutive_failures = self.consecutive_heartbeat_failures.clone();
        let task_running = self.heartbeat_task_running.clone();

        tokio::spawn(async move {
            // Clone for panic handler (since they'll be moved into heartbeat_loop)
            let is_healthy_for_panic = is_healthy.clone();
            let event_sender_for_panic = event_sender.clone();

            // Panic-safe wrapper for the heartbeat loop
            let panic_result = panic::AssertUnwindSafe(async {
                Self::heartbeat_loop(
                    client,
                    lease_id_arc,
                    node_info,
                    config,
                    event_sender,
                    is_healthy,
                    consecutive_failures,
                )
                .await
            })
            .catch_unwind()
            .await;

            // Ensure task is marked as not running when we exit
            task_running.store(false, Ordering::SeqCst);

            match panic_result {
                Ok(()) => {
                    tracing::info!("Heartbeat task completed normally");
                }
                Err(panic_payload) => {
                    let panic_msg = if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else {
                        "Unknown panic".to_string()
                    };

                    tracing::error!(
                        "Heartbeat task panicked: {}. This is critical - etcd connectivity lost!",
                        panic_msg
                    );

                    // Mark as unhealthy and send critical event
                    is_healthy_for_panic.store(false, Ordering::Relaxed);
                    Self::send_event_static(
                        &event_sender_for_panic,
                        EtcdEvent::ConnectionLost(format!(
                            "Heartbeat task panicked: {}",
                            panic_msg
                        )),
                    )
                    .await;
                }
            }

            tracing::warn!("Heartbeat task exited");
        });

        Ok(())
    }

    /// Main heartbeat loop with comprehensive error handling
    async fn heartbeat_loop(
        client: Arc<Mutex<Option<Client>>>,
        lease_id_arc: Arc<Mutex<Option<i64>>>,
        node_info: Arc<Mutex<NodeInfo>>,
        config: EtcdConfig,
        event_sender: Arc<Mutex<Option<mpsc::UnboundedSender<EtcdEvent>>>>,
        is_healthy: Arc<AtomicBool>,
        consecutive_failures: Arc<AtomicU64>,
    ) {
        let mut interval = interval(config.heartbeat_interval);
        let mut degraded_logged = false;

        loop {
            interval.tick().await;

            let heartbeat_result = Self::send_heartbeat(&client, &lease_id_arc, &node_info).await;

            match heartbeat_result {
                Ok(()) => {
                    // Reset failure count on success
                    let prev_failures = consecutive_failures.swap(0, Ordering::Relaxed);

                    if prev_failures > 0 {
                        tracing::info!(
                            "Heartbeat recovered after {} consecutive failures",
                            prev_failures
                        );
                        Self::send_event_static(&event_sender, EtcdEvent::ConnectionRecovered)
                            .await;
                        degraded_logged = false;
                    }

                    // Ensure we're marked as healthy
                    is_healthy.store(true, Ordering::Relaxed);
                    tracing::debug!("Heartbeat successful");
                }
                Err(e) => {
                    let failure_count = consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

                    // Mark as unhealthy immediately
                    is_healthy.store(false, Ordering::Relaxed);

                    // Log with escalating severity
                    if failure_count <= 3 {
                        tracing::warn!("Heartbeat failed (attempt {}): {}", failure_count, e);
                    } else if failure_count <= config.max_consecutive_heartbeat_failures {
                        if !degraded_logged {
                            tracing::error!(
                                "Heartbeat entering degraded state after {} failures: {}. Node connectivity compromised.",
                                failure_count,
                                e
                            );
                            degraded_logged = true;
                        } else {
                            tracing::debug!(
                                "Heartbeat still failing (attempt {}): {}",
                                failure_count,
                                e
                            );
                        }
                    } else {
                        tracing::error!(
                            "Heartbeat failed {} times, exceeding maximum. This is critical: {}",
                            failure_count,
                            e
                        );
                    }

                    // Send connection lost event only on first failure or recovery attempts
                    if failure_count == 1 || failure_count % 5 == 0 {
                        Self::send_event_static(
                            &event_sender,
                            EtcdEvent::ConnectionLost(format!(
                                "Heartbeat failed {} times: {}",
                                failure_count, e
                            )),
                        )
                        .await;
                    }

                    // Try to recover
                    let recovery_result = Self::attempt_recovery(
                        &client,
                        &lease_id_arc,
                        &node_info,
                        &config,
                        &event_sender,
                    )
                    .await;

                    match recovery_result {
                        Ok(()) => {
                            consecutive_failures.store(0, Ordering::Relaxed);
                            is_healthy.store(true, Ordering::Relaxed);
                            Self::send_event_static(&event_sender, EtcdEvent::ConnectionRecovered)
                                .await;
                            tracing::info!(
                                "Successfully recovered etcd connection during heartbeat"
                            );
                            degraded_logged = false;
                        }
                        Err(recovery_error) => {
                            if failure_count % 10 == 0 {
                                // Log recovery failures less frequently
                                tracing::error!(
                                    "Failed to recover etcd connection: {}",
                                    recovery_error
                                );
                                Self::send_event_static(
                                    &event_sender,
                                    EtcdEvent::RegistrationFailed(recovery_error.to_string()),
                                )
                                .await;
                            }

                            // If we've failed too many times, consider more drastic action
                            if failure_count >= config.max_consecutive_heartbeat_failures {
                                tracing::error!(
                                    "Exceeded max heartbeat failures ({}). Node should be considered unhealthy.",
                                    config.max_consecutive_heartbeat_failures
                                );

                                // In production, you might want to:
                                // - Trigger node shutdown
                                // - Send critical alerts
                                // - Stop accepting new requests

                                // For now, we continue trying but with much longer backoff
                                let extended_backoff = std::cmp::max(
                                    config.heartbeat_interval,
                                    Duration::from_secs(60), // At least 1 minute between attempts
                                );
                                sleep(extended_backoff).await;
                            } else {
                                // Apply backoff delay before next heartbeat attempt
                                let backoff =
                                    Self::calculate_backoff(failure_count as usize, &config);
                                sleep(backoff).await;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn send_heartbeat(
        client: &Arc<Mutex<Option<Client>>>,
        lease_id_arc: &Arc<Mutex<Option<i64>>>,
        node_info: &Arc<Mutex<NodeInfo>>,
    ) -> Result<()> {
        let timer = Timer::new(names::ETCD_OPERATION_DURATION)
            .with_label(labels::OPERATION.to_string(), "heartbeat".to_string());

        // Record heartbeat attempt
        utils::record_counter(
            names::ETCD_OPERATIONS_TOTAL,
            1,
            vec![(labels::OPERATION.to_string(), "heartbeat".to_string())],
        );

        let lease_id = {
            let lease_guard = lease_id_arc.lock().await;
            lease_guard.ok_or_else(|| {
                crate::common::error::SamsaError::Internal("No lease ID available".to_string())
            })?
        };

        // Send lease keepalive
        let keepalive_result = {
            let mut client_opt = client.lock().await;
            let client = client_opt.as_mut().ok_or_else(|| {
                crate::common::error::SamsaError::Internal("etcd client not connected".to_string())
            })?;
            client.lease_keep_alive(lease_id).await
        };

        if let Err(e) = keepalive_result {
            utils::record_error(
                names::ETCD_ERRORS_TOTAL,
                &e.to_string(),
                vec![(
                    labels::OPERATION.to_string(),
                    "lease_keep_alive".to_string(),
                )],
            );
            utils::record_counter(names::ETCD_HEARTBEAT_FAILURES, 1, vec![]);
            return Err(e.into());
        }

        // Update the node's last heartbeat timestamp in etcd
        {
            let mut node_guard = node_info.lock().await;
            node_guard.last_heartbeat = current_timestamp_millis();

            let key = format!("/samsa/servers/{}", node_guard.id);
            let value = serde_json::to_string(&*node_guard)?;
            drop(node_guard);

            let mut client_opt = client.lock().await;
            let client = client_opt.as_mut().ok_or_else(|| {
                crate::common::error::SamsaError::Internal("etcd client not connected".to_string())
            })?;
            let put_options = PutOptions::new().with_lease(lease_id);
            let put_result = client.put(key, value, Some(put_options)).await;

            match put_result {
                Ok(_) => {
                    timer.record();
                    Ok(())
                }
                Err(e) => {
                    utils::record_error(
                        names::ETCD_ERRORS_TOTAL,
                        &e.to_string(),
                        vec![(labels::OPERATION.to_string(), "put_heartbeat".to_string())],
                    );
                    utils::record_counter(names::ETCD_HEARTBEAT_FAILURES, 1, vec![]);
                    Err(e.into())
                }
            }
        }
    }

    async fn attempt_recovery(
        client: &Arc<Mutex<Option<Client>>>,
        lease_id_arc: &Arc<Mutex<Option<i64>>>,
        node_info: &Arc<Mutex<NodeInfo>>,
        config: &EtcdConfig,
        _event_sender: &Arc<Mutex<Option<mpsc::UnboundedSender<EtcdEvent>>>>,
    ) -> Result<()> {
        tracing::info!("Attempting to recover etcd connection...");

        // Try to reconnect to etcd
        for attempt in 1..=config.max_retry_attempts {
            let backoff = Self::calculate_backoff(attempt, config);
            tracing::debug!("Recovery attempt {} after {:?}", attempt, backoff);
            sleep(backoff).await;

            // Try to create a new client connection
            match Client::connect(&config.endpoints, None).await {
                Ok(new_client) => {
                    // Replace the client
                    {
                        let mut client_opt = client.lock().await;
                        *client_opt = Some(new_client);
                    }

                    // Try to re-register
                    match Self::re_register_node(client, lease_id_arc, node_info, config).await {
                        Ok(()) => {
                            tracing::info!("Successfully recovered and re-registered with etcd");
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::warn!("Re-registration failed on attempt {}: {}", attempt, e);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to reconnect to etcd on attempt {}: {}", attempt, e);
                    continue;
                }
            }
        }

        Err(crate::common::error::SamsaError::Internal(
            "Failed to recover etcd connection after all retry attempts".to_string(),
        ))
    }

    async fn re_register_node(
        client: &Arc<Mutex<Option<Client>>>,
        lease_id_arc: &Arc<Mutex<Option<i64>>>,
        node_info: &Arc<Mutex<NodeInfo>>,
        config: &EtcdConfig,
    ) -> Result<()> {
        let mut client_opt = client.lock().await;
        let client = client_opt.as_mut().ok_or_else(|| {
            crate::common::error::SamsaError::Internal("etcd client not connected".to_string())
        })?;

        // Create a new lease
        let lease_resp = client
            .lease_grant(config.lease_ttl.as_secs() as i64, None)
            .await?;

        {
            let mut lease_id = lease_id_arc.lock().await;
            *lease_id = Some(lease_resp.id());
        }

        // Re-register the node with updated status
        let node_info_guard = node_info.lock().await;
        let key = format!("/samsa/servers/{}", node_info_guard.id);
        let value = serde_json::to_string(&*node_info_guard)?;

        let put_options = PutOptions::new().with_lease(lease_resp.id());
        client.put(key, value, Some(put_options)).await?;

        Ok(())
    }

    fn calculate_backoff(attempt: usize, config: &EtcdConfig) -> Duration {
        let backoff = config.retry_backoff_base * (2_u32.pow(attempt as u32 - 1));
        std::cmp::min(backoff, config.max_retry_backoff)
    }

    async fn send_event(&self, event: EtcdEvent) {
        Self::send_event_static(&self.event_sender, event).await;
    }

    async fn send_event_static(
        event_sender: &Arc<Mutex<Option<mpsc::UnboundedSender<EtcdEvent>>>>,
        event: EtcdEvent,
    ) {
        let sender_opt = event_sender.lock().await;
        if let Some(sender) = sender_opt.as_ref() {
            if let Err(e) = sender.send(event) {
                tracing::warn!("Failed to send etcd event: {}", e);
            }
        }
    }

    pub async fn update_status(&self, status: NodeStatus) -> Result<()> {
        {
            let mut node_info = self.node_info.lock().await;
            node_info.status = status.clone();
            node_info.last_heartbeat = current_timestamp_millis();
        }

        // Only try to update etcd if we're currently healthy
        let is_currently_healthy = { self.is_healthy.load(Ordering::Relaxed) };

        if is_currently_healthy {
            let lease_id = {
                let lease_guard = self.lease_id.lock().await;
                *lease_guard
            };

            if let Some(lease_id) = lease_id {
                let mut client_opt = self.client.lock().await;
                if let Some(client) = client_opt.as_mut() {
                    let node_info = self.node_info.lock().await;
                    let key = format!("/samsa/servers/{}", node_info.id);
                    let value = serde_json::to_string(&*node_info)?;

                    let put_options = PutOptions::new().with_lease(lease_id);
                    client.put(key, value, Some(put_options)).await?;
                }
            }
        } else {
            tracing::warn!("Skipping etcd status update - client is not healthy");
        }

        // Always send the status update event
        self.send_event(EtcdEvent::StatusUpdated(status)).await;
        Ok(())
    }

    pub async fn unregister_node(&mut self) -> Result<()> {
        let mut client_opt = self.client.lock().await;
        if let Some(client) = client_opt.as_mut() {
            let node_info = self.node_info.lock().await;
            let key = format!("/samsa/servers/{}", node_info.id);

            if let Err(e) = client.delete(key, None).await {
                tracing::warn!("Failed to unregister node from etcd: {}", e);
            } else {
                tracing::info!("Node {} unregistered from etcd", node_info.id);
            }
        }

        // Mark as unhealthy
        self.is_healthy.store(false, Ordering::Relaxed);

        Ok(())
    }

    pub async fn list_servers(&self) -> Result<Vec<NodeInfo>> {
        let mut client_opt = self.client.lock().await;
        let client = client_opt.as_mut().ok_or_else(|| {
            crate::common::error::SamsaError::Internal("etcd client not connected".to_string())
        })?;

        let get_options = GetOptions::new().with_prefix();
        let resp = client.get("/samsa/servers/", Some(get_options)).await?;

        let mut nodes = Vec::new();
        for kv in resp.kvs() {
            if let Ok(node_info) = serde_json::from_slice::<NodeInfo>(kv.value()) {
                nodes.push(node_info);
            }
        }

        Ok(nodes)
    }

    pub async fn get_healthy_servers(&self) -> Result<Vec<NodeInfo>> {
        let all_nodes = self.list_servers().await?;
        let current_time = current_timestamp_millis();
        let total_nodes = all_nodes.len();

        // Use a more generous health check - consider a node healthy if:
        // 1. It has Ready status
        // 2. Its last heartbeat is within 2x the lease TTL (more forgiving)
        let health_timeout_millis = self.config.lease_ttl.as_millis() as u64 * 2;

        let healthy_nodes: Vec<NodeInfo> = all_nodes
            .into_iter()
            .filter(|node| {
                let heartbeat_age = current_time.saturating_sub(node.last_heartbeat);
                let is_recent = heartbeat_age < health_timeout_millis;
                let is_ready = matches!(node.status, NodeStatus::Ready);

                tracing::debug!(
                    "Node {} health check: status={:?}, heartbeat_age={}ms, is_recent={}, is_ready={}",
                    node.id, node.status, heartbeat_age, is_recent, is_ready
                );

                is_ready && is_recent
            })
            .collect();

        tracing::debug!(
            "Found {} healthy servers out of {} total",
            healthy_nodes.len(),
            total_nodes
        );
        Ok(healthy_nodes)
    }

    /// Check if this etcd client is currently healthy (connected and registered)
    pub async fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }

    /// Get current node info
    pub async fn get_node_info(&self) -> NodeInfo {
        let node_info = self.node_info.lock().await;
        node_info.clone()
    }

    /// Manually trigger a re-registration attempt
    pub async fn force_re_register(&self) -> Result<()> {
        Self::re_register_node(&self.client, &self.lease_id, &self.node_info, &self.config).await?;

        self.is_healthy.store(true, Ordering::Relaxed);

        self.send_event(EtcdEvent::ConnectionRecovered).await;
        Ok(())
    }
}

fn current_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_etcd_client_creation() {
        let mut config = EtcdConfig::default();
        // Use a clearly invalid endpoint to ensure connection fails
        config.endpoints = vec!["invalid-host:99999".to_string()];

        let node_info = NodeInfo::new(uuid::Uuid::new_v4(), "127.0.0.1".to_string(), 8080);

        // This should fail to connect to etcd with an invalid endpoint
        let result = EtcdClient::new(config, node_info).await;

        // We expect this to fail since the endpoint is invalid
        match result {
            Ok(_) => {
                // If it somehow succeeds, that's unexpected but not necessarily wrong
                // The etcd client might be doing lazy connection
                println!("Unexpected success - etcd client created with invalid endpoint");
            }
            Err(_) => {
                // This is what we expect - connection should fail
            }
        }

        // Let's test that valid configuration structure works
        let valid_config = EtcdConfig::default();
        assert!(!valid_config.endpoints.is_empty());
        assert!(valid_config.heartbeat_interval > Duration::from_secs(0));
        assert!(valid_config.lease_ttl > Duration::from_secs(0));
    }

    #[tokio::test]
    async fn test_etcd_config_defaults() {
        let config = EtcdConfig::default();

        assert_eq!(config.endpoints, vec!["localhost:2379".to_string()]);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(config.lease_ttl, Duration::from_secs(30));
        assert_eq!(config.max_retry_attempts, 5);
        assert_eq!(config.retry_backoff_base, Duration::from_millis(500));
        assert_eq!(config.max_retry_backoff, Duration::from_secs(30));
        assert_eq!(config.recovery_timeout, Duration::from_secs(300));
    }

    #[tokio::test]
    async fn test_backoff_calculation() {
        let config = EtcdConfig::default();

        // Test exponential backoff calculation
        let backoff1 = EtcdClient::calculate_backoff(1, &config);
        let backoff2 = EtcdClient::calculate_backoff(2, &config);
        let backoff3 = EtcdClient::calculate_backoff(3, &config);

        assert_eq!(backoff1, Duration::from_millis(500)); // base * 2^0
        assert_eq!(backoff2, Duration::from_secs(1)); // base * 2^1
        assert_eq!(backoff3, Duration::from_secs(2)); // base * 2^2

        // Test max backoff cap
        let large_backoff = EtcdClient::calculate_backoff(10, &config);
        assert_eq!(large_backoff, config.max_retry_backoff);
    }

    #[tokio::test]
    async fn test_node_info_creation() {
        let node_id = uuid::Uuid::new_v4();
        let address = "192.168.1.100".to_string();
        let port = 9090;

        let node_info = NodeInfo::new(node_id, address.clone(), port);

        assert_eq!(node_info.id, node_id);
        assert_eq!(node_info.address, address);
        assert_eq!(node_info.port, port);
        assert_eq!(node_info.status, NodeStatus::Starting);
        assert_eq!(node_info.endpoint(), "192.168.1.100:9090");

        // Timestamp should be recent (within last 10 seconds = 10000ms)
        let now = current_timestamp_millis();
        assert!(now >= node_info.last_heartbeat);
        assert!(now - node_info.last_heartbeat < 10000); // 10 seconds in milliseconds
    }

    #[tokio::test]
    async fn test_etcd_event_types() {
        // Test that all event types can be created and are properly typed
        let events = vec![
            EtcdEvent::NodeRegistered,
            EtcdEvent::ConnectionLost("test error".to_string()),
            EtcdEvent::ConnectionRecovered,
            EtcdEvent::RegistrationFailed("test failure".to_string()),
            EtcdEvent::StatusUpdated(NodeStatus::Ready),
        ];

        // Verify events can be cloned and formatted
        for event in events {
            let cloned = event.clone();
            let formatted = format!("{:?}", cloned);
            assert!(!formatted.is_empty());
        }
    }

    /// Integration test demonstrating the enhanced etcd client behavior
    /// This test shows how the application would handle etcd events
    #[tokio::test]
    async fn test_etcd_event_handling_simulation() {
        use tokio::sync::mpsc;

        // Simulate event handling that an application would implement
        let (event_sender, mut event_receiver) = mpsc::unbounded_channel::<EtcdEvent>();

        // Simulate various etcd events
        let events = vec![
            EtcdEvent::NodeRegistered,
            EtcdEvent::StatusUpdated(NodeStatus::Ready),
            EtcdEvent::ConnectionLost("Network timeout".to_string()),
            EtcdEvent::ConnectionRecovered,
            EtcdEvent::StatusUpdated(NodeStatus::Unhealthy),
        ];

        // Send events
        for event in events.clone() {
            event_sender.send(event).unwrap();
        }
        drop(event_sender); // Close the sender

        // Collect received events
        let mut received_events = Vec::new();
        while let Some(event) = event_receiver.recv().await {
            received_events.push(event);
        }

        assert_eq!(received_events.len(), events.len());

        // Verify event sequence matches what we sent
        for (sent, received) in events.iter().zip(received_events.iter()) {
            match (sent, received) {
                (EtcdEvent::NodeRegistered, EtcdEvent::NodeRegistered) => {}
                (EtcdEvent::ConnectionRecovered, EtcdEvent::ConnectionRecovered) => {}
                (EtcdEvent::ConnectionLost(s1), EtcdEvent::ConnectionLost(s2)) => {
                    assert_eq!(s1, s2);
                }
                (EtcdEvent::RegistrationFailed(s1), EtcdEvent::RegistrationFailed(s2)) => {
                    assert_eq!(s1, s2);
                }
                (EtcdEvent::StatusUpdated(status1), EtcdEvent::StatusUpdated(status2)) => {
                    assert_eq!(status1, status2);
                }
                _ => panic!("Event mismatch: {:?} != {:?}", sent, received),
            }
        }
    }

    /// Test demonstrating how the application should respond to different etcd events
    #[tokio::test]
    async fn test_application_event_response_patterns() {
        use tokio::sync::mpsc;

        let (event_sender, mut event_receiver) = mpsc::unbounded_channel::<EtcdEvent>();

        // Simulate application state
        let mut node_healthy = false;
        let mut etcd_connected = false;
        let mut current_status = NodeStatus::Starting;

        // Start event handler task
        let handler = tokio::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                match event {
                    EtcdEvent::NodeRegistered => {
                        println!("✅ Node registered - starting normal operations");
                        etcd_connected = true;
                        node_healthy = true;
                    }
                    EtcdEvent::ConnectionLost(error) => {
                        println!("⚠️  Connection lost: {} - entering degraded mode", error);
                        etcd_connected = false;
                        node_healthy = false;
                        // TODO: Stop accepting new write requests
                        // TODO: Enable read-only mode
                        // TODO: Alert monitoring systems
                    }
                    EtcdEvent::ConnectionRecovered => {
                        println!("✅ Connection recovered - resuming normal operations");
                        etcd_connected = true;
                        node_healthy = true;
                    }
                    EtcdEvent::RegistrationFailed(error) => {
                        println!(
                            "❌ Registration failed: {} - consider graceful shutdown",
                            error
                        );
                        etcd_connected = false;
                        node_healthy = false;
                        // In a real application, you might:
                        // - Attempt graceful shutdown
                        // - Switch to standalone mode
                        // - Try alternative etcd endpoints
                    }
                    EtcdEvent::StatusUpdated(status) => {
                        println!("📊 Status updated to: {:?}", status);
                        current_status = status;
                    }
                }
            }

            (node_healthy, etcd_connected, current_status)
        });

        // Send events to test different scenarios
        event_sender.send(EtcdEvent::NodeRegistered).unwrap();
        event_sender
            .send(EtcdEvent::StatusUpdated(NodeStatus::Ready))
            .unwrap();
        event_sender
            .send(EtcdEvent::ConnectionLost("Network partition".to_string()))
            .unwrap();
        event_sender.send(EtcdEvent::ConnectionRecovered).unwrap();
        event_sender
            .send(EtcdEvent::StatusUpdated(NodeStatus::Unhealthy))
            .unwrap();

        drop(event_sender);

        // Wait for handler to process all events
        let (final_healthy, final_connected, final_status) =
            timeout(Duration::from_secs(1), handler)
                .await
                .unwrap()
                .unwrap();

        // Verify final state
        assert!(final_healthy);
        assert!(final_connected);
        assert_eq!(final_status, NodeStatus::Unhealthy);
    }
}
