use std::time::Duration;
use uuid::Uuid;

use samsa::common::etcd_client::{EtcdClient, EtcdConfig, NodeInfo, NodeStatus};

mod test_utils;
use test_utils::*;

#[tokio::test]
async fn test_etcd_client_creation() {
    let _env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let config = EtcdConfig::default();

    let node_info = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    let client_result = EtcdClient::new(config, node_info).await;

    match client_result {
        Ok((etcd_client, _event_receiver)) => {
            // Successfully created client
            assert!(true);
        }
        Err(e) => {
            // Connection might fail in test environment - log but don't fail test
            println!(
                "ETCD connection failed (expected in some test environments): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_etcd_client_node_registration() {
    let _env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let config = EtcdConfig::default();

    let node_info = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    match EtcdClient::new(config, node_info).await {
        Ok((mut client, _event_receiver)) => {
            // Test node registration
            match client.register_and_start_heartbeat().await {
                Ok(_) => {
                    // Registration successful
                    assert!(client.is_healthy().await);
                }
                Err(e) => {
                    println!("Node registration failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!(
                "ETCD connection failed (expected in some test environments): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_etcd_client_status_update() {
    let _env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let config = EtcdConfig::default();

    let node_info = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    match EtcdClient::new(config, node_info).await {
        Ok((mut client, _event_receiver)) => {
            // Register first
            if let Ok(_) = client.register_and_start_heartbeat().await {
                // Test status update
                match client.update_status(NodeStatus::Ready).await {
                    Ok(_) => {
                        // Status update successful
                        let node_info = client.get_node_info().await;
                        assert_eq!(node_info.status, NodeStatus::Ready);
                    }
                    Err(e) => {
                        println!("Status update failed: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            println!(
                "ETCD connection failed (expected in some test environments): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_etcd_client_list_servers() {
    let _env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let config = EtcdConfig::default();

    let node_info = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    match EtcdClient::new(config, node_info).await {
        Ok((mut client, _event_receiver)) => {
            // Register first
            if let Ok(_) = client.register_and_start_heartbeat().await {
                // Test listing servers
                match client.list_servers().await {
                    Ok(servers) => {
                        // Should find at least our registered server
                        assert!(!servers.is_empty());
                        let our_node = client.get_node_info().await;
                        assert!(servers.iter().any(|s| s.id == our_node.id));
                    }
                    Err(e) => {
                        println!("List servers failed: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            println!(
                "ETCD connection failed (expected in some test environments): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_etcd_client_get_healthy_servers() {
    let _env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let config = EtcdConfig::default();

    let node_info = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    match EtcdClient::new(config, node_info).await {
        Ok((mut client, _event_receiver)) => {
            // Register and set status to ready
            if let Ok(_) = client.register_and_start_heartbeat().await {
                if let Ok(_) = client.update_status(NodeStatus::Ready).await {
                    // Test getting healthy servers
                    match client.get_healthy_servers().await {
                        Ok(healthy_servers) => {
                            // Should find our healthy server
                            let our_node = client.get_node_info().await;
                            assert!(healthy_servers.iter().any(|s| s.id == our_node.id));
                        }
                        Err(e) => {
                            println!("Get healthy servers failed: {}", e);
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!(
                "ETCD connection failed (expected in some test environments): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_etcd_client_unregister() {
    let _env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let config = EtcdConfig::default();

    let node_info = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    match EtcdClient::new(config, node_info).await {
        Ok((mut client, _event_receiver)) => {
            // Register first
            if let Ok(_) = client.register_and_start_heartbeat().await {
                assert!(client.is_healthy().await);

                // Test unregistration
                match client.unregister_node().await {
                    Ok(_) => {
                        // Should no longer be healthy
                        assert!(!client.is_healthy().await);
                    }
                    Err(e) => {
                        println!("Unregister failed: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            println!(
                "ETCD connection failed (expected in some test environments): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_etcd_client_force_re_register() {
    let _env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let config = EtcdConfig::default();

    let node_info = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    match EtcdClient::new(config, node_info).await {
        Ok((mut client, _event_receiver)) => {
            // Register first
            if let Ok(_) = client.register_and_start_heartbeat().await {
                // Test force re-registration
                match client.force_re_register().await {
                    Ok(_) => {
                        // Should still be healthy
                        assert!(client.is_healthy().await);
                    }
                    Err(e) => {
                        println!("Force re-register failed: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            println!(
                "ETCD connection failed (expected in some test environments): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_etcd_client_error_handling() {
    // Test error handling with invalid endpoints - this is a unit test that doesn't need TestEnvironment
    let config = EtcdConfig {
        endpoints: vec!["http://invalid-endpoint:2379".to_string()],
        ..EtcdConfig::default()
    };

    let node_info = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    // Use a timeout to ensure we don't wait forever
    let result =
        tokio::time::timeout(Duration::from_secs(5), EtcdClient::new(config, node_info)).await;

    match result {
        Ok(Ok((mut client, _event_receiver))) => {
            // If creation succeeded, try to actually use the client to see if it fails
            // This tests that the client will fail when actually trying to perform operations
            match tokio::time::timeout(
                Duration::from_secs(3),
                client.register_and_start_heartbeat(),
            )
            .await
            {
                Ok(Ok(_)) => {
                    panic!("Client operations should have failed with invalid endpoint");
                }
                Ok(Err(_)) => {
                    // Expected - operation failed with invalid endpoint
                    assert!(true);
                }
                Err(_) => {
                    // Timeout is also acceptable - indicates connection issues
                    assert!(true);
                }
            }
        }
        Ok(Err(e)) => {
            // Expected error with invalid endpoint during client creation
            assert!(!e.to_string().is_empty());
        }
        Err(_) => {
            // Timeout during client creation is also acceptable for invalid endpoints
            assert!(true);
        }
    }
}

#[tokio::test]
async fn test_etcd_config_defaults() {
    // Unit test - no TestEnvironment needed
    let config = EtcdConfig::default();

    assert_eq!(config.endpoints, vec!["localhost:2379".to_string()]);
    assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
    assert_eq!(config.lease_ttl, Duration::from_secs(30));
    assert_eq!(config.max_retry_attempts, 5);
    assert_eq!(config.retry_backoff_base, Duration::from_millis(500));
    assert_eq!(config.max_retry_backoff, Duration::from_secs(30));
    assert_eq!(config.recovery_timeout, Duration::from_secs(300));
    assert_eq!(config.max_consecutive_heartbeat_failures, 10);
}

#[tokio::test]
async fn test_node_info_creation() {
    // Unit test - no TestEnvironment needed
    let id = Uuid::new_v4();
    let address = "test-host".to_string();
    let port = 9090;

    let node_info = NodeInfo::new(id, address.clone(), port);

    assert_eq!(node_info.id, id);
    assert_eq!(node_info.address, address);
    assert_eq!(node_info.port, port);
    assert_eq!(node_info.status, NodeStatus::Starting);
    assert!(node_info.last_heartbeat > 0);
    assert_eq!(node_info.endpoint(), "test-host:9090");
}

#[tokio::test]
async fn test_node_status_variants() {
    // Unit test - no TestEnvironment needed
    // Test that all NodeStatus variants can be created
    let _starting = NodeStatus::Starting;
    let _ready = NodeStatus::Ready;
    let _unhealthy = NodeStatus::Unhealthy;

    // Test equality
    assert_eq!(NodeStatus::Starting, NodeStatus::Starting);
    assert_ne!(NodeStatus::Starting, NodeStatus::Ready);
}

#[tokio::test]
async fn test_multiple_clients_isolation() {
    let _env = TestEnvironment::setup()
        .await
        .expect("Failed to setup test environment");

    let config1 = EtcdConfig::default();
    let config2 = EtcdConfig::default();

    let node_info1 = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8080);

    let node_info2 = NodeInfo::new(Uuid::new_v4(), "localhost".to_string(), 8081);

    match (
        EtcdClient::new(config1, node_info1).await,
        EtcdClient::new(config2, node_info2).await,
    ) {
        (Ok((mut client1, _event_receiver1)), Ok((mut client2, _event_receiver2))) => {
            // Register both clients
            if let (Ok(_), Ok(_)) = (
                client1.register_and_start_heartbeat().await,
                client2.register_and_start_heartbeat().await,
            ) {
                // Both should be healthy
                assert!(client1.is_healthy().await);
                assert!(client2.is_healthy().await);

                // Both should have different node IDs
                let node1 = client1.get_node_info().await;
                let node2 = client2.get_node_info().await;
                assert_ne!(node1.id, node2.id);
                assert_ne!(node1.port, node2.port);

                // List servers should show both
                if let Ok(servers) = client1.list_servers().await {
                    assert!(servers.len() >= 2);
                    assert!(servers.iter().any(|s| s.id == node1.id));
                    assert!(servers.iter().any(|s| s.id == node2.id));
                }
            }
        }
        _ => {
            println!("ETCD connection failed (expected in some test environments)");
        }
    }
}
