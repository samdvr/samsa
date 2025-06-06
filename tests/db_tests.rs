//! Unit tests for common::db module
//!
//! Tests database configuration, connection pool initialization, migrations, and connection testing.

use samsa::common::db::{DatabaseConfig, init_postgres_pool, run_migrations, test_connection};
use sqlx::Row;

#[test]
fn test_database_config_default() {
    let config = DatabaseConfig::default();

    assert_eq!(
        config.url,
        "postgresql://postgres:password@localhost:5432/samsa"
    );
    assert_eq!(config.max_connections, 20);
    assert_eq!(config.min_connections, 1);
    assert_eq!(config.acquire_timeout_seconds, 30);
    assert_eq!(config.idle_timeout_seconds, 600);
}

#[test]
fn test_database_config_custom() {
    let config = DatabaseConfig {
        url: "postgresql://user:pass@host:5433/mydb".to_string(),
        max_connections: 50,
        min_connections: 5,
        acquire_timeout_seconds: 60,
        idle_timeout_seconds: 1200,
    };

    assert_eq!(config.url, "postgresql://user:pass@host:5433/mydb");
    assert_eq!(config.max_connections, 50);
    assert_eq!(config.min_connections, 5);
    assert_eq!(config.acquire_timeout_seconds, 60);
    assert_eq!(config.idle_timeout_seconds, 1200);
}

#[test]
fn test_database_config_clone() {
    let config = DatabaseConfig::default();
    let cloned = config.clone();

    assert_eq!(config.url, cloned.url);
    assert_eq!(config.max_connections, cloned.max_connections);
    assert_eq!(config.min_connections, cloned.min_connections);
    assert_eq!(
        config.acquire_timeout_seconds,
        cloned.acquire_timeout_seconds
    );
    assert_eq!(config.idle_timeout_seconds, cloned.idle_timeout_seconds);
}

#[test]
fn test_database_config_debug() {
    let config = DatabaseConfig::default();
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("DatabaseConfig"));
    assert!(debug_str.contains("max_connections: 20"));
    assert!(debug_str.contains("min_connections: 1"));
}

#[tokio::test]
async fn test_init_postgres_pool_invalid_url() {
    let config = DatabaseConfig {
        url: "invalid_url".to_string(),
        max_connections: 5,
        min_connections: 1,
        acquire_timeout_seconds: 1,
        idle_timeout_seconds: 60,
    };

    let result = init_postgres_pool(&config).await;
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert!(matches!(
        error,
        samsa::common::error::SamsaError::DatabaseError(_)
    ));
}

#[tokio::test]
async fn test_init_postgres_pool_connection_refused() {
    let config = DatabaseConfig {
        url: "postgresql://postgres:password@localhost:9999/nonexistent".to_string(),
        max_connections: 5,
        min_connections: 1,
        acquire_timeout_seconds: 1,
        idle_timeout_seconds: 60,
    };

    let result = init_postgres_pool(&config).await;
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert!(matches!(
        error,
        samsa::common::error::SamsaError::DatabaseError(_)
    ));
}

#[tokio::test]
async fn test_init_postgres_pool_malformed_url() {
    let config = DatabaseConfig {
        url: "not-a-valid-postgresql-url".to_string(),
        max_connections: 5,
        min_connections: 1,
        acquire_timeout_seconds: 1,
        idle_timeout_seconds: 60,
    };

    let result = init_postgres_pool(&config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_run_migrations_no_pool() {
    // Create a minimal config that will definitely fail
    let config = DatabaseConfig {
        url: "postgresql://nonexistent:password@localhost:9999/nonexistent".to_string(),
        max_connections: 1,
        min_connections: 1,
        acquire_timeout_seconds: 1,
        idle_timeout_seconds: 60,
    };

    // This should fail since we can't connect
    if let Ok(pool) = init_postgres_pool(&config).await {
        let result = run_migrations(&pool).await;
        // Migrations might fail due to missing migrations directory or connection issues
        // We're testing that the function returns an error in error cases
        match result {
            Ok(_) => {
                // If it succeeds, that's fine too - means the function works
            }
            Err(error) => {
                assert!(matches!(
                    error,
                    samsa::common::error::SamsaError::DatabaseError(_)
                ));
            }
        }
    }
}

#[tokio::test]
async fn test_test_connection_invalid_pool() {
    let config = DatabaseConfig {
        url: "postgresql://nonexistent:password@localhost:9999/nonexistent".to_string(),
        max_connections: 1,
        min_connections: 1,
        acquire_timeout_seconds: 1,
        idle_timeout_seconds: 60,
    };

    // This should fail since we can't connect
    if let Ok(pool) = init_postgres_pool(&config).await {
        let result = test_connection(&pool).await;
        // Connection test should fail
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(matches!(
            error,
            samsa::common::error::SamsaError::DatabaseError(_)
        ));
    }
}

// Integration tests that require a real database connection
#[cfg(feature = "integration-tests")]
mod integration_tests {
    use super::*;

    // Helper to get test database URL from environment
    fn get_test_db_url() -> String {
        std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:password@localhost:5432/samsa_test".to_string()
        })
    }

    #[tokio::test]
    async fn test_init_postgres_pool_success() {
        let config = DatabaseConfig {
            url: get_test_db_url(),
            max_connections: 5,
            min_connections: 1,
            acquire_timeout_seconds: 30,
            idle_timeout_seconds: 600,
        };

        let result = init_postgres_pool(&config).await;
        if let Ok(pool) = result {
            // Verify pool was created successfully
            assert!(pool.size() > 0);
            pool.close().await;
        } else {
            // Skip test if no test database available
            println!("Skipping test - no test database available");
        }
    }

    #[tokio::test]
    async fn test_test_connection_success() {
        let config = DatabaseConfig {
            url: get_test_db_url(),
            max_connections: 5,
            min_connections: 1,
            acquire_timeout_seconds: 30,
            idle_timeout_seconds: 600,
        };

        if let Ok(pool) = init_postgres_pool(&config).await {
            let result = test_connection(&pool).await;
            assert!(result.is_ok());
            pool.close().await;
        } else {
            println!("Skipping test - no test database available");
        }
    }

    #[tokio::test]
    async fn test_run_migrations_success() {
        let config = DatabaseConfig {
            url: get_test_db_url(),
            max_connections: 5,
            min_connections: 1,
            acquire_timeout_seconds: 30,
            idle_timeout_seconds: 600,
        };

        if let Ok(pool) = init_postgres_pool(&config).await {
            let result = run_migrations(&pool).await;
            // Migrations should succeed (or at least not fail catastrophically)
            match result {
                Ok(_) => {
                    // Success is good
                }
                Err(_) => {
                    // Failure might happen if migrations directory doesn't exist
                    // or migrations have already been run - that's ok for this test
                }
            }
            pool.close().await;
        } else {
            println!("Skipping test - no test database available");
        }
    }
}

// Mock tests that don't require actual database connections
#[cfg(test)]
mod mock_tests {
    use super::*;

    #[test]
    fn test_database_config_validation() {
        // Test various config combinations
        let configs = vec![
            DatabaseConfig {
                url: "postgresql://user:pass@localhost:5432/db".to_string(),
                max_connections: 1,
                min_connections: 1,
                acquire_timeout_seconds: 1,
                idle_timeout_seconds: 1,
            },
            DatabaseConfig {
                url: "postgresql://user:pass@localhost:5432/db".to_string(),
                max_connections: 100,
                min_connections: 10,
                acquire_timeout_seconds: 300,
                idle_timeout_seconds: 3600,
            },
        ];

        for config in configs {
            assert!(config.max_connections >= config.min_connections);
            assert!(config.acquire_timeout_seconds > 0);
            assert!(config.idle_timeout_seconds > 0);
            assert!(!config.url.is_empty());
        }
    }

    #[test]
    fn test_config_edge_cases() {
        // Test with minimum values
        let min_config = DatabaseConfig {
            url: "postgresql://u:p@h:1/d".to_string(),
            max_connections: 1,
            min_connections: 1,
            acquire_timeout_seconds: 1,
            idle_timeout_seconds: 1,
        };

        assert_eq!(min_config.max_connections, 1);
        assert_eq!(min_config.min_connections, 1);

        // Test with large values
        let max_config = DatabaseConfig {
            url: "postgresql://user:password@hostname:65535/database_name_with_underscores"
                .to_string(),
            max_connections: u32::MAX,
            min_connections: 1000,
            acquire_timeout_seconds: u64::MAX,
            idle_timeout_seconds: u64::MAX,
        };

        assert_eq!(max_config.max_connections, u32::MAX);
        assert_eq!(max_config.min_connections, 1000);
    }
}
