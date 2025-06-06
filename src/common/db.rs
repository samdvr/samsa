use crate::common::error::{Result, SamsaError};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tracing::{info, warn};

/// Database configuration structure
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_seconds: u64,
    pub idle_timeout_seconds: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgresql://postgres:password@localhost:5432/samsa".to_string(),
            max_connections: 20,
            min_connections: 1,
            acquire_timeout_seconds: 30,
            idle_timeout_seconds: 600,
        }
    }
}

/// Initialize PostgreSQL connection pool
pub async fn init_postgres_pool(config: &DatabaseConfig) -> Result<PgPool> {
    info!("Initializing PostgreSQL connection pool");

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(std::time::Duration::from_secs(
            config.acquire_timeout_seconds,
        ))
        .idle_timeout(std::time::Duration::from_secs(config.idle_timeout_seconds))
        .connect(&config.url)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

    info!("PostgreSQL connection pool initialized successfully");
    Ok(pool)
}

/// Run database migrations
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    info!("Running database migrations");

    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|e| {
            warn!("Failed to run migrations: {}", e);
            SamsaError::DatabaseError(Box::new(e))
        })?;

    info!("Database migrations completed successfully");
    Ok(())
}

/// Test database connection
pub async fn test_connection(pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT 1")
        .execute(pool)
        .await
        .map_err(|e| SamsaError::DatabaseError(Box::new(e)))?;

    info!("Database connection test successful");
    Ok(())
}
