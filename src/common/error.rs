use thiserror::Error;

#[derive(Error, Debug)]
pub enum SamsaError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("ETCD error: {0}")]
    Etcd(#[from] Box<etcd_client::Error>),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("gRPC error: {0}")]
    Grpc(#[from] Box<tonic::Status>),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    DatabaseError(Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, SamsaError>;

impl From<etcd_client::Error> for SamsaError {
    fn from(err: etcd_client::Error) -> Self {
        SamsaError::Etcd(Box::new(err))
    }
}

impl From<tonic::Status> for SamsaError {
    fn from(err: tonic::Status) -> Self {
        SamsaError::Grpc(Box::new(err))
    }
}

impl From<SamsaError> for tonic::Status {
    fn from(err: SamsaError) -> Self {
        match err {
            SamsaError::NotFound(msg) => tonic::Status::not_found(msg),
            SamsaError::AlreadyExists(msg) => tonic::Status::already_exists(msg),
            SamsaError::Validation(msg) => tonic::Status::invalid_argument(msg),
            SamsaError::Config(msg) => tonic::Status::failed_precondition(msg),
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}
