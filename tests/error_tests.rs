//! Comprehensive unit tests for the error module
//!
//! Tests all error variants, conversions, and trait implementations

use samsa::common::error::{Result, SamsaError};
use std::io;
use tonic::Status;

#[test]
fn test_config_error() {
    let error = SamsaError::Config("Invalid configuration".to_string());
    assert_eq!(
        error.to_string(),
        "Configuration error: Invalid configuration"
    );
}

#[test]
fn test_storage_error() {
    let error = SamsaError::Storage("Storage failure".to_string());
    assert_eq!(error.to_string(), "Storage error: Storage failure");
}

#[test]
fn test_network_error() {
    let error = SamsaError::Network("Connection failed".to_string());
    assert_eq!(error.to_string(), "Network error: Connection failed");
}

#[test]
fn test_validation_error() {
    let error = SamsaError::Validation("Invalid input".to_string());
    assert_eq!(error.to_string(), "Validation error: Invalid input");
}

#[test]
fn test_not_found_error() {
    let error = SamsaError::NotFound("Resource not found".to_string());
    assert_eq!(error.to_string(), "Not found: Resource not found");
}

#[test]
fn test_already_exists_error() {
    let error = SamsaError::AlreadyExists("Resource exists".to_string());
    assert_eq!(error.to_string(), "Already exists: Resource exists");
}

#[test]
fn test_internal_error() {
    let error = SamsaError::Internal("Internal server error".to_string());
    assert_eq!(error.to_string(), "Internal error: Internal server error");
}

#[test]
fn test_io_error_conversion() {
    let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
    let samsa_error: SamsaError = io_error.into();

    match samsa_error {
        SamsaError::Io(_) => {} // Expected
        _ => panic!("Expected IO error variant"),
    }
}

#[test]
fn test_serde_json_error_conversion() {
    let json_str = "{ invalid json";
    let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
    let samsa_error: SamsaError = json_error.into();

    match samsa_error {
        SamsaError::Serialization(_) => {} // Expected
        _ => panic!("Expected Serialization error variant"),
    }
}

#[test]
fn test_tonic_status_conversion_from_samsaerror() {
    // Test NotFound -> not_found
    let samsa_error = SamsaError::NotFound("Test not found".to_string());
    let status: Status = samsa_error.into();
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(status.message(), "Test not found");

    // Test AlreadyExists -> already_exists
    let samsa_error = SamsaError::AlreadyExists("Test exists".to_string());
    let status: Status = samsa_error.into();
    assert_eq!(status.code(), tonic::Code::AlreadyExists);
    assert_eq!(status.message(), "Test exists");

    // Test Validation -> invalid_argument
    let samsa_error = SamsaError::Validation("Invalid data".to_string());
    let status: Status = samsa_error.into();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(status.message(), "Invalid data");

    // Test Config -> failed_precondition
    let samsa_error = SamsaError::Config("Bad config".to_string());
    let status: Status = samsa_error.into();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), "Bad config");

    // Test other errors -> internal
    let samsa_error = SamsaError::Storage("Storage issue".to_string());
    let status: Status = samsa_error.into();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().contains("Storage error: Storage issue"));
}

#[test]
fn test_tonic_status_conversion_to_samsaerror() {
    let status = Status::not_found("Resource missing");
    let samsa_error: SamsaError = status.into();

    match samsa_error {
        SamsaError::Grpc(_) => {} // Expected
        _ => panic!("Expected Grpc error variant"),
    }
}

#[test]
fn test_etcd_error_conversion() {
    // Create a mock etcd error by using the constructor that creates an etcd error
    let etcd_error = etcd_client::Error::InvalidArgs("Invalid arguments".to_string());
    let samsa_error: SamsaError = etcd_error.into();

    match samsa_error {
        SamsaError::Etcd(_) => {} // Expected
        _ => panic!("Expected Etcd error variant"),
    }
}

#[test]
fn test_object_store_error_conversion() {
    let os_error = object_store::Error::Generic {
        store: "test",
        source: Box::new(io::Error::other("test error")),
    };
    let samsa_error: SamsaError = os_error.into();

    match samsa_error {
        SamsaError::ObjectStore(_) => {} // Expected
        _ => panic!("Expected ObjectStore error variant"),
    }
}

#[test]
fn test_error_debug() {
    let error = SamsaError::Config("test".to_string());
    let debug_str = format!("{:?}", error);
    assert!(debug_str.contains("Config"));
    assert!(debug_str.contains("test"));
}

#[test]
fn test_result_type_alias() {
    fn returns_ok() -> Result<i32> {
        Ok(42)
    }

    fn returns_err() -> Result<i32> {
        Err(SamsaError::Internal("test error".to_string()))
    }

    assert_eq!(returns_ok().unwrap(), 42);
    assert!(returns_err().is_err());
}

#[test]
fn test_error_chain() {
    // Test that we can create a chain of errors
    let io_error = io::Error::new(io::ErrorKind::PermissionDenied, "Access denied");
    let samsa_error: SamsaError = io_error.into();

    // Convert to tonic status
    let status: Status = samsa_error.into();

    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().contains("IO error"));
}

#[test]
fn test_all_error_variants_display() {
    let errors = vec![
        SamsaError::Config("config".to_string()),
        SamsaError::Storage("storage".to_string()),
        SamsaError::Network("network".to_string()),
        SamsaError::Validation("validation".to_string()),
        SamsaError::NotFound("not found".to_string()),
        SamsaError::AlreadyExists("exists".to_string()),
        SamsaError::Internal("internal".to_string()),
    ];

    for error in errors {
        let display_str = error.to_string();
        assert!(!display_str.is_empty());
        assert!(!display_str.contains("Error")); // Should not contain the word "Error" in the message part
    }
}
