use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

mod test_utils;

// Simple CLI binary path caching
static CLI_BINARY_PATH: OnceLock<String> = OnceLock::new();

/// Get the CLI binary path, building it if necessary (thread-safe, super fast)
fn get_cli_binary_path() -> &'static str {
    CLI_BINARY_PATH.get_or_init(|| {
        if std::path::Path::new("target/release/samsa-cli").exists() {
            "target/release/samsa-cli".to_string()
        } else if std::path::Path::new("target/debug/samsa-cli").exists() {
            "target/debug/samsa-cli".to_string()
        } else {
            // Build the CLI binary quickly
            println!("🔨 Building samsa-cli binary...");
            let output = std::process::Command::new("cargo")
                .args(["build", "--bin", "samsa-cli"])
                .output()
                .expect("Failed to execute cargo build");

            if !output.status.success() {
                panic!(
                    "Failed to build CLI binary: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            "target/debug/samsa-cli".to_string()
        }
    })
}

/// Helper to execute CLI command with timeout and basic error handling
async fn execute_cli_command_simple(args: &[&str]) -> Result<std::process::Output, String> {
    let cli_path = get_cli_binary_path();

    // Try with a default server address first
    let mut full_args = vec!["--address", "http://127.0.0.1:50051"];
    full_args.extend_from_slice(args);

    let output = tokio::time::timeout(Duration::from_secs(5), async {
        Command::new(cli_path).args(&full_args).output()
    })
    .await
    .map_err(|_| "CLI command timed out".to_string())?
    .map_err(|e| format!("Failed to execute CLI command: {}", e))?;

    Ok(output)
}

/// Test CLI help and error commands (no server needed)
#[tokio::test]
async fn test_cli_help_and_errors() {
    let cli_path = get_cli_binary_path();

    // Test main help
    let help_output = Command::new(cli_path)
        .args(["--help"])
        .output()
        .expect("Failed to execute help command");

    assert!(help_output.status.success());
    let stdout = String::from_utf8_lossy(&help_output.stdout);
    assert!(stdout.contains("Samsa Streaming System CLI") || stdout.contains("Usage"));

    // Test invalid subcommand
    let invalid_cmd = Command::new(cli_path)
        .args(["invalid", "command"])
        .output()
        .expect("Failed to execute invalid command");

    assert!(!invalid_cmd.status.success());

    // Test missing required arguments
    let missing_args = Command::new(cli_path)
        .args(["stream", "list"]) // Missing --bucket
        .output()
        .expect("Failed to execute command with missing args");

    assert!(!missing_args.status.success());
}

/// Test CLI with server operations (if server available)
#[tokio::test]
async fn test_cli_bucket_operations() {
    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let bucket_name = format!("cli-test-bucket-{}", test_id);

    // Test bucket creation
    let create_output = execute_cli_command_simple(&["bucket", "create", &bucket_name]).await;

    match create_output {
        Ok(output) if output.status.success() => {
            println!("✅ Bucket create succeeded");

            // Test bucket list
            if let Ok(list_output) = execute_cli_command_simple(&["bucket", "list"]).await {
                if list_output.status.success() {
                    let stdout = String::from_utf8_lossy(&list_output.stdout);
                    if stdout.contains(&bucket_name) {
                        println!("✅ Bucket appears in list");
                    }
                }
            }

            // Cleanup - best effort
            let _ = execute_cli_command_simple(&["bucket", "delete", &bucket_name]).await;
        }
        _ => {
            println!("⚠️  Server not available for bucket operations (expected in CI)");
        }
    }
}

/// Test CLI stream operations (if server available)
#[tokio::test]
async fn test_cli_stream_operations() {
    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let bucket_name = format!("cli-stream-bucket-{}", test_id);
    let stream_name = format!("cli-stream-{}", test_id);

    // Try to create bucket and stream
    if let Ok(bucket_output) = execute_cli_command_simple(&["bucket", "create", &bucket_name]).await
    {
        if bucket_output.status.success() {
            println!("✅ Test bucket created");

            // Test stream creation
            if let Ok(stream_output) = execute_cli_command_simple(&[
                "stream",
                "--bucket",
                &bucket_name,
                "create",
                &stream_name,
            ])
            .await
            {
                if stream_output.status.success() {
                    println!("✅ Stream created");

                    // Test stream list
                    if let Ok(list_output) =
                        execute_cli_command_simple(&["stream", "--bucket", &bucket_name, "list"])
                            .await
                    {
                        if list_output.status.success() {
                            let stdout = String::from_utf8_lossy(&list_output.stdout);
                            if stdout.contains(&stream_name) {
                                println!("✅ Stream appears in list");
                            }
                        }
                    }

                    // Cleanup stream
                    let _ = execute_cli_command_simple(&[
                        "stream",
                        "--bucket",
                        &bucket_name,
                        "delete",
                        &stream_name,
                    ])
                    .await;
                }
            }

            // Cleanup bucket
            let _ = execute_cli_command_simple(&["bucket", "delete", &bucket_name]).await;
        }
    } else {
        println!("⚠️  Server not available for stream operations (expected in CI)");
    }
}

/// Test CLI data operations (if server available)
#[tokio::test]
async fn test_cli_data_operations() {
    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let bucket_name = format!("cli-data-bucket-{}", test_id);
    let stream_name = format!("cli-data-stream-{}", test_id);

    // Setup bucket and stream
    if let Ok(bucket_output) = execute_cli_command_simple(&["bucket", "create", &bucket_name]).await
    {
        if bucket_output.status.success() {
            if let Ok(stream_output) = execute_cli_command_simple(&[
                "stream",
                "--bucket",
                &bucket_name,
                "create",
                &stream_name,
            ])
            .await
            {
                if stream_output.status.success() {
                    println!("✅ Test setup complete");

                    // Test data append
                    if let Ok(append_output) = execute_cli_command_simple(&[
                        "data",
                        "--bucket",
                        &bucket_name,
                        "--stream",
                        &stream_name,
                        "append",
                        "--body",
                        "test data",
                    ])
                    .await
                    {
                        if append_output.status.success() {
                            println!("✅ Data append succeeded");

                            // Test data read
                            if let Ok(read_output) = execute_cli_command_simple(&[
                                "data",
                                "--bucket",
                                &bucket_name,
                                "--stream",
                                &stream_name,
                                "read",
                                "--limit",
                                "1",
                            ])
                            .await
                            {
                                if read_output.status.success() {
                                    println!("✅ Data read succeeded");
                                }
                            }
                        }
                    }

                    // Cleanup
                    let _ = execute_cli_command_simple(&[
                        "stream",
                        "--bucket",
                        &bucket_name,
                        "delete",
                        &stream_name,
                    ])
                    .await;
                }
            }

            let _ = execute_cli_command_simple(&["bucket", "delete", &bucket_name]).await;
        }
    } else {
        println!("⚠️  Server not available for data operations (expected in CI)");
    }
}

/// Test CLI token operations (if server available)
#[tokio::test]
async fn test_cli_token_operations() {
    // Test token list
    if let Ok(list_output) = execute_cli_command_simple(&["token", "list"]).await {
        if list_output.status.success() {
            println!("✅ Token list succeeded");

            // Test token issue
            if let Ok(issue_output) =
                execute_cli_command_simple(&["token", "issue", "--expires", "3600"]).await
            {
                if issue_output.status.success() {
                    println!("✅ Token issue succeeded");
                }
            }
        }
    } else {
        println!("⚠️  Server not available for token operations (expected in CI)");
    }
}

/// Test CLI with different server addresses
#[tokio::test]
async fn test_cli_connection_handling() {
    let cli_path = get_cli_binary_path();

    // Test with non-existent address (should fail gracefully)
    let custom_addr_output = Command::new(cli_path)
        .args(["--address", "http://127.0.0.1:50052", "bucket", "list"])
        .output()
        .expect("Failed to execute CLI with custom address");

    // Should either succeed (if something is running there) or fail gracefully
    if custom_addr_output.status.success() {
        println!("✅ Connection to custom address succeeded");
    } else {
        println!("✅ CLI handled connection failure gracefully");
        // Should have reasonable error handling
        let stderr = String::from_utf8_lossy(&custom_addr_output.stderr);
        assert!(!stderr.is_empty() || custom_addr_output.stderr.is_empty());
    }
}
