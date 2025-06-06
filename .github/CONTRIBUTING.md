# Contributing to Samsa

Thank you for your interest in contributing to Samsa ! This document provides guidelines and information about how to contribute effectively.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [CI/CD Pipeline](#cicd-pipeline)
- [Security](#security)
- [Documentation](#documentation)

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/samsa.git
   cd samsa
   ```
3. **Add the upstream remote**:
   ```bash
   git remote add upstream https://github.com/your-org/samsa.git
   ```

## Development Setup

### Prerequisites

- **Rust 1.70+**: Install via [rustup](https://rustup.rs/)
- **Protocol Buffers**: Install `protoc` for gRPC compilation
- **etcd**: For running integration tests (can use Docker)

### Local Development

1. **Build the project**:

   ```bash
   cargo build
   ```

2. **Run tests**:

   ```bash
   cargo test
   ```

3. **Start development services**:

   ```bash
   # Start etcd for testing
   docker run -d --name etcd-dev \
     -p 2379:2379 -p 2380:2380 \
     quay.io/coreos/etcd:v3.5.14 \
     /usr/local/bin/etcd \
     --data-dir=/etcd-data \
     --listen-client-urls=http://0.0.0.0:2379 \
     --advertise-client-urls=http://0.0.0.0:2379 \
     --listen-peer-urls=http://0.0.0.0:2380 \
     --initial-advertise-peer-urls=http://0.0.0.0:2380 \
     --initial-cluster=default=http://0.0.0.0:2380 \
     --initial-cluster-token=etcd-cluster \
     --initial-cluster-state=new
   ```

4. **Run integration tests**:
   ```bash
   ./run_consolidated_tests.sh
   ```

## Pull Request Process

1. **Create a feature branch**:

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following our coding standards

3. **Test your changes**:

   ```bash
   # Run unit tests
   cargo test

   # Run formatting
   cargo fmt

   # Run linting
   cargo clippy

   # Run integration tests
   ./run_consolidated_tests.sh
   ```

4. **Commit your changes** using [Conventional Commits](https://www.conventionalcommits.org/):

   ```bash
   git commit -m "feat(storage): add batch compression support"
   git commit -m "fix(cli): handle connection timeout gracefully"
   git commit -m "docs: update API documentation"
   ```

5. **Push to your fork**:

   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request** using our [PR template](.github/PULL_REQUEST_TEMPLATE.md)

### PR Requirements

Your pull request must:

- [ ] Pass all CI checks
- [ ] Include tests for new functionality
- [ ] Update documentation if needed
- [ ] Follow conventional commit format
- [ ] Have a clear description of changes
- [ ] Address any feedback from reviewers

## Coding Standards

### Rust Style

- Follow the official [Rust style guide](https://doc.rust-lang.org/nightly/style-guide/)
- Use `cargo fmt` for consistent formatting
- Run `cargo clippy` and address all warnings
- Add documentation comments for public APIs

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/) with these types:

- `feat`: New features
- `fix`: Bug fixes
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or modifying tests
- `chore`: Maintenance tasks
- `perf`: Performance improvements
- `ci`: CI/CD changes

Examples:

```
feat(storage): implement batched record storage
fix(cli): resolve connection timeout issues
docs(api): add examples for stream operations
test(storage): add integration tests for batch flushing
```

## Testing

### Unit Tests

Run unit tests with:

```bash
cargo test --lib
```

### Integration Tests

Our integration tests require etcd and test the full system:

```bash
./run_consolidated_tests.sh
```

### Test Guidelines

- Write tests for all new functionality
- Include both positive and negative test cases
- Test error conditions and edge cases
- Use descriptive test names
- Add integration tests for new features

### Test Structure

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_feature_success_case() {
        // Arrange
        let storage = create_test_storage().await;

        // Act
        let result = storage.some_operation().await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_feature_error_case() {
        // Test error conditions
    }
}
```

## CI/CD Pipeline

Our CI/CD pipeline runs automatically on all pull requests and includes:

### CI Workflow (`.github/workflows/ci.yml`)

- **Check**: `cargo check` for compilation
- **Format**: `cargo fmt --check` for code formatting
- **Clippy**: `cargo clippy` for linting
- **Test**: Unit and integration tests with etcd
- **Build**: Cross-platform binary builds
- **Docs**: Generate and deploy documentation

### Security Workflow (`.github/workflows/security.yml`)

- **Audit**: `cargo audit` for security vulnerabilities
- **Deny**: `cargo deny` for license and dependency checking
- **CodeQL**: Static analysis for security issues
- **SBOM**: Software Bill of Materials generation

### Coverage Workflow (`.github/workflows/coverage.yml`)

- **Coverage**: Generate code coverage reports
- **Differential**: Compare coverage between branches
- **Upload**: Send coverage to Codecov

### Release Workflow (`.github/workflows/release.yml`)

- **Validation**: Verify version and changelog
- **Build**: Cross-platform release binaries
- **Publish**: GitHub releases and crates.io
- **Changelog**: Automated changelog generation

### Required Checks

All PRs must pass:

- [ ] Compilation check
- [ ] Code formatting
- [ ] Linting (no warnings)
- [ ] All tests pass
- [ ] Security audit
- [ ] Documentation builds

## Security

### Reporting Vulnerabilities

Please report security vulnerabilities privately to the maintainers rather than opening public issues.

### Security Best Practices

- Follow secure coding practices
- Validate all inputs
- Handle errors gracefully
- Avoid exposing sensitive information in logs
- Use secure dependencies

### Security Tools

Our CI runs several security tools:

- `cargo audit`: Check for known vulnerabilities
- `cargo deny`: License and dependency validation
- CodeQL: Static security analysis
- Semgrep: Additional security scanning

## Documentation

### Code Documentation

- Add `///` documentation comments for public APIs
- Include examples in documentation:
  ````rust
  /// Creates a new storage instance
  ///
  /// # Example
  ///
  /// ```rust
  /// let storage = Storage::new(config).await?;
  /// ```
  pub fn new(config: Config) -> Result<Self> {
      // implementation
  }
  ````

### API Documentation

- Document all public APIs
- Include usage examples
- Explain error conditions
- Document configuration options

### User Documentation

- Update README.md for user-facing changes
- Add examples for new features
- Update configuration documentation
- Include troubleshooting information

## Getting Help

- **Issues**: Use GitHub issues for bugs and feature requests
- **Discussions**: Use GitHub discussions for questions
- **Documentation**: Check the README and API docs
- **Code**: Look at examples in the `examples/` directory

## Code Review

### For Authors

- Keep PRs focused and small
- Provide clear descriptions
- Respond to feedback promptly
- Update tests and docs as needed

### For Reviewers

- Be constructive and helpful
- Focus on correctness, performance, and maintainability
- Check test coverage
- Verify documentation updates

## Release Process

Releases are automated via GitHub Actions:

1. **Version Bump**: Update `Cargo.toml` version
2. **Tag**: Create a git tag (e.g., `v0.2.0`)
3. **Push**: Push the tag to trigger release workflow
4. **Automated**: CI builds, tests, and publishes the release

Thank you for contributing to Samsa ! 🚀
