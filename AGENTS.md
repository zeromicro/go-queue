# AGENTS.md - Go-Queue Project

This file provides guidance to AI agents working on the Go-Queue project.

## Project Overview

Go-Queue is a comprehensive queue library for Go that supports multiple queue backends. It provides:

- **Language**: Go (Golang) 1.20+
- **Type**: Queue library/package
- **Purpose**: Unified interface for various queue systems
- **Features**: Multiple queue implementations with consistent API

## Key Configuration Files

- `go.mod` - Go module definition and dependencies
- `go.sum` - Dependency checksums
- `readme.md` - Project documentation

## Supported Queue Backends

The library supports multiple queue systems:

1. **Beanstalkd** (`dq/` directory)
2. **Kafka** (`kq/` directory)
3. **NATS** (`natsmq/` and `natsq/` directories)
4. **NATS Streaming** (`stanq/` directory)
5. **RabbitMQ** (`rabbitmq/` directory)

## Build and Test Commands

### Installation
```bash
# Install the package
go get github.com/zeromicro/go-queue

# Install dependencies
go mod tidy
```

### Development
```bash
# Build the project
go build ./...

# Build specific queue implementation
go build ./dq/...      # Beanstalkd
go build ./kq/...      # Kafka
go build ./natsmq/...  # NATS
go build ./rabbitmq/... # RabbitMQ
```

### Testing
```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run tests for specific queue
go test ./dq/...
go test ./kq/...
```

### Code Quality
```bash
# Format code
gofmt -w .

# Check for formatting issues
gofmt -d .

# Run linter (if available)
golangci-lint run
```

## Project Structure

```
dq/          # Beanstalkd queue implementation
kq/          # Kafka queue implementation
natsmq/      # NATS queue implementation
natsq/       # NATS queue implementation (alternative)
stanq/       # NATS Streaming queue implementation
rabbitmq/    # RabbitMQ queue implementation
example/     # Usage examples
go.mod       # Go module definition
go.sum       # Dependency checksums
readme.md    # Project documentation
```

## Code Style Guidelines

- **Go Standards**: Follow official Go code review comments
- **Formatting**: Use `gofmt` for consistent formatting
- **Naming**: Use camelCase for variables, PascalCase for exported types
- **Error Handling**: Explicit error handling (no panic for expected errors)
- **Documentation**: Add godoc comments for exported functions/types
- **Consistency**: Maintain consistent API across different queue backends

## Testing Instructions

- **Unit Tests**: Each queue implementation has its own tests
- **Integration Tests**: May require running queue servers
- **Mock Testing**: Use interfaces for mocking in tests
- **Coverage**: Aim for high test coverage of all queue operations

## Queue Implementation Details

### Common Interface
All queue implementations should provide:
- `Producer` interface for sending messages
- `Consumer` interface for receiving messages
- Consistent error handling
- Configuration options

### Backend-Specific Features
Each queue backend may have:
- Unique configuration options
- Specific performance characteristics
- Different reliability guarantees
- Backend-specific features

## Security Considerations

- **Connection Security**: Use TLS for queue connections when possible
- **Authentication**: Secure queue server authentication
- **Input Validation**: Validate queue names and message content
- **Error Handling**: Don't expose sensitive information in errors
- **Resource Limits**: Prevent resource exhaustion attacks

## Performance Considerations

- **Batch Processing**: Support for batch message operations
- **Connection Pooling**: Efficient connection management
- **Memory Usage**: Minimize allocations during message processing
- **Concurrency**: Thread-safe operations where appropriate
- **Benchmarking**: Consider adding performance benchmarks

## Usage Examples

```go
// Example usage pattern
import "github.com/zeromicro/go-queue/dq" // or other queue

// Create producer
producer := dq.NewProducer(config)

// Send message
err := producer.Send(context.Background(), "queue-name", []byte("message"))
if err != nil {
    // Handle error
}

// Create consumer
consumer := dq.NewConsumer(config, "queue-name")

// Consume messages
for {
    msg, err := consumer.Consume(context.Background())
    if err != nil {
        // Handle error
        continue
    }
    // Process message
    _ = msg.Ack() // Acknowledge message
}
```

## Git Conventions

- **Commit Messages**: Clear, descriptive commit messages
- **Branching**: Use feature branches for new development
- **Pull Requests**: Required for merging to main branch
- **Tags**: Use semantic versioning for releases
- **Changelog**: Maintain changelog for significant changes

## CI/CD

- **GitHub Actions**: Likely configured in `.github/workflows/`
- **Automated Testing**: Runs on every push/PR
- **Build Verification**: Ensures all queue implementations build
- **Test Coverage**: Reports test coverage metrics
- **Release Process**: Automated release workflows

## Documentation

- **readme.md**: Contains usage examples and API documentation
- **Godoc**: Use godoc comments for inline documentation
- **Examples**: `example/` directory contains practical usage examples
- **Backend Docs**: Each queue backend may have specific documentation

## Dependency Management

- **Go Modules**: Uses Go modules for dependency management
- **Backend-Specific**: Each queue may have its own dependencies
- **Updates**: Regularly update dependencies for security and features
- **Compatibility**: Ensure backward compatibility when updating

## Cross-Queue Considerations

- **API Consistency**: Maintain consistent API across backends
- **Error Handling**: Consistent error types and handling
- **Configuration**: Similar configuration patterns
- **Testing**: Consistent testing approaches
- **Documentation**: Similar documentation structure

## Future Enhancements

- **Additional Backends**: Support for more queue systems
- **Performance**: Optimize queue operations
- **Monitoring**: Add metrics and tracing support
- **Documentation**: Expand usage examples and tutorials
- **Tooling**: CLI tools for queue management

## Task Implementation
1. **Analyze Requirements**: Refer to `README.md` for detailed feature specifications and system design.
2. **Implementation**: Modify source code in the respective directories (e.g., `src/`, `internal/`).
3. **Verification**: Run provided build and test commands (see above) to ensure correctness.
4. **Push Changes**:
   - Commit changes: `git commit -m "feat: implement <feature>"`
   - Push to remote: `git push origin <branch-name>`
