# CDP Pipeline Workflow Monorepo

This repository is structured as a monorepo to facilitate easier development, testing, and deployment of the CDP Pipeline Workflow components.

## Directory Structure

```
cdp-pipeline-workflow/
├── cmd/                              # Command-line applications
│   ├── pipeline/                     # Main pipeline application
│   └── tools/                        # Utility tools
├── internal/                         # Private application and library code
│   ├── common/                       # Shared code used across the project
│   │   ├── types/                    # Common type definitions
│   │   ├── utils/                    # Utility functions
│   │   └── config/                   # Configuration handling
│   └── platform/                     # Platform-specific implementations
│       └── stellar/                  # Stellar-specific code
├── pkg/                              # Public library code
│   ├── processor/                    # Core processor interfaces and implementations
│   │   ├── base/                     # Base processor implementations
│   │   ├── contract/                 # Contract-specific processors
│   │   │   ├── kale/                 # Kale processor
│   │   │   ├── soroswap/             # Soroswap processor
│   │   │   └── common/               # Shared contract processor code
│   │   └── ledger/                   # Ledger processors
│   ├── consumer/                     # Consumer implementations
│   │   ├── database/                 # Database consumers (PostgreSQL, etc.)
│   │   ├── messaging/                # Messaging consumers (ZeroMQ, Kafka, etc.)
│   │   ├── api/                      # API consumers (REST, GraphQL, etc.)
│   │   └── websocket/                # WebSocket consumers
│   └── pluginapi/                    # Plugin API interfaces and implementations
├── plugins/                          # Plugin implementations
│   ├── kale/                         # Kale plugin
│   └── soroswap/                     # Soroswap plugin
├── examples/                         # Example code
│   ├── kale/                         # Kale examples
│   └── soroswap/                     # Soroswap examples
├── docs/                             # Documentation
│   ├── architecture/                 # Architecture diagrams and descriptions
│   ├── api/                          # API documentation
│   └── guides/                       # User and developer guides
├── deployment/                       # Deployment configurations
│   ├── kubernetes/                   # Kubernetes manifests
│   └── docker/                       # Dockerfiles and docker-compose files
├── tests/                            # Integration and end-to-end tests
│   ├── integration/                  # Integration tests
│   └── e2e/                          # End-to-end tests
├── go.mod                            # Go modules definition
└── README.md                         # Project overview
```

## Package Structure

### cmd/

Contains the main executable applications of the project. Each subdirectory is a separate application.

### internal/

Contains code that's private to this repository and not meant to be imported by other projects. This is enforced by Go's module system.

### pkg/

Contains code that can be imported and used by other projects. This is where most of the reusable components live.

#### pkg/processor/

Contains all processor implementations, with interfaces defined in the `base` package.

- **base/**: Core interfaces and base implementations
- **contract/**: Processors for specific smart contracts
- **ledger/**: Processors for Stellar ledger data

#### pkg/consumer/

Contains all consumer implementations, with interfaces defined in the `base` package.

- **base/**: Core interfaces and base implementations
- **database/**: Consumers that save data to databases
- **messaging/**: Consumers that publish to message queues
- **api/**: Consumers that expose data via APIs
- **websocket/**: Consumers that send data over WebSockets

### plugins/

Contains plugin implementations that can be loaded dynamically.

### examples/

Contains example code showing how to use the various components.

## Key Interfaces

### Processor

```go
// Processor defines the interface for processing messages.
type Processor interface {
	Process(context.Context, Message) error
	Subscribe(Processor)
}
```

### Consumer

```go
// Consumer defines the interface for consuming messages.
type Consumer interface {
	Process(context.Context, Message) error
	Subscribe(Processor)
}
```

## Getting Started

See the examples directory for usage examples.

## Building and Testing

```bash
# Build all applications
go build ./cmd/...

# Run tests
go test ./...

# Run specific example
go run ./examples/kale/kale_processor_example.go
``` 