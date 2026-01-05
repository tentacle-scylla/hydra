# Hydra

A Go library for managing ScyllaDB clusters in Docker containers.

## Development

### Prerequisites

- Go 1.21+
- Docker Desktop (for macOS/Windows)
- golangci-lint

### Running Tests

```bash
make test
```

### Code Quality

Always run validation before committing:

```bash
# Format code
make fmt

# Run static analysis
make vet

# Run linter
make lint

# Run all checks
make check
```

## Release Process

**IMPORTANT**: Always perform all validation steps locally before creating releases.

### Creating a Release

Use the `make release` target which automatically:
1. Formats code with `gofmt`
2. Runs `go vet` for static analysis
3. Runs `golangci-lint` for comprehensive linting
4. Runs all tests
5. Validates VERSION parameter is provided
6. Checks working directory is clean (no uncommitted changes)
7. Creates git tag
8. Pushes to origin with tags

```bash
# Create a new release (e.g., v0.1.4)
make release VERSION=v0.1.4
```

**Never create releases manually** - always use `make release` to ensure all validation passes.

## Architecture

### Core Components

- **Cluster Management** (`cluster.go`): Create, start, stop, destroy ScyllaDB clusters
- **Manager Integration** (`cluster.go`): Optional Scylla Manager support with auto-configuration
- **Docker Utilities** (`docker.go`): Docker container and network management
- **Port Management** (`cluster.go`): Dynamic port allocation and collision detection

### Key Features

- **Dynamic Port Allocation**: Automatically finds available ports for CQL and Manager
- **Port Collision Detection**: Validates ports against both container labels and system availability
- **macOS Compatibility**: Auto-configures Docker VM kernel parameters (aio-max-nr) for ScyllaDB
- **Progress Events**: Real-time cluster creation status updates
- **Scylla Manager Integration**: Optional backup, repair, and cluster management

### macOS Docker VM Configuration

On macOS, ScyllaDB runs in the Docker Desktop VM which requires kernel parameter tuning. Hydra automatically configures `aio-max-nr` to 10M (10485760) to support multiple ScyllaDB nodes with Manager.

This is done via the `ensureAioMaxNr()` function which uses `justincormack/nsenter1` to modify kernel parameters in the Docker VM.

## Testing in Tentacle

Hydra is used by the Tentacle desktop application. For local development across both repositories, see the workspace setup in the parent README.
