# Hydra

[![CI](https://github.com/tentacle-scylla/hydra/workflows/CI/badge.svg)](https://github.com/tentacle-scylla/hydra/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/tentacle-scylla/hydra)](https://goreportcard.com/report/github.com/tentacle-scylla/hydra)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/tentacle-scylla/hydra)](https://pkg.go.dev/github.com/tentacle-scylla/hydra)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Spin up ephemeral ScyllaDB clusters via Docker Compose. Useful for integration tests, local development, and experimenting with different configurations.

## Installation

```bash
go get github.com/tentacle-scylla/hydra
```

## CLI

```bash
go install github.com/tentacle-scylla/hydra/cmd/hydra@latest
```

### Create a cluster

```bash
# Interactive mode (prompts for options)
hydra create

# Single node with defaults
hydra create --non-interactive

# Specific version and node count
hydra create --version 6.2 --nodes 3

# Multi-datacenter
hydra create --multi-dc "east:2,west:2"

# Without authentication
hydra create --no-auth
```

### Manage clusters

```bash
hydra list              # List all clusters
hydra stop              # Stop a running cluster (keeps data)
hydra start             # Start a stopped cluster
hydra destroy           # Remove cluster and all data
hydra logs              # Show cluster logs
hydra versions          # List available ScyllaDB versions
```

### Connect

Once the cluster is running:

```bash
cqlsh -u cassandra -p cassandra 127.0.0.1 9142
```

## Library

```go
import "github.com/tentacle-scylla/hydra"

// Single node
cfg := hydra.SingleNode("my-cluster", hydra.Scylla6v2)

// 3-node cluster
cfg := hydra.ThreeNode("my-cluster", hydra.Scylla6v2)

// Multi-DC
cfg := hydra.MultiDC("my-cluster", hydra.Scylla6v2, map[string]int{
    "dc1": 2,
    "dc2": 1,
})

// Start
cluster := hydra.New(cfg)
if err := cluster.Start(ctx); err != nil {
    log.Fatal(err)
}
defer cluster.Destroy(ctx)

// Connect
port := cluster.Info.FirstCQLPort()
fmt.Printf("CQL available at 127.0.0.1:%d\n", port)
```

### Configuration

```go
cfg := hydra.ClusterConfig{
    Name:            "test-cluster",
    Version:         hydra.Scylla6v2,
    Authentication:  true,
    DefaultUsername: "cassandra",
    DefaultPassword: "cassandra",
    BasePort:        9142,        // Auto-assigned if 0
    MemoryPerNode:   512,         // MB
    CPUPerNode:      1,
    StartTimeout:    2 * time.Minute,
    Topology: &hydra.ClusterTopology{
        Datacenters: map[string]hydra.DatacenterConfig{
            "dc1": {Nodes: 3},
        },
    },
}
```

### Progress callbacks

```go
cluster.SetProgressCallback(func(event hydra.ProgressEvent) {
    switch event.Type {
    case hydra.ProgressWaiting:
        fmt.Println("Waiting for nodes...")
    case hydra.ProgressNodeReady:
        fmt.Println(event.Message)
    case hydra.ProgressAllReady:
        fmt.Println("Cluster ready!")
    }
})
```

## Supported versions

**Open Source** (6.2 is the last OSS release):
- 6.2, 6.1, 6.0
- 5.4 (LTS), 5.2, 5.1

**Enterprise**:
- 2025.3, 2025.2, 2025.1

```go
hydra.OSSVersions()        // []ScyllaVersion
hydra.EnterpriseVersions() // []ScyllaVersion
hydra.QuickTestVersions()  // Latest + LTS
```

## Requirements

- Docker
- Docker Compose

## License

MIT
