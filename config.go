// Package hydra provides dynamic ScyllaDB cluster management.
// It can spawn single or multi-node clusters with various configurations,
// useful for both integration tests and in-app test environments.
package hydra

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// ScyllaVersion represents a ScyllaDB version (OSS or Enterprise)
type ScyllaVersion string

const (
	ScyllaLatest ScyllaVersion = "latest"

	// Enterprise versions (year-based, 2025.x+)
	ScyllaEnterprise2025v3 ScyllaVersion = "2025.3"
	ScyllaEnterprise2025v2 ScyllaVersion = "2025.2"
	ScyllaEnterprise2025v1 ScyllaVersion = "2025.1"

	// Open Source versions (OSS stopped at 6.2)
	Scylla6v2 ScyllaVersion = "6.2" // Last OSS release
	Scylla6v1 ScyllaVersion = "6.1"
	Scylla6v0 ScyllaVersion = "6.0"
	Scylla5v4 ScyllaVersion = "5.4" // LTS
	Scylla5v2 ScyllaVersion = "5.2"
	Scylla5v1 ScyllaVersion = "5.1"
)

// DefaultVersion is the default ScyllaDB version for new clusters
const DefaultVersion = Scylla6v2

// OSSVersions returns all Open Source versions
func OSSVersions() []ScyllaVersion {
	return []ScyllaVersion{
		Scylla6v2,
		Scylla6v1,
		Scylla6v0,
		Scylla5v4,
		Scylla5v2,
		Scylla5v1,
	}
}

// EnterpriseVersions returns all Enterprise versions
func EnterpriseVersions() []ScyllaVersion {
	return []ScyllaVersion{
		ScyllaEnterprise2025v3,
		ScyllaEnterprise2025v2,
		ScyllaEnterprise2025v1,
	}
}

// AllVersions returns all supported versions (OSS + Enterprise)
func AllVersions() []ScyllaVersion {
	return append(EnterpriseVersions(), OSSVersions()...)
}

// QuickTestVersions returns a minimal set for fast testing (latest OSS + LTS)
func QuickTestVersions() []ScyllaVersion {
	return []ScyllaVersion{
		Scylla6v2, // Latest OSS
		Scylla5v4, // LTS
	}
}

// FullTestVersions returns versions for comprehensive OSS testing
func FullTestVersions() []ScyllaVersion {
	return []ScyllaVersion{
		Scylla6v2,
		Scylla6v0,
		Scylla5v4,
		Scylla5v2,
	}
}

// FullTestWithEnterpriseVersions returns all versions including Enterprise
func FullTestWithEnterpriseVersions() []ScyllaVersion {
	return []ScyllaVersion{
		ScyllaEnterprise2025v3,
		Scylla6v2,
		Scylla6v0,
		Scylla5v4,
		Scylla5v2,
	}
}

// ClusterTopology defines the cluster structure
type ClusterTopology struct {
	// Datacenters defines the datacenter configuration
	// Key is datacenter name, value is number of nodes
	Datacenters map[string]DatacenterConfig
}

// DatacenterConfig defines a datacenter's configuration
type DatacenterConfig struct {
	// Nodes is the number of nodes in this datacenter
	Nodes int
	// Racks defines rack distribution (optional)
	// If empty, all nodes go to rack1
	Racks map[string]int
}

// ClusterConfig defines the configuration for a test cluster
type ClusterConfig struct {
	// Name is a unique identifier for this cluster (used in container names)
	Name string

	// Version is the ScyllaDB version to use
	Version ScyllaVersion

	// Topology defines the cluster structure
	// If nil, defaults to single node in dc1
	Topology *ClusterTopology

	// Authentication enables username/password authentication
	Authentication bool

	// DefaultUsername is the superuser username (default: cassandra)
	DefaultUsername string

	// DefaultPassword is the superuser password (default: cassandra)
	DefaultPassword string

	// BasePort is the starting CQL port (default: 9142)
	// Each node gets BasePort + node_index
	BasePort int

	// BaseShardAwarePort is the starting shard-aware port (default: 19142)
	BaseShardAwarePort int

	// MemoryPerNode is memory limit per node in MB (default: 512)
	MemoryPerNode int

	// CPUPerNode is the number of CPUs (smp) per node (default: 1)
	CPUPerNode int

	// StartTimeout is how long to wait for cluster to be ready
	StartTimeout time.Duration

	// Network is the Docker network name (auto-generated if empty)
	Network string

	// Labels are additional Docker labels to apply
	Labels map[string]string

	// WithManager enables Scylla Manager for the cluster
	// This adds scylla-manager, scylla-manager-db, and agent containers
	WithManager bool

	// ManagerAuthToken is the authentication token for Manager agent communication
	// If empty, a random token will be generated
	ManagerAuthToken string

	// ManagerPort is the HTTP port for Scylla Manager API (default: 5080)
	ManagerPort int
}

// DefaultConfig returns a default single-node cluster configuration
func DefaultConfig() ClusterConfig {
	return ClusterConfig{
		Name:               "tentacle-test",
		Version:            DefaultVersion,
		Authentication:     true,
		DefaultUsername:    "cassandra",
		DefaultPassword:    "cassandra",
		BasePort:           9142,
		BaseShardAwarePort: 19142,
		MemoryPerNode:      512,
		CPUPerNode:         1,
		StartTimeout:       2 * time.Minute,
		Topology: &ClusterTopology{
			Datacenters: map[string]DatacenterConfig{
				"dc1": {Nodes: 1},
			},
		},
	}
}

// SingleNode returns a single-node cluster configuration
func SingleNode(name string, version ScyllaVersion) ClusterConfig {
	cfg := DefaultConfig()
	cfg.Name = name
	cfg.Version = version
	return cfg
}

// ThreeNode returns a 3-node single-DC cluster configuration
func ThreeNode(name string, version ScyllaVersion) ClusterConfig {
	cfg := DefaultConfig()
	cfg.Name = name
	cfg.Version = version
	cfg.Topology = &ClusterTopology{
		Datacenters: map[string]DatacenterConfig{
			"dc1": {Nodes: 3},
		},
	}
	return cfg
}

// MultiDC returns a multi-datacenter cluster configuration
func MultiDC(name string, version ScyllaVersion, nodesPerDC map[string]int) ClusterConfig {
	cfg := DefaultConfig()
	cfg.Name = name
	cfg.Version = version

	dcs := make(map[string]DatacenterConfig)
	for dc, nodes := range nodesPerDC {
		dcs[dc] = DatacenterConfig{Nodes: nodes}
	}
	cfg.Topology = &ClusterTopology{Datacenters: dcs}

	return cfg
}

// WithRacks returns a cluster with rack awareness
func WithRacks(name string, version ScyllaVersion, dcRacks map[string]map[string]int) ClusterConfig {
	cfg := DefaultConfig()
	cfg.Name = name
	cfg.Version = version

	dcs := make(map[string]DatacenterConfig)
	for dc, racks := range dcRacks {
		totalNodes := 0
		for _, n := range racks {
			totalNodes += n
		}
		dcs[dc] = DatacenterConfig{
			Nodes: totalNodes,
			Racks: racks,
		}
	}
	cfg.Topology = &ClusterTopology{Datacenters: dcs}

	return cfg
}

// NodeInfo represents information about a running node
type NodeInfo struct {
	ContainerID   string
	ContainerName string
	Datacenter    string
	Rack          string
	CQLPort       int
	ShardPort     int
	InternalIP    string
}

// ManagerInfo represents information about a running Scylla Manager
type ManagerInfo struct {
	ContainerID   string
	ContainerName string
	APIPort       int
	AuthToken     string
}

// ClusterInfo represents information about a running cluster
type ClusterInfo struct {
	Name    string
	Version ScyllaVersion
	Nodes   []NodeInfo
	Network string
	Ready   bool
	Manager *ManagerInfo // nil if Manager is not enabled
}

// CQLEndpoint returns the first available CQL endpoint (host:port)
func (c *ClusterInfo) CQLEndpoint() string {
	if len(c.Nodes) == 0 {
		return ""
	}
	return c.Nodes[0].InternalIP + ":" + string(rune(c.Nodes[0].CQLPort))
}

// CQLHosts returns all CQL host addresses
func (c *ClusterInfo) CQLHosts() []string {
	hosts := make([]string, len(c.Nodes))
	for i, node := range c.Nodes {
		hosts[i] = "127.0.0.1"
		_ = node // We use localhost since we map ports
	}
	return hosts
}

// CQLPorts returns all CQL ports
func (c *ClusterInfo) CQLPorts() []int {
	ports := make([]int, len(c.Nodes))
	for i, node := range c.Nodes {
		ports[i] = node.CQLPort
	}
	return ports
}

// FirstCQLPort returns the first node's CQL port
func (c *ClusterInfo) FirstCQLPort() int {
	if len(c.Nodes) == 0 {
		return 0
	}
	return c.Nodes[0].CQLPort
}

// GenerateAuthToken generates a random auth token for Manager agent communication
func GenerateAuthToken() string {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to a fixed token if random fails (shouldn't happen)
		return "tentacle-manager-token"
	}
	return hex.EncodeToString(bytes)
}

// DefaultManagerPort is the default HTTP port for Scylla Manager API
const DefaultManagerPort = 5080
