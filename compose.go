package hydra

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// ClusterFiles holds all generated files for a cluster
type ClusterFiles struct {
	ComposeYAML []byte
	// NodeConfigs maps node name to its config files
	// e.g. "node1" -> {"cassandra-rackdc.properties": "dc=west\nrack=rack1\n"}
	NodeConfigs map[string]map[string]string
}

// ComposeFile represents a docker-compose.yml structure
type ComposeFile struct {
	Services map[string]ComposeService `yaml:"services"`
	Networks map[string]ComposeNetwork `yaml:"networks,omitempty"`
	Volumes  map[string]ComposeVolume  `yaml:"volumes,omitempty"`
}

// ComposeService represents a service in docker-compose
type ComposeService struct {
	Image         string              `yaml:"image"`
	ContainerName string              `yaml:"container_name"`
	Command       string              `yaml:"command,omitempty"`
	Environment   []string            `yaml:"environment,omitempty"`
	Ports         []string            `yaml:"ports,omitempty"`
	Networks      []string            `yaml:"networks,omitempty"`
	NetworkMode   string              `yaml:"network_mode,omitempty"`
	Volumes       []string            `yaml:"volumes,omitempty"`
	DependsOn     []string            `yaml:"depends_on,omitempty"`
	Healthcheck   *ComposeHealthcheck `yaml:"healthcheck,omitempty"`
	Labels        map[string]string   `yaml:"labels,omitempty"`
}

// ComposeHealthcheck represents a healthcheck configuration
type ComposeHealthcheck struct {
	Test        []string `yaml:"test"`
	Interval    string   `yaml:"interval"`
	Timeout     string   `yaml:"timeout"`
	Retries     int      `yaml:"retries"`
	StartPeriod string   `yaml:"start_period"`
}

// ComposeNetwork represents a network in docker-compose
type ComposeNetwork struct {
	Driver string `yaml:"driver,omitempty"`
}

// ComposeVolume represents a volume in docker-compose
type ComposeVolume struct {
	Driver string `yaml:"driver,omitempty"`
}

// nodeSpec holds computed node specifications
type nodeSpec struct {
	name        string
	dc          string
	rack        string
	index       int
	cqlPort     int
	shardPort   int
	isFirstNode bool
}

// GenerateCompose generates a docker-compose configuration for the cluster
func GenerateCompose(cfg ClusterConfig) (*ComposeFile, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("cluster name is required")
	}

	if cfg.Topology == nil || len(cfg.Topology.Datacenters) == 0 {
		cfg.Topology = &ClusterTopology{
			Datacenters: map[string]DatacenterConfig{
				"dc1": {Nodes: 1},
			},
		}
	}

	// Set defaults
	if cfg.BasePort == 0 {
		cfg.BasePort = 9142
	}
	if cfg.BaseShardAwarePort == 0 {
		cfg.BaseShardAwarePort = 19142
	}
	if cfg.MemoryPerNode == 0 {
		cfg.MemoryPerNode = 512
	}
	if cfg.CPUPerNode == 0 {
		cfg.CPUPerNode = 1
	}
	if cfg.DefaultUsername == "" {
		cfg.DefaultUsername = "cassandra"
	}
	if cfg.DefaultPassword == "" {
		cfg.DefaultPassword = "cassandra"
	}
	if cfg.Version == "" {
		cfg.Version = DefaultVersion
	}
	if cfg.ManagerPort == 0 {
		cfg.ManagerPort = DefaultManagerPort
	}
	if cfg.WithManager && cfg.ManagerAuthToken == "" {
		cfg.ManagerAuthToken = GenerateAuthToken()
	}

	networkName := cfg.Network
	if networkName == "" {
		networkName = cfg.Name + "-net"
	}

	// Build node specifications
	nodes := buildNodeSpecs(cfg)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes configured")
	}

	// Identify seeds: first node in each datacenter
	// This is important for multi-DC setups
	seeds := findSeeds(nodes)
	seedNames := make([]string, len(seeds))
	for i, seed := range seeds {
		seedNames[i] = seed.name
	}
	seedList := strings.Join(seedNames, ",")

	compose := &ComposeFile{
		Services: make(map[string]ComposeService),
		Networks: map[string]ComposeNetwork{
			networkName: {Driver: "bridge"},
		},
		Volumes: make(map[string]ComposeVolume),
	}

	for _, node := range nodes {
		serviceName := node.name
		// Node name already includes DC for multi-DC (e.g., "dc1-node1")
		containerName := fmt.Sprintf("%s-%s", cfg.Name, node.name)
		volumeName := fmt.Sprintf("%s-%s-data", cfg.Name, node.name)

		// Build command
		cmdParts := []string{
			fmt.Sprintf("--smp %d", cfg.CPUPerNode),
			fmt.Sprintf("--memory %dM", cfg.MemoryPerNode),
			"--overprovisioned 1",
			"--developer-mode 1",
		}

		if cfg.Authentication {
			cmdParts = append(cmdParts,
				"--authenticator PasswordAuthenticator",
				"--authorizer CassandraAuthorizer",
			)
		}

		// Set seeds for non-seed nodes (seeds don't need --seeds on first boot)
		if !isNodeSeed(node, seeds) {
			cmdParts = append(cmdParts, fmt.Sprintf("--seeds %s", seedList))
		}

		// DC/rack is configured via cassandra-rackdc.properties (see entrypoint script below)
		var envSlice []string

		service := ComposeService{
			Image:         fmt.Sprintf("scylladb/scylla:%s", cfg.Version),
			ContainerName: containerName,
			Command:       strings.Join(cmdParts, " "),
			Environment:   envSlice,
			Ports: []string{
				fmt.Sprintf("%d:9042", node.cqlPort),
				fmt.Sprintf("%d:19042", node.shardPort),
			},
			Networks: []string{networkName},
			Volumes: []string{
				fmt.Sprintf("%s:/var/lib/scylla", volumeName),
				// Mount node-specific configs (relative path, will be resolved by docker-compose)
				fmt.Sprintf("./%s/cassandra-rackdc.properties:/etc/scylla/cassandra-rackdc.properties:ro", node.name),
				fmt.Sprintf("./%s/scylla.yaml:/etc/scylla/scylla.yaml:ro", node.name),
			},
			Labels: map[string]string{
				"tentacle.cluster":    cfg.Name,
				"tentacle.node":       node.name,
				"tentacle.dc":         node.dc,
				"tentacle.rack":       node.rack,
				"tentacle.cql-port":   fmt.Sprintf("%d", node.cqlPort),
				"tentacle.shard-port": fmt.Sprintf("%d", node.shardPort),
			},
		}

		// Add custom labels
		for k, v := range cfg.Labels {
			service.Labels[k] = v
		}

		// Seeds get healthcheck, non-seeds depend on their DC's seed
		if isNodeSeed(node, seeds) {
			healthTest := []string{"CMD", "cqlsh", "-e", "SELECT now() FROM system.local"}
			if cfg.Authentication {
				healthTest = []string{"CMD", "cqlsh", "-u", cfg.DefaultUsername, "-p", cfg.DefaultPassword, "-e", "SELECT now() FROM system.local"}
			}
			service.Healthcheck = &ComposeHealthcheck{
				Test:        healthTest,
				Interval:    "5s",
				Timeout:     "10s",
				Retries:     30,
				StartPeriod: "30s",
			}
		} else {
			// Non-seed nodes depend on their DC's seed
			dcSeed := findDCSeed(node.dc, seeds)
			if dcSeed != nil {
				service.DependsOn = []string{dcSeed.name}
			}
		}

		compose.Services[serviceName] = service
		compose.Volumes[volumeName] = ComposeVolume{}
	}

	// Add Scylla Manager services if enabled
	if cfg.WithManager {
		addManagerServices(compose, cfg, networkName, nodes)
	}

	return compose, nil
}

// buildNodeSpecs creates node specifications from cluster config
func buildNodeSpecs(cfg ClusterConfig) []nodeSpec {
	var nodes []nodeSpec
	portIndex := 0 // Global index for port allocation

	// Sort datacenter names for deterministic ordering
	dcNames := make([]string, 0, len(cfg.Topology.Datacenters))
	for dc := range cfg.Topology.Datacenters {
		dcNames = append(dcNames, dc)
	}
	sort.Strings(dcNames)

	// Check if multi-DC for naming
	isMultiDC := len(dcNames) > 1

	for _, dc := range dcNames {
		dcCfg := cfg.Topology.Datacenters[dc]
		dcNodeIndex := 0 // Per-DC node index for naming

		if len(dcCfg.Racks) > 0 {
			// Distribute nodes across racks
			rackNames := make([]string, 0, len(dcCfg.Racks))
			for rack := range dcCfg.Racks {
				rackNames = append(rackNames, rack)
			}
			sort.Strings(rackNames)

			for _, rack := range rackNames {
				count := dcCfg.Racks[rack]
				for i := 0; i < count; i++ {
					name := fmt.Sprintf("node%d", dcNodeIndex+1)
					if isMultiDC {
						name = fmt.Sprintf("%s-node%d", dc, dcNodeIndex+1)
					}
					nodes = append(nodes, nodeSpec{
						name:        name,
						dc:          dc,
						rack:        rack,
						index:       portIndex,
						cqlPort:     cfg.BasePort + portIndex,
						shardPort:   cfg.BaseShardAwarePort + portIndex,
						isFirstNode: portIndex == 0,
					})
					portIndex++
					dcNodeIndex++
				}
			}
		} else {
			// All nodes in rack1
			for i := 0; i < dcCfg.Nodes; i++ {
				name := fmt.Sprintf("node%d", dcNodeIndex+1)
				if isMultiDC {
					name = fmt.Sprintf("%s-node%d", dc, dcNodeIndex+1)
				}
				nodes = append(nodes, nodeSpec{
					name:        name,
					dc:          dc,
					rack:        "rack1",
					index:       portIndex,
					cqlPort:     cfg.BasePort + portIndex,
					shardPort:   cfg.BaseShardAwarePort + portIndex,
					isFirstNode: portIndex == 0,
				})
				portIndex++
				dcNodeIndex++
			}
		}
	}

	return nodes
}

// ToYAML converts the compose file to YAML
func (c *ComposeFile) ToYAML() ([]byte, error) {
	return yaml.Marshal(c)
}

// GenerateComposeYAML generates docker-compose YAML for the cluster
func GenerateComposeYAML(cfg ClusterConfig) ([]byte, error) {
	compose, err := GenerateCompose(cfg)
	if err != nil {
		return nil, err
	}
	return compose.ToYAML()
}

// GenerateClusterFiles generates all files needed for the cluster
func GenerateClusterFiles(cfg ClusterConfig) (*ClusterFiles, error) {
	// Set defaults for Manager auth token before generating compose
	// (GenerateCompose will also set this, but we need it for config files)
	if cfg.WithManager && cfg.ManagerAuthToken == "" {
		cfg.ManagerAuthToken = GenerateAuthToken()
	}

	compose, err := GenerateCompose(cfg)
	if err != nil {
		return nil, err
	}

	composeYAML, err := compose.ToYAML()
	if err != nil {
		return nil, err
	}

	// Build node specifications to get DC/rack info
	nodes := buildNodeSpecs(cfg)

	// Generate config files for each node
	nodeConfigs := make(map[string]map[string]string)
	for _, node := range nodes {
		nodeConfigs[node.name] = map[string]string{
			"cassandra-rackdc.properties": generateRackDCProperties(node.dc, node.rack),
			"scylla.yaml":                 generateScyllaYAML(cfg),
		}
	}

	// Generate Manager and Agent config files if enabled
	if cfg.WithManager {
		// Manager config goes in its own directory
		nodeConfigs["scylla-manager"] = map[string]string{
			"scylla-manager.yaml": generateManagerConfig(cfg),
		}

		// Agent config for each node
		for _, node := range nodes {
			agentDir := fmt.Sprintf("agent-%s", node.name)
			nodeConfigs[agentDir] = map[string]string{
				"scylla-manager-agent.yaml": generateAgentConfig(cfg, node),
			}
		}
	}

	return &ClusterFiles{
		ComposeYAML: composeYAML,
		NodeConfigs: nodeConfigs,
	}, nil
}

// generateRackDCProperties generates the cassandra-rackdc.properties content
func generateRackDCProperties(dc, rack string) string {
	return fmt.Sprintf("dc=%s\nrack=%s\n", dc, rack)
}

// generateScyllaYAML generates the scylla.yaml content with required settings
func generateScyllaYAML(cfg ClusterConfig) string {
	// Use GossipingPropertyFileSnitch to read DC/rack from cassandra-rackdc.properties
	yaml := `# Generated by tentacle cluster provisioning
cluster_name: '` + cfg.Name + `'
endpoint_snitch: GossipingPropertyFileSnitch
`

	// ScyllaDB 5.4+ introduced Raft-based consistent cluster management which
	// requires schema commit log. For development/test clusters, we disable it
	// to allow simpler setup without persistent schema logs.
	// This is safe for test clusters but should NOT be used in production.
	if needsRaftDisabled(cfg.Version) {
		yaml += `# Disable Raft-based cluster management for simpler dev/test setup
consistent_cluster_management: false
`
	}

	// If Manager is enabled, expose the REST API on all interfaces
	// This allows the agent sidecar to connect to the Scylla API
	if cfg.WithManager {
		yaml += `# REST API configuration for Scylla Manager Agent
api_address: 0.0.0.0
api_port: 10000
`
	}

	return yaml
}

// needsRaftDisabled returns true for ScyllaDB versions that have Raft enabled by default
// but we want to disable it for simpler dev/test cluster setup
func needsRaftDisabled(version ScyllaVersion) bool {
	// ScyllaDB 5.4 (LTS) and 5.2 have Raft which requires schema commit log
	// ScyllaDB 6.x handles this better and works without disabling
	switch version {
	case Scylla5v4, Scylla5v2, Scylla5v1:
		return true
	default:
		return false
	}
}

// needsPhasedStartup returns true for ScyllaDB versions that need seeds to be
// started first before non-seed nodes (to avoid gossip race conditions)
func needsPhasedStartup(version ScyllaVersion) bool {
	// ScyllaDB 5.x has gossip issues when nodes start concurrently
	// ScyllaDB 6.x and Enterprise versions handle this better
	switch version {
	case Scylla5v4, Scylla5v2, Scylla5v1:
		return true
	default:
		return false
	}
}

// findSeeds returns the first node in each datacenter (to be used as seeds)
func findSeeds(nodes []nodeSpec) []nodeSpec {
	seenDCs := make(map[string]bool)
	var seeds []nodeSpec

	for _, node := range nodes {
		if !seenDCs[node.dc] {
			seenDCs[node.dc] = true
			seeds = append(seeds, node)
		}
	}

	return seeds
}

// isNodeSeed checks if a node is in the seeds list
func isNodeSeed(node nodeSpec, seeds []nodeSpec) bool {
	for _, seed := range seeds {
		if seed.name == node.name {
			return true
		}
	}
	return false
}

// findDCSeed returns the seed node for a given datacenter
func findDCSeed(dc string, seeds []nodeSpec) *nodeSpec {
	for i := range seeds {
		if seeds[i].dc == dc {
			return &seeds[i]
		}
	}
	return nil
}

// addManagerServices adds Scylla Manager related services to the compose file
// This includes:
// - scylla-manager-db: A dedicated ScyllaDB instance for Manager's backend
// - scylla-manager: The Manager server
// - scylla-manager-agent-<node>: An agent sidecar for each ScyllaDB node
func addManagerServices(compose *ComposeFile, cfg ClusterConfig, networkName string, nodes []nodeSpec) {
	managerDBName := "scylla-manager-db"
	managerDBContainerName := fmt.Sprintf("%s-%s", cfg.Name, managerDBName)
	managerDBVolumeName := fmt.Sprintf("%s-%s-data", cfg.Name, managerDBName)

	// Manager DB service - a minimal ScyllaDB instance for Manager's metadata
	managerDBService := ComposeService{
		Image:         "scylladb/scylla:latest",
		ContainerName: managerDBContainerName,
		Command:       "--smp 1 --memory 512M --overprovisioned 1 --developer-mode 1",
		Networks:      []string{networkName},
		Volumes: []string{
			fmt.Sprintf("%s:/var/lib/scylla", managerDBVolumeName),
		},
		Healthcheck: &ComposeHealthcheck{
			Test:        []string{"CMD", "cqlsh", "-e", "SELECT now() FROM system.local"},
			Interval:    "5s",
			Timeout:     "10s",
			Retries:     30,
			StartPeriod: "30s",
		},
		Labels: map[string]string{
			"tentacle.cluster": cfg.Name,
			"tentacle.service": "manager-db",
		},
	}
	compose.Services[managerDBName] = managerDBService
	compose.Volumes[managerDBVolumeName] = ComposeVolume{}

	// Manager service
	managerName := "scylla-manager"
	managerContainerName := fmt.Sprintf("%s-%s", cfg.Name, managerName)

	managerService := ComposeService{
		Image:         "scylladb/scylla-manager:latest",
		ContainerName: managerContainerName,
		Networks:      []string{networkName},
		Ports: []string{
			fmt.Sprintf("%d:5080", cfg.ManagerPort),   // HTTP API
			fmt.Sprintf("%d:5443", cfg.ManagerPort+1), // HTTPS API
		},
		DependsOn: []string{managerDBName},
		Volumes: []string{
			"./scylla-manager/scylla-manager.yaml:/etc/scylla-manager/scylla-manager.yaml:ro",
		},
		Healthcheck: &ComposeHealthcheck{
			Test:        []string{"CMD", "sctool", "status"},
			Interval:    "10s",
			Timeout:     "5s",
			Retries:     30,
			StartPeriod: "60s",
		},
		Labels: map[string]string{
			"tentacle.cluster":      cfg.Name,
			"tentacle.service":      "manager",
			"tentacle.manager-port": fmt.Sprintf("%d", cfg.ManagerPort),
		},
	}
	compose.Services[managerName] = managerService

	// Agent sidecar for each ScyllaDB node
	// The agent shares the network namespace with its ScyllaDB node so that
	// the Manager can reach the agent at the same address as the ScyllaDB node.
	// This is the standard sidecar pattern for Scylla Manager.
	for _, node := range nodes {
		agentName := fmt.Sprintf("agent-%s", node.name)
		agentContainerName := fmt.Sprintf("%s-%s", cfg.Name, agentName)
		nodeContainerName := fmt.Sprintf("%s-%s", cfg.Name, node.name)
		nodeVolumeName := fmt.Sprintf("%s-%s-data", cfg.Name, node.name)

		agentService := ComposeService{
			Image:         "scylladb/scylla-manager-agent:latest",
			ContainerName: agentContainerName,
			// Share network namespace with the ScyllaDB node container
			// This allows the agent to be reached at the same address as the node
			NetworkMode: fmt.Sprintf("service:%s", node.name),
			DependsOn:   []string{node.name},
			Volumes: []string{
				fmt.Sprintf("./agent-%s/scylla-manager-agent.yaml:/etc/scylla-manager-agent/scylla-manager-agent.yaml:ro", node.name),
				// Mount the ScyllaDB data volume - the agent needs access for backups
				fmt.Sprintf("%s:/var/lib/scylla:ro", nodeVolumeName),
			},
			// Override the default command to specify the config file location
			Command: "-c /etc/scylla-manager-agent/scylla-manager-agent.yaml",
			Labels: map[string]string{
				"tentacle.cluster":     cfg.Name,
				"tentacle.service":     "manager-agent",
				"tentacle.node":        node.name,
				"tentacle.scylla-host": nodeContainerName,
			},
		}
		compose.Services[agentName] = agentService
	}
}

// generateManagerConfig generates the scylla-manager.yaml configuration
func generateManagerConfig(_ ClusterConfig) string {
	return `# Scylla Manager configuration
# Generated by tentacle cluster provisioning

# HTTP API configuration
http: 0.0.0.0:5080

# HTTPS API configuration (optional)
# https: 0.0.0.0:5443

# Database configuration - uses the dedicated manager-db instance
database:
  hosts:
    - scylla-manager-db

# Logging
logger:
  mode: stderr
  level: info
`
}

// generateAgentConfig generates the scylla-manager-agent.yaml configuration for a node
func generateAgentConfig(cfg ClusterConfig, _ nodeSpec) string {
	// Since the agent shares the network namespace with the ScyllaDB node
	// (via network_mode: service:<node>), we use localhost to connect to the Scylla API
	return fmt.Sprintf(`# Scylla Manager Agent configuration
# Generated by tentacle cluster provisioning

# Authentication token - must match for all agents in the cluster
auth_token: %s

# Scylla API configuration
# The agent shares network namespace with ScyllaDB, so use localhost
scylla:
  api_address: localhost
  api_port: "10000"

# HTTPS configuration for agent API
# The agent listens for Manager connections on port 10001
https: 0.0.0.0:10001

# CPU configuration - disable CPU pinning for Docker environment
cpu: -1

# Logging
logger:
  mode: stderr
  level: info
`, cfg.ManagerAuthToken)
}
