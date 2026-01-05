package hydra

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// Cluster represents a managed test cluster
type Cluster struct {
	Config      ClusterConfig
	Info        *ClusterInfo
	composeFile string
	tempDir     string
	progressCb  ProgressCallback
}

// New creates a new cluster manager (does not start the cluster)
func New(cfg ClusterConfig) *Cluster {
	return &Cluster{
		Config: cfg,
	}
}

// Start starts the cluster and waits for it to be ready
func (c *Cluster) Start(ctx context.Context) error {
	// Generate auth token for Manager before generating files
	// We need to do this here so c.Config has the token for registerClusterWithManager
	if c.Config.WithManager && c.Config.ManagerAuthToken == "" {
		c.Config.ManagerAuthToken = GenerateAuthToken()
	}

	// Generate all cluster files
	files, err := GenerateClusterFiles(c.Config)
	if err != nil {
		return fmt.Errorf("failed to generate cluster files: %w", err)
	}

	// Create temp directory for all cluster files
	c.tempDir, err = os.MkdirTemp("", "tentacle-cluster-"+c.Config.Name+"-")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Write docker-compose.yml
	c.composeFile = filepath.Join(c.tempDir, "docker-compose.yml")
	if err := os.WriteFile(c.composeFile, files.ComposeYAML, 0644); err != nil {
		return fmt.Errorf("failed to write compose file: %w", err)
	}

	// Write node config files
	for nodeName, configs := range files.NodeConfigs {
		nodeDir := filepath.Join(c.tempDir, nodeName)
		if err := os.MkdirAll(nodeDir, 0755); err != nil {
			return fmt.Errorf("failed to create node config dir: %w", err)
		}
		for filename, content := range configs {
			filePath := filepath.Join(nodeDir, filename)
			if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
				return fmt.Errorf("failed to write %s: %w", filename, err)
			}
		}
	}

	// Pull image with progress reporting (before docker-compose up)
	imageName := fmt.Sprintf("scylladb/scylla:%s", c.Config.Version)
	if err := c.pullImageWithProgress(ctx, imageName); err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}

	// Ensure Docker VM has sufficient aio-max-nr for ScyllaDB
	if err := ensureAioMaxNr(); err != nil {
		return fmt.Errorf("failed to configure system for ScyllaDB: %w", err)
	}

	// Report that we're starting containers
	c.reportProgress(ProgressEvent{
		Type:    ProgressStarting,
		Message: "Starting containers...",
	})

	// Get node specifications to determine seed and non-seed nodes
	nodes := buildNodeSpecs(c.Config)

	// Build initial node statuses for progress reporting
	seeds := findSeeds(nodes)
	nodeStatuses := make([]NodeStatus, len(nodes))
	for i, node := range nodes {
		nodeStatuses[i] = NodeStatus{
			Name:       node.name,
			Port:       node.cqlPort,
			Datacenter: node.dc,
			Rack:       node.rack,
			IsSeed:     isNodeSeed(node, seeds),
			Starting:   false,
			Checking:   false,
			Ready:      false,
		}
	}

	// For multi-node clusters with ScyllaDB 5.x, start seeds first to prevent gossip race conditions.
	// ScyllaDB 6.x+ handles concurrent startup better, so we can start all nodes in parallel.
	if len(nodes) > 1 && needsPhasedStartup(c.Config.Version) {
		// Phased startup for ScyllaDB 5.x: seeds first, then non-seeds
		// Collect seed and non-seed node names
		seedNames := make([]string, len(seeds))
		for i, seed := range seeds {
			seedNames[i] = seed.name
		}

		var nonSeedNames []string
		for _, node := range nodes {
			if !isNodeSeed(node, seeds) {
				nonSeedNames = append(nonSeedNames, node.name)
			}
		}

		// Phase 1: Start all seed nodes in parallel
		// Mark seeds as starting
		for i := range nodeStatuses {
			if nodeStatuses[i].IsSeed {
				nodeStatuses[i].Starting = true
			}
		}
		c.reportProgress(ProgressEvent{
			Type:         ProgressStarting,
			Message:      fmt.Sprintf("Starting %d seed node(s)...", len(seeds)),
			NodesTotal:   len(nodes),
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})

		args := append([]string{"up", "-d"}, seedNames...)
		if err := c.runDockerCompose(ctx, args...); err != nil {
			return fmt.Errorf("failed to start seed nodes: %w", err)
		}

		// Wait for all seeds to be ready
		// Mark seeds as checking
		for i := range nodeStatuses {
			if nodeStatuses[i].IsSeed {
				nodeStatuses[i].Checking = true
			}
		}
		c.reportProgress(ProgressEvent{
			Type:         ProgressWaiting,
			Message:      fmt.Sprintf("Waiting for %d seed(s) to be ready...", len(seeds)),
			NodesTotal:   len(nodes),
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})

		readyCount := 0
		for _, seed := range seeds {
			if err := c.waitForNodeReady(ctx, seed.cqlPort); err != nil {
				_ = c.runDockerCompose(ctx, "logs", seed.name)
				return fmt.Errorf("seed node %s failed to become ready: %w", seed.name, err)
			}
			// Mark this seed as ready
			for i := range nodeStatuses {
				if nodeStatuses[i].Name == seed.name {
					nodeStatuses[i].Checking = false
					nodeStatuses[i].Ready = true
					break
				}
			}
			readyCount++
			c.reportProgress(ProgressEvent{
				Type:         ProgressNodeReady,
				Message:      fmt.Sprintf("Seed %s ready (%d/%d seeds)", seed.name, readyCount, len(seeds)),
				NodesReady:   readyCount,
				NodesTotal:   len(nodes),
				NodeStatuses: copyNodeStatuses(nodeStatuses),
			})
		}

		// Phase 2: Start all non-seed nodes in parallel (if any)
		if len(nonSeedNames) > 0 {
			// Mark non-seeds as starting
			for i := range nodeStatuses {
				if !nodeStatuses[i].IsSeed {
					nodeStatuses[i].Starting = true
				}
			}
			c.reportProgress(ProgressEvent{
				Type:         ProgressStarting,
				Message:      fmt.Sprintf("Starting %d remaining node(s)...", len(nonSeedNames)),
				NodesReady:   len(seeds),
				NodesTotal:   len(nodes),
				NodeStatuses: copyNodeStatuses(nodeStatuses),
			})

			args = append([]string{"up", "-d"}, nonSeedNames...)
			if err := c.runDockerCompose(ctx, args...); err != nil {
				return fmt.Errorf("failed to start non-seed nodes: %w", err)
			}

			// Mark non-seeds as checking
			for i := range nodeStatuses {
				if !nodeStatuses[i].IsSeed {
					nodeStatuses[i].Checking = true
				}
			}
			c.reportProgress(ProgressEvent{
				Type:         ProgressWaiting,
				Message:      "Waiting for remaining nodes...",
				NodesReady:   len(seeds),
				NodesTotal:   len(nodes),
				NodeStatuses: copyNodeStatuses(nodeStatuses),
			})

			// Wait for non-seed nodes
			for _, node := range nodes {
				if isNodeSeed(node, seeds) {
					continue
				}
				if err := c.waitForNodeReady(ctx, node.cqlPort); err != nil {
					_ = c.runDockerCompose(ctx, "logs", node.name)
					return fmt.Errorf("node %s failed to become ready: %w", node.name, err)
				}
				// Mark this node as ready
				for i := range nodeStatuses {
					if nodeStatuses[i].Name == node.name {
						nodeStatuses[i].Checking = false
						nodeStatuses[i].Ready = true
						break
					}
				}
				readyCount++
				c.reportProgress(ProgressEvent{
					Type:         ProgressNodeReady,
					Message:      fmt.Sprintf("Node %s ready (%d/%d)", node.name, readyCount, len(nodes)),
					NodesReady:   readyCount,
					NodesTotal:   len(nodes),
					NodeStatuses: copyNodeStatuses(nodeStatuses),
				})
			}
		}

		// All ready
		c.reportProgress(ProgressEvent{
			Type:         ProgressAllReady,
			Message:      fmt.Sprintf("All %d nodes ready", len(nodes)),
			NodesReady:   len(nodes),
			NodesTotal:   len(nodes),
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})
	} else if len(nodes) > 1 {
		// Parallel startup for ScyllaDB 6.x+: start all nodes at once
		// Mark all nodes as starting
		for i := range nodeStatuses {
			nodeStatuses[i].Starting = true
		}
		c.reportProgress(ProgressEvent{
			Type:         ProgressStarting,
			Message:      fmt.Sprintf("Starting all %d nodes...", len(nodes)),
			NodesTotal:   len(nodes),
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})

		// Start all nodes at once
		if err := c.runDockerCompose(ctx, "up", "-d"); err != nil {
			return fmt.Errorf("failed to start cluster: %w", err)
		}

		// Mark all nodes as checking
		for i := range nodeStatuses {
			nodeStatuses[i].Checking = true
		}
		c.reportProgress(ProgressEvent{
			Type:         ProgressWaiting,
			Message:      fmt.Sprintf("Waiting for %d nodes to be ready...", len(nodes)),
			NodesTotal:   len(nodes),
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})

		// Wait for all nodes
		readyCount := 0
		for _, node := range nodes {
			if err := c.waitForNodeReady(ctx, node.cqlPort); err != nil {
				_ = c.runDockerCompose(ctx, "logs", node.name)
				return fmt.Errorf("node %s failed to become ready: %w", node.name, err)
			}
			// Mark this node as ready
			for i := range nodeStatuses {
				if nodeStatuses[i].Name == node.name {
					nodeStatuses[i].Checking = false
					nodeStatuses[i].Ready = true
					break
				}
			}
			readyCount++
			c.reportProgress(ProgressEvent{
				Type:         ProgressNodeReady,
				Message:      fmt.Sprintf("Node %s ready (%d/%d)", node.name, readyCount, len(nodes)),
				NodesReady:   readyCount,
				NodesTotal:   len(nodes),
				NodeStatuses: copyNodeStatuses(nodeStatuses),
			})
		}

		// All ready
		c.reportProgress(ProgressEvent{
			Type:         ProgressAllReady,
			Message:      fmt.Sprintf("All %d nodes ready", len(nodes)),
			NodesReady:   len(nodes),
			NodesTotal:   len(nodes),
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})
	} else {
		// Single node cluster - just start everything
		nodeStatuses[0].Starting = true
		c.reportProgress(ProgressEvent{
			Type:         ProgressStarting,
			Message:      "Starting node...",
			NodesTotal:   1,
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})

		if err := c.runDockerCompose(ctx, "up", "-d"); err != nil {
			return fmt.Errorf("failed to start cluster: %w", err)
		}

		nodeStatuses[0].Checking = true
		c.reportProgress(ProgressEvent{
			Type:         ProgressWaiting,
			Message:      "Waiting for node to be ready...",
			NodesTotal:   1,
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})

		if err := c.waitForNodeReady(ctx, nodes[0].cqlPort); err != nil {
			_ = c.runDockerCompose(ctx, "logs")
			return fmt.Errorf("node failed to become ready: %w", err)
		}

		nodeStatuses[0].Checking = false
		nodeStatuses[0].Ready = true
		c.reportProgress(ProgressEvent{
			Type:         ProgressAllReady,
			Message:      "Node ready",
			NodesReady:   1,
			NodesTotal:   1,
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})
	}

	// Populate cluster info
	c.Info = c.buildClusterInfo()

	// Start Manager services if enabled
	if c.Config.WithManager {
		if err := c.startManagerServices(ctx); err != nil {
			return fmt.Errorf("failed to start manager services: %w", err)
		}
	}

	return nil
}

// pullImageWithProgress pulls the scylla image with detailed progress reporting
func (c *Cluster) pullImageWithProgress(ctx context.Context, imageName string) error {
	dockerClient, err := NewDockerClient()
	if err != nil {
		// Fall back to docker-compose pull if SDK fails
		c.reportProgress(ProgressEvent{
			Type:    ProgressPullingImage,
			Message: fmt.Sprintf("Pulling %s (progress unavailable)...", imageName),
			ImagePull: &ImagePullProgress{
				ImageName: imageName,
				Status:    "pulling",
			},
		})
		return nil // Let docker-compose handle it
	}
	defer func() { _ = dockerClient.Close() }()

	// Check if image exists first
	c.reportProgress(ProgressEvent{
		Type:    ProgressPullingImage,
		Message: fmt.Sprintf("Checking if %s exists locally...", imageName),
		ImagePull: &ImagePullProgress{
			ImageName: imageName,
			Status:    "checking",
		},
	})

	// Set up progress callback
	dockerClient.SetPullCallback(func(event ImagePullEvent) {
		c.reportProgress(ProgressEvent{
			Type:    ProgressPullingImage,
			Message: event.Message,
			ImagePull: &ImagePullProgress{
				ImageName:         event.ImageName,
				Status:            event.Status,
				Percent:           event.Percent,
				LayersDownloading: event.LayersDownloading,
				LayersDownloaded:  event.LayersDownloaded,
				LayersExtracting:  event.LayersExtracting,
				LayersComplete:    event.LayersComplete,
			},
		})
	})

	// Pull if not exists
	pulled, err := dockerClient.PullImageIfNotExists(ctx, imageName)
	if err != nil {
		return err
	}

	if !pulled {
		c.reportProgress(ProgressEvent{
			Type:    ProgressPullingImage,
			Message: fmt.Sprintf("Image %s already exists", imageName),
			ImagePull: &ImagePullProgress{
				ImageName: imageName,
				Status:    "cached",
				Percent:   100,
			},
		})
	}

	return nil
}

// Stop stops the cluster
func (c *Cluster) Stop(ctx context.Context) error {
	if c.composeFile == "" {
		return nil
	}

	if err := c.runDockerCompose(ctx, "down"); err != nil {
		return fmt.Errorf("failed to stop cluster: %w", err)
	}

	return nil
}

// Destroy stops the cluster and removes all data
func (c *Cluster) Destroy(ctx context.Context) error {
	if c.composeFile == "" {
		return nil
	}

	if err := c.runDockerCompose(ctx, "down", "-v", "--remove-orphans"); err != nil {
		return fmt.Errorf("failed to destroy cluster: %w", err)
	}

	// Clean up temp directory
	if c.tempDir != "" {
		_ = os.RemoveAll(c.tempDir)
		c.tempDir = ""
	}

	c.composeFile = ""
	c.Info = nil

	return nil
}

// Logs returns the cluster logs
func (c *Cluster) Logs(ctx context.Context) (string, error) {
	if c.composeFile == "" {
		return "", fmt.Errorf("cluster not started")
	}

	cmd := exec.CommandContext(ctx, "docker-compose", "-f", c.composeFile, "logs")
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// Status returns the current cluster status
func (c *Cluster) Status(ctx context.Context) (string, error) {
	if c.composeFile == "" {
		return "", fmt.Errorf("cluster not started")
	}

	cmd := exec.CommandContext(ctx, "docker-compose", "-f", c.composeFile, "ps")
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// runDockerCompose runs a docker-compose command
func (c *Cluster) runDockerCompose(ctx context.Context, args ...string) error {
	fullArgs := append([]string{"-f", c.composeFile}, args...)
	cmd := exec.CommandContext(ctx, "docker-compose", fullArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ProgressCallback is called during cluster startup to report progress
type ProgressCallback func(event ProgressEvent)

// NodeStatus represents the status of a single node during startup
type NodeStatus struct {
	Name       string // Node name (e.g., "node1")
	Port       int    // CQL port
	Datacenter string // DC name
	Rack       string // Rack name
	IsSeed     bool   // Whether this node is a seed node
	Starting   bool   // Whether the node is starting (container launched)
	Checking   bool   // Whether we're currently checking this node
	Ready      bool   // Whether the node is ready
}

// ProgressEvent represents a progress update during cluster operations
type ProgressEvent struct {
	Type       ProgressType
	Message    string
	NodesReady int
	NodesTotal int
	// NodeStatuses provides per-node status for detailed UI display
	NodeStatuses []NodeStatus
	// CurrentNode is the node currently being checked (if any)
	CurrentNode string
	// ImagePull provides details during image pull operations
	ImagePull *ImagePullProgress
}

// ImagePullProgress provides detailed progress during image pulling
type ImagePullProgress struct {
	ImageName string
	Status    string // "checking", "pulling", "downloading", "extracting", "cached", "complete", "error"
	Percent   int
	// Aggregated layer counts for stable UI display
	LayersDownloading int // Currently downloading
	LayersDownloaded  int // Downloaded, waiting to extract
	LayersExtracting  int // Currently extracting
	LayersComplete    int // Fully done (extracted or cached)
}

// ProgressType indicates the type of progress event
type ProgressType int

const (
	ProgressWaiting ProgressType = iota
	ProgressNodeReady
	ProgressAllReady
	ProgressCheckingNode // indicates we're checking a specific node
	// New detailed progress types
	ProgressPullingImage    // pulling a docker image
	ProgressCreatingNetwork // creating docker network
	ProgressCreatingVolumes // creating docker volumes
	ProgressStarting        // starting containers via docker-compose
	ProgressStopping        // stopping containers
	ProgressRemoving        // removing containers/volumes/networks
	// Manager-related progress types
	ProgressStartingManager    // starting scylla-manager services
	ProgressWaitingForManager  // waiting for manager to be ready
	ProgressRegisteringCluster // registering cluster with manager
)

// SetProgressCallback sets a callback for progress updates
func (c *Cluster) SetProgressCallback(cb ProgressCallback) {
	c.progressCb = cb
}

// countReady counts the number of true values in a bool slice
func countReady(ready []bool) int {
	count := 0
	for _, r := range ready {
		if r {
			count++
		}
	}
	return count
}

// copyNodeStatuses creates a copy of the node statuses slice
func copyNodeStatuses(statuses []NodeStatus) []NodeStatus {
	copied := make([]NodeStatus, len(statuses))
	copy(copied, statuses)
	return copied
}

func (c *Cluster) reportProgress(event ProgressEvent) {
	if c.progressCb != nil {
		c.progressCb(event)
	}
}

// waitForNodeReady waits for a single node to be ready (used for seed node)
func (c *Cluster) waitForNodeReady(ctx context.Context, port int) error {
	timeout := c.Config.StartTimeout
	if timeout == 0 {
		timeout = 2 * time.Minute
	}

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if c.checkNodeReady(port) {
			return nil
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timeout waiting for node on port %d to be ready", port)
}

// checkNodeReady checks if a single node is accepting connections
func (c *Cluster) checkNodeReady(port int) bool {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Port = port
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.DisableInitialHostLookup = true

	if c.Config.Authentication {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: c.Config.DefaultUsername,
			Password: c.Config.DefaultPassword,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return false
	}
	defer session.Close()

	// Try a simple query
	var now time.Time
	if err := session.Query("SELECT now() FROM system.local").Scan(&now); err != nil {
		return false
	}

	return true
}

// buildClusterInfo builds cluster info from the running containers
func (c *Cluster) buildClusterInfo() *ClusterInfo {
	nodes := buildNodeSpecs(c.Config)

	info := &ClusterInfo{
		Name:    c.Config.Name,
		Version: c.Config.Version,
		Nodes:   make([]NodeInfo, len(nodes)),
		Network: c.Config.Network,
		Ready:   true,
	}

	if info.Network == "" {
		info.Network = c.Config.Name + "-net"
	}

	for i, node := range nodes {
		info.Nodes[i] = NodeInfo{
			ContainerName: fmt.Sprintf("%s-%s", c.Config.Name, node.name),
			Datacenter:    node.dc,
			Rack:          node.rack,
			CQLPort:       node.cqlPort,
			ShardPort:     node.shardPort,
			InternalIP:    "127.0.0.1",
		}
	}

	// Add Manager info if enabled
	if c.Config.WithManager {
		managerPort := c.Config.ManagerPort
		if managerPort == 0 {
			managerPort = DefaultManagerPort
		}
		info.Manager = &ManagerInfo{
			ContainerName: fmt.Sprintf("%s-scylla-manager", c.Config.Name),
			APIPort:       managerPort,
			AuthToken:     c.Config.ManagerAuthToken,
		}
	}

	return info
}

// Session creates a new gocql session to the cluster
func (c *Cluster) Session() (*gocql.Session, error) {
	if c.Info == nil || len(c.Info.Nodes) == 0 {
		return nil, fmt.Errorf("cluster not ready")
	}

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Port = c.Info.Nodes[0].CQLPort
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.DisableInitialHostLookup = true

	if c.Config.Authentication {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: c.Config.DefaultUsername,
			Password: c.Config.DefaultPassword,
		}
	}

	return cluster.CreateSession()
}

// ConnectionConfig returns a scylla.ConnectionConfig for this cluster
func (c *Cluster) ConnectionConfig() map[string]interface{} {
	if c.Info == nil || len(c.Info.Nodes) == 0 {
		return nil
	}

	return map[string]interface{}{
		"hosts":                    []string{"127.0.0.1"},
		"port":                     c.Info.Nodes[0].CQLPort,
		"username":                 c.Config.DefaultUsername,
		"password":                 c.Config.DefaultPassword,
		"disableInitialHostLookup": true,
	}
}

// Quick helpers for common operations

// StartCluster is a convenience function to start a cluster with defaults
func StartCluster(ctx context.Context, name string, version ScyllaVersion) (*Cluster, error) {
	cfg := SingleNode(name, version)
	cluster := New(cfg)
	if err := cluster.Start(ctx); err != nil {
		return nil, err
	}
	return cluster, nil
}

// StartDefaultCluster starts a default single-node cluster
func StartDefaultCluster(ctx context.Context) (*Cluster, error) {
	return StartCluster(ctx, "tentacle-test", DefaultVersion)
}

// IsDockerAvailable checks if Docker is available
func IsDockerAvailable() bool {
	cmd := exec.Command("docker", "info")
	return cmd.Run() == nil
}

// IsClusterRunning checks if a cluster with the given name is running
func IsClusterRunning(name string) bool {
	cmd := exec.Command("docker", "ps", "--filter", "label=tentacle.cluster="+name, "--format", "{{.Names}}")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) != ""
}

// ClusterExists checks if a cluster with the given name exists (running or stopped)
func ClusterExists(name string) bool {
	cmd := exec.Command("docker", "ps", "-a", "--filter", "label=tentacle.cluster="+name, "--format", "{{.Names}}")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) != ""
}

// GetRunningClusterPort returns the CQL port of a running cluster's first node
func GetRunningClusterPort(name string) (int, error) {
	cmd := exec.Command("docker", "ps", "--filter", "label=tentacle.cluster="+name, "--format", "{{.Label \"tentacle.cql-port\"}}")
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	portStr := strings.TrimSpace(string(output))
	if portStr == "" {
		return 0, fmt.Errorf("cluster %s not found", name)
	}

	// Get first port if multiple nodes
	ports := strings.Split(portStr, "\n")
	return strconv.Atoi(ports[0])
}

// GetAllUsedPorts returns all CQL and shard-aware ports used by running tentacle clusters
func GetAllUsedPorts() ([]int, error) {
	// Get all CQL ports
	cmd := exec.Command("docker", "ps", "--filter", "label=tentacle.cluster", "--format", "{{.Label \"tentacle.cql-port\"}}")
	cqlOutput, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	// Get all shard ports
	cmd = exec.Command("docker", "ps", "--filter", "label=tentacle.cluster", "--format", "{{.Label \"tentacle.shard-port\"}}")
	shardOutput, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	// Get Manager ports
	cmd = exec.Command("docker", "ps", "--filter", "label=tentacle.cluster", "--format", "{{.Label \"tentacle.manager-port\"}}")
	managerOutput, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var ports []int
	for _, line := range strings.Split(string(cqlOutput), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		port, err := strconv.Atoi(line)
		if err == nil {
			ports = append(ports, port)
		}
	}
	for _, line := range strings.Split(string(shardOutput), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		port, err := strconv.Atoi(line)
		if err == nil {
			ports = append(ports, port)
		}
	}
	for _, line := range strings.Split(string(managerOutput), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		port, err := strconv.Atoi(line)
		if err == nil {
			// Manager uses 2 consecutive ports
			ports = append(ports, port, port+1)
		}
	}

	return ports, nil
}

// FindAvailablePort finds an available base port for a new cluster
// It checks both running containers and actual port availability
func FindAvailablePort(nodeCount int) (int, error) {
	usedPorts, err := GetAllUsedPorts()
	if err != nil {
		return 0, err
	}

	// Create a set of used ports for quick lookup
	usedSet := make(map[int]bool)
	for _, p := range usedPorts {
		usedSet[p] = true
	}

	// Start from default port and find a range that's free
	// We need nodeCount CQL ports + nodeCount shard ports
	basePort := 9142
	for basePort < 65535-nodeCount*2 {
		conflict := false
		// Check CQL ports (basePort + i) and shard ports (basePort + 10000 + i)
		for i := 0; i < nodeCount; i++ {
			cqlPort := basePort + i
			shardPort := basePort + 10000 + i
			// Check both the used set AND actual port availability
			if usedSet[cqlPort] || usedSet[shardPort] ||
				!isPortAvailable(cqlPort) || !isPortAvailable(shardPort) {
				conflict = true
				break
			}
		}
		if !conflict {
			return basePort, nil
		}
		// Jump by nodeCount to find next potential range
		basePort += nodeCount
	}

	return 0, fmt.Errorf("no available port range found")
}

// CheckPortConflict checks if the given port range conflicts with existing clusters
func CheckPortConflict(basePort, nodeCount int) (bool, string, error) {
	return CheckPortConflictWithManager(basePort, nodeCount, false, 0)
}

// CheckPortConflictWithManager checks if the given port range conflicts with existing clusters,
// including Manager ports if enabled
func CheckPortConflictWithManager(basePort, nodeCount int, withManager bool, managerPort int) (bool, string, error) {
	usedPorts, err := GetAllUsedPorts()
	if err != nil {
		return false, "", err
	}

	usedSet := make(map[int]bool)
	for _, p := range usedPorts {
		usedSet[p] = true
	}

	// Check CQL and shard-aware ports for each node
	for i := 0; i < nodeCount; i++ {
		cqlPort := basePort + i
		shardPort := basePort + 10000 + i
		if usedSet[cqlPort] || !isPortAvailable(cqlPort) {
			return true, fmt.Sprintf("CQL port %d is already in use", cqlPort), nil
		}
		if usedSet[shardPort] || !isPortAvailable(shardPort) {
			return true, fmt.Sprintf("shard-aware port %d is already in use", shardPort), nil
		}
	}

	// Check Manager ports if enabled
	if withManager {
		if managerPort == 0 {
			managerPort = DefaultManagerPort
		}

		// Manager uses two ports: HTTP API and HTTPS API
		managerHTTPPort := managerPort
		managerHTTPSPort := managerPort + 1

		if usedSet[managerHTTPPort] || !isPortAvailable(managerHTTPPort) {
			return true, fmt.Sprintf("Manager HTTP port %d is already in use", managerHTTPPort), nil
		}
		if usedSet[managerHTTPSPort] || !isPortAvailable(managerHTTPSPort) {
			return true, fmt.Sprintf("Manager HTTPS port %d is already in use", managerHTTPSPort), nil
		}
	}

	return false, "", nil
}

// isPortAvailable checks if a port is available by attempting to listen on it
func isPortAvailable(port int) bool {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	_ = listener.Close() // Ignore close error - we're just checking availability
	return true
}

// FindAvailableManagerPort finds an available port for Scylla Manager
func FindAvailableManagerPort() (int, error) {
	usedPorts, err := GetAllUsedPorts()
	if err != nil {
		return 0, err
	}

	// Create a set of used ports for quick lookup
	usedSet := make(map[int]bool)
	for _, p := range usedPorts {
		usedSet[p] = true
	}

	// Start from default Manager port and find a free port
	// Manager uses 2 consecutive ports: HTTP and HTTPS
	basePort := DefaultManagerPort
	for basePort < 65535-1 {
		httpPort := basePort
		httpsPort := basePort + 1

		// Check both that the ports aren't in our used set AND that they're actually available
		if !usedSet[httpPort] && !usedSet[httpsPort] &&
			isPortAvailable(httpPort) && isPortAvailable(httpsPort) {
			return basePort, nil
		}
		basePort++
	}

	return 0, fmt.Errorf("no available manager port found")
}

// ListClusters returns names of all tentacle clusters (running or stopped)
func ListClusters() ([]string, error) {
	cmd := exec.Command("docker", "ps", "-a", "--filter", "label=tentacle.cluster", "--format", "{{.Label \"tentacle.cluster\"}}")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	// Deduplicate cluster names (multiple containers per cluster)
	seen := make(map[string]bool)
	var clusters []string
	for _, line := range strings.Split(string(output), "\n") {
		name := strings.TrimSpace(line)
		if name != "" && !seen[name] {
			seen[name] = true
			clusters = append(clusters, name)
		}
	}

	return clusters, nil
}

// ListRunningClusters returns names of running tentacle clusters
func ListRunningClusters() ([]string, error) {
	cmd := exec.Command("docker", "ps", "--filter", "label=tentacle.cluster", "--format", "{{.Label \"tentacle.cluster\"}}")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	// Deduplicate cluster names
	seen := make(map[string]bool)
	var clusters []string
	for _, line := range strings.Split(string(output), "\n") {
		name := strings.TrimSpace(line)
		if name != "" && !seen[name] {
			seen[name] = true
			clusters = append(clusters, name)
		}
	}

	return clusters, nil
}

// StopClusterByName stops all containers belonging to a cluster by name
func StopClusterByName(ctx context.Context, name string) error {
	return StopClusterByNameWithProgress(ctx, name, nil)
}

// StopClusterByNameWithProgress stops all containers with progress reporting
func StopClusterByNameWithProgress(ctx context.Context, name string, progressCb ProgressCallback) error {
	reportProgress := func(event ProgressEvent) {
		if progressCb != nil {
			progressCb(event)
		}
	}

	reportProgress(ProgressEvent{
		Type:    ProgressStopping,
		Message: fmt.Sprintf("Finding containers for cluster %s...", name),
	})

	// Get all container IDs for this cluster
	cmd := exec.CommandContext(ctx, "docker", "ps", "-q", "--filter", "label=tentacle.cluster="+name)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	containerIDs := strings.Fields(string(output))
	if len(containerIDs) == 0 {
		reportProgress(ProgressEvent{
			Type:    ProgressStopping,
			Message: "No containers to stop",
		})
		return nil // No containers to stop
	}

	reportProgress(ProgressEvent{
		Type:       ProgressStopping,
		Message:    fmt.Sprintf("Stopping %d container(s)...", len(containerIDs)),
		NodesTotal: len(containerIDs),
	})

	// Stop all containers
	args := append([]string{"stop"}, containerIDs...)
	cmd = exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to stop containers: %w", err)
	}

	reportProgress(ProgressEvent{
		Type:       ProgressStopping,
		Message:    fmt.Sprintf("Stopped %d container(s)", len(containerIDs)),
		NodesReady: len(containerIDs),
		NodesTotal: len(containerIDs),
	})

	return nil
}

// StartClusterByName starts all stopped containers belonging to a cluster by name
// and waits for them to be ready. It accepts an optional progress callback.
func StartClusterByName(ctx context.Context, name string, progressCb ProgressCallback) error {
	// Get all stopped container IDs for this cluster
	cmd := exec.CommandContext(ctx, "docker", "ps", "-aq", "--filter", "label=tentacle.cluster="+name, "--filter", "status=exited")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	containerIDs := strings.Fields(string(output))
	if len(containerIDs) == 0 {
		return fmt.Errorf("no stopped containers found for cluster %s", name)
	}

	// Start all containers
	args := append([]string{"start"}, containerIDs...)
	cmd = exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start containers: %w", err)
	}

	// Get node info from the now-running containers
	nodes, err := getClusterNodeInfo(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to get cluster node info: %w", err)
	}

	// Wait for nodes to be ready
	return waitForClusterReadyWithNodeInfo(ctx, name, nodes, progressCb)
}

// clusterNodeInfo holds node info retrieved from Docker labels
type clusterNodeInfo struct {
	name string
	port int
	dc   string
	rack string
}

// getClusterNodeInfo returns node info for all containers in a cluster
func getClusterNodeInfo(ctx context.Context, clusterName string) ([]clusterNodeInfo, error) {
	// Get node info using docker inspect format with multiple labels
	cmd := exec.CommandContext(ctx, "docker", "ps", "--filter", "label=tentacle.cluster="+clusterName,
		"--format", "{{.Label \"tentacle.node\"}}|{{.Label \"tentacle.cql-port\"}}|{{.Label \"tentacle.dc\"}}|{{.Label \"tentacle.rack\"}}")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var nodes []clusterNodeInfo
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) < 4 {
			continue
		}
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		nodes = append(nodes, clusterNodeInfo{
			name: parts[0],
			port: port,
			dc:   parts[2],
			rack: parts[3],
		})
	}

	return nodes, nil
}

// waitForClusterReadyWithNodeInfo waits for all nodes in a cluster to accept CQL connections
// with detailed per-node progress reporting
func waitForClusterReadyWithNodeInfo(ctx context.Context, name string, nodes []clusterNodeInfo, progressCb ProgressCallback) error {
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes to check for cluster %s", name)
	}

	reportProgress := func(event ProgressEvent) {
		if progressCb != nil {
			progressCb(event)
		}
	}

	// Build initial node statuses
	nodeStatuses := make([]NodeStatus, len(nodes))
	for i, node := range nodes {
		nodeStatuses[i] = NodeStatus{
			Name:       node.name,
			Port:       node.port,
			Datacenter: node.dc,
			Rack:       node.rack,
			Ready:      false,
			Checking:   false,
		}
	}

	reportProgress(ProgressEvent{
		Type:         ProgressWaiting,
		Message:      fmt.Sprintf("Waiting for %d node(s) to be ready...", len(nodes)),
		NodesTotal:   len(nodes),
		NodeStatuses: nodeStatuses,
	})

	deadline := time.Now().Add(2 * time.Minute)
	nodeReady := make([]bool, len(nodes))
	previousReady := 0

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Mark all non-ready nodes as "checking"
		for i := range nodes {
			if !nodeReady[i] {
				nodeStatuses[i].Checking = true
			}
		}

		// Report that we're checking nodes
		reportProgress(ProgressEvent{
			Type:         ProgressCheckingNode,
			Message:      fmt.Sprintf("Checking nodes... (%d/%d ready)", countReady(nodeReady), len(nodes)),
			NodesReady:   countReady(nodeReady),
			NodesTotal:   len(nodes),
			NodeStatuses: copyNodeStatuses(nodeStatuses),
		})

		// Check all nodes
		for i, node := range nodes {
			if nodeReady[i] {
				continue // Skip already-ready nodes
			}

			if checkPortReady(node.port) {
				nodeReady[i] = true
				nodeStatuses[i].Ready = true
			}
			nodeStatuses[i].Checking = false
		}

		currentReady := countReady(nodeReady)

		// Report if new nodes became ready
		if currentReady > previousReady {
			reportProgress(ProgressEvent{
				Type:         ProgressNodeReady,
				Message:      fmt.Sprintf("%d/%d nodes ready", currentReady, len(nodes)),
				NodesReady:   currentReady,
				NodesTotal:   len(nodes),
				NodeStatuses: copyNodeStatuses(nodeStatuses),
			})
			previousReady = currentReady
		}

		// Check if all nodes are ready
		if currentReady == len(nodes) {
			reportProgress(ProgressEvent{
				Type:         ProgressAllReady,
				Message:      fmt.Sprintf("All %d nodes ready", len(nodes)),
				NodesReady:   len(nodes),
				NodesTotal:   len(nodes),
				NodeStatuses: copyNodeStatuses(nodeStatuses),
			})
			return nil
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timeout waiting for cluster to be ready")
}

// checkPortReady checks if a CQL port is accepting connections
func checkPortReady(port int) bool {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Port = port
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	cluster.Timeout = 2 * time.Second
	cluster.ConnectTimeout = 2 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return false
	}
	session.Close()
	return true
}

// ListStoppedClusters returns names of stopped tentacle clusters
func ListStoppedClusters() ([]string, error) {
	cmd := exec.Command("docker", "ps", "-a", "--filter", "label=tentacle.cluster", "--filter", "status=exited", "--format", "{{.Label \"tentacle.cluster\"}}")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	// Deduplicate cluster names
	seen := make(map[string]bool)
	var clusters []string
	for _, line := range strings.Split(string(output), "\n") {
		name := strings.TrimSpace(line)
		if name != "" && !seen[name] {
			seen[name] = true
			clusters = append(clusters, name)
		}
	}

	return clusters, nil
}

// DestroyClusterByName stops and removes all containers and volumes for a cluster
func DestroyClusterByName(ctx context.Context, name string) error {
	return DestroyClusterByNameWithProgress(ctx, name, nil)
}

// DestroyClusterByNameWithProgress stops and removes all containers and volumes with progress reporting
func DestroyClusterByNameWithProgress(ctx context.Context, name string, progressCb ProgressCallback) error {
	reportProgress := func(event ProgressEvent) {
		if progressCb != nil {
			progressCb(event)
		}
	}

	reportProgress(ProgressEvent{
		Type:    ProgressRemoving,
		Message: fmt.Sprintf("Finding resources for cluster %s...", name),
	})

	// Get all container IDs for this cluster
	cmd := exec.CommandContext(ctx, "docker", "ps", "-aq", "--filter", "label=tentacle.cluster="+name)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	containerIDs := strings.Fields(string(output))
	if len(containerIDs) > 0 {
		reportProgress(ProgressEvent{
			Type:    ProgressStopping,
			Message: fmt.Sprintf("Stopping %d container(s)...", len(containerIDs)),
		})

		// Stop containers first
		args := append([]string{"stop"}, containerIDs...)
		cmd = exec.CommandContext(ctx, "docker", args...)
		_ = cmd.Run() // Ignore errors, containers might already be stopped

		reportProgress(ProgressEvent{
			Type:    ProgressRemoving,
			Message: fmt.Sprintf("Removing %d container(s)...", len(containerIDs)),
		})

		// Remove containers with volumes
		args = append([]string{"rm", "-v"}, containerIDs...)
		cmd = exec.CommandContext(ctx, "docker", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to remove containers: %w", err)
		}
	}

	reportProgress(ProgressEvent{
		Type:    ProgressRemoving,
		Message: "Removing networks...",
	})

	// Remove networks associated with this cluster
	// Networks can be named either "name-net" or with docker-compose prefix like "projectname_name-net"
	cmd = exec.CommandContext(ctx, "docker", "network", "ls", "-q", "--filter", "name="+name)
	output, err = cmd.Output()
	if err == nil {
		networks := strings.Fields(string(output))
		for _, network := range networks {
			rmCmd := exec.CommandContext(ctx, "docker", "network", "rm", network)
			_ = rmCmd.Run() // Ignore errors, network might be in use or already removed
		}
	}

	// Also try the exact network name pattern
	networkName := name + "-net"
	cmd = exec.CommandContext(ctx, "docker", "network", "rm", networkName)
	_ = cmd.Run() // Ignore errors, network might not exist

	// Prune any dangling networks (this helps clean up orphaned networks)
	cmd = exec.CommandContext(ctx, "docker", "network", "prune", "-f")
	_ = cmd.Run() // Best effort

	reportProgress(ProgressEvent{
		Type:    ProgressRemoving,
		Message: "Removing volumes...",
	})

	// Remove named volumes
	cmd = exec.CommandContext(ctx, "docker", "volume", "ls", "-q", "--filter", "name="+name+"-")
	output, err = cmd.Output()
	if err == nil {
		volumes := strings.Fields(string(output))
		if len(volumes) > 0 {
			args := append([]string{"volume", "rm"}, volumes...)
			cmd = exec.CommandContext(ctx, "docker", args...)
			_ = cmd.Run() // Best effort
		}
	}

	reportProgress(ProgressEvent{
		Type:    ProgressRemoving,
		Message: "Cluster destroyed successfully",
	})

	return nil
}

// GetClusterLogs returns logs for a running cluster by name
func GetClusterLogs(ctx context.Context, name string) (string, error) {
	// Get all container IDs for this cluster
	cmd := exec.CommandContext(ctx, "docker", "ps", "-q", "--filter", "label=tentacle.cluster="+name)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to list containers: %w", err)
	}

	containerIDs := strings.Fields(string(output))
	if len(containerIDs) == 0 {
		return "", fmt.Errorf("no running containers found for cluster %s", name)
	}

	var logs strings.Builder
	for _, id := range containerIDs {
		cmd = exec.CommandContext(ctx, "docker", "logs", "--tail", "100", id)
		containerLogs, err := cmd.CombinedOutput()
		if err != nil {
			continue
		}
		logs.WriteString(fmt.Sprintf("=== Container %s ===\n", id))
		logs.Write(containerLogs)
		logs.WriteString("\n")
	}

	return logs.String(), nil
}

// startManagerServices starts the Scylla Manager services (manager-db, manager, agents)
// and registers the cluster with the manager
func (c *Cluster) startManagerServices(ctx context.Context) error {
	// Pull manager images first
	c.reportProgress(ProgressEvent{
		Type:    ProgressPullingImage,
		Message: "Pulling Scylla Manager images...",
	})

	// Pull images in sequence (they're usually smaller than ScyllaDB)
	managerImages := []string{
		"scylladb/scylla-manager:latest",
		"scylladb/scylla-manager-agent:latest",
	}
	for _, img := range managerImages {
		if err := c.pullImageWithProgress(ctx, img); err != nil {
			return fmt.Errorf("failed to pull %s: %w", img, err)
		}
	}

	// Ensure Docker VM has sufficient aio-max-nr for ScyllaDB
	if err := ensureAioMaxNr(); err != nil {
		return fmt.Errorf("failed to configure system for ScyllaDB: %w", err)
	}

	// Get node count for progress reporting
	nodes := buildNodeSpecs(c.Config)
	nodeCount := len(nodes)

	// Start manager-db first (it needs to be ready before manager)
	c.reportProgress(ProgressEvent{
		Type:       ProgressStartingManager,
		Message:    "Starting Scylla Manager database...",
		NodesReady: nodeCount,
		NodesTotal: nodeCount,
	})
	if err := c.runDockerCompose(ctx, "up", "-d", "scylla-manager-db"); err != nil {
		return fmt.Errorf("failed to start manager-db: %w", err)
	}

	// Wait for manager-db to be ready
	c.reportProgress(ProgressEvent{
		Type:       ProgressWaitingForManager,
		Message:    "Waiting for Manager database to be ready...",
		NodesReady: nodeCount,
		NodesTotal: nodeCount,
	})
	if err := c.waitForManagerDB(ctx); err != nil {
		return fmt.Errorf("manager-db failed to become ready: %w", err)
	}

	// Start the manager service
	c.reportProgress(ProgressEvent{
		Type:       ProgressStartingManager,
		Message:    "Starting Scylla Manager...",
		NodesReady: nodeCount,
		NodesTotal: nodeCount,
	})
	if err := c.runDockerCompose(ctx, "up", "-d", "scylla-manager"); err != nil {
		return fmt.Errorf("failed to start manager: %w", err)
	}

	// Start all agent sidecars
	agentNames := make([]string, len(nodes))
	for i, node := range nodes {
		agentNames[i] = fmt.Sprintf("agent-%s", node.name)
	}

	c.reportProgress(ProgressEvent{
		Type:       ProgressStartingManager,
		Message:    fmt.Sprintf("Starting %d Manager agent(s)...", len(agentNames)),
		NodesReady: nodeCount,
		NodesTotal: nodeCount,
	})
	args := append([]string{"up", "-d"}, agentNames...)
	if err := c.runDockerCompose(ctx, args...); err != nil {
		return fmt.Errorf("failed to start agents: %w", err)
	}

	// Wait for manager to be ready (check health endpoint)
	c.reportProgress(ProgressEvent{
		Type:       ProgressWaitingForManager,
		Message:    "Waiting for Scylla Manager to be ready...",
		NodesReady: nodeCount,
		NodesTotal: nodeCount,
	})
	if err := c.waitForManager(ctx); err != nil {
		return fmt.Errorf("manager failed to become ready: %w", err)
	}

	// Register the cluster with the manager
	c.reportProgress(ProgressEvent{
		Type:       ProgressRegisteringCluster,
		Message:    "Registering cluster with Scylla Manager...",
		NodesReady: nodeCount,
		NodesTotal: nodeCount,
	})
	if err := c.registerClusterWithManager(ctx); err != nil {
		// Log but don't fail - the cluster is still usable without manager registration
		c.reportProgress(ProgressEvent{
			Type:       ProgressRegisteringCluster,
			Message:    fmt.Sprintf("Warning: Failed to register cluster with Manager: %v", err),
			NodesReady: nodeCount,
			NodesTotal: nodeCount,
		})
	} else {
		c.reportProgress(ProgressEvent{
			Type:       ProgressRegisteringCluster,
			Message:    "Cluster registered with Scylla Manager",
			NodesReady: nodeCount,
			NodesTotal: nodeCount,
		})
	}

	return nil
}

// waitForManagerDB waits for the manager-db ScyllaDB instance to be ready
func (c *Cluster) waitForManagerDB(ctx context.Context) error {
	timeout := 2 * time.Minute
	deadline := time.Now().Add(timeout)

	managerDBContainer := fmt.Sprintf("%s-scylla-manager-db", c.Config.Name)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Use docker exec to check if cqlsh works
		cmd := exec.CommandContext(ctx, "docker", "exec", managerDBContainer,
			"cqlsh", "-e", "SELECT now() FROM system.local")
		if err := cmd.Run(); err == nil {
			return nil
		}

		time.Sleep(3 * time.Second)
	}

	return fmt.Errorf("timeout waiting for manager-db to be ready")
}

// waitForManager waits for the Scylla Manager to be ready
func (c *Cluster) waitForManager(ctx context.Context) error {
	timeout := 2 * time.Minute
	deadline := time.Now().Add(timeout)

	managerContainer := fmt.Sprintf("%s-scylla-manager", c.Config.Name)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Use sctool status to check if manager is ready
		cmd := exec.CommandContext(ctx, "docker", "exec", managerContainer, "sctool", "status")
		if err := cmd.Run(); err == nil {
			return nil
		}

		time.Sleep(3 * time.Second)
	}

	return fmt.Errorf("timeout waiting for manager to be ready")
}

// registerClusterWithManager registers the ScyllaDB cluster with Scylla Manager
func (c *Cluster) registerClusterWithManager(ctx context.Context) error {
	managerContainer := fmt.Sprintf("%s-scylla-manager", c.Config.Name)

	// Get the first node's container name (hostname within docker network)
	nodes := buildNodeSpecs(c.Config)
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes to register")
	}

	firstNodeContainer := fmt.Sprintf("%s-%s", c.Config.Name, nodes[0].name)

	// Register the cluster using sctool
	// sctool cluster add --name <name> --host <host> --auth-token <token>
	cmd := exec.CommandContext(ctx, "docker", "exec", managerContainer,
		"sctool", "cluster", "add",
		"--name", c.Config.Name,
		"--host", firstNodeContainer,
		"--auth-token", c.Config.ManagerAuthToken,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sctool cluster add failed: %w (output: %s)", err, string(output))
	}

	return nil
}

// ensureAioMaxNr ensures the Docker VM has sufficient aio-max-nr for ScyllaDB
// This is particularly important on macOS where the default limit is often too low
func ensureAioMaxNr() error {
	// Required value for ScyllaDB (10M to support multiple nodes)
	// Each ScyllaDB node needs significant AIO capacity, especially with Manager
	requiredValue := "10485760"

	// Use nsenter to access the Docker VM's /proc filesystem
	// This works on Docker Desktop for Mac/Windows
	cmd := exec.Command("docker", "run", "--rm", "--privileged", "--pid=host",
		"justincormack/nsenter1", "/bin/sh", "-c",
		fmt.Sprintf("echo %s > /proc/sys/fs/aio-max-nr", requiredValue))

	if err := cmd.Run(); err != nil {
		// If this fails, it might be because we're on Linux (not needed) or the user
		// doesn't have the nsenter image. We'll just warn and continue.
		return nil // Don't fail the cluster creation, just continue
	}

	return nil
}
