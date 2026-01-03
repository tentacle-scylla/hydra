// Command hydra provides a CLI for managing ScyllaDB test clusters.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tentacle-scylla/hydra"
	"github.com/tentacle-scylla/hydra/cmd/ui"
)

// Version is set at build time via ldflags:
// go build -ldflags "-X main.Version=1.0.0"
var Version = "dev"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "create":
		cmdCreate(args)
	case "start":
		cmdStart(args)
	case "stop":
		cmdStop(args)
	case "destroy":
		cmdDestroy(args)
	case "list", "ls":
		cmdList(args)
	case "logs":
		cmdLogs(args)
	case "seed":
		cmdSeed(args)
	case "versions":
		cmdVersions()
	case "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printBanner() {
	logoLines := []string{
		"",
		"⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣴⣾⣿⣿⣿⣿⣿⣿⣶⣤⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠀⠀⠀⠀",
		"⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣴⣿⣿⣿⡿⢟⣯⣭⣭⣛⢿⣿⣿⣿⣷⣄⠀⠀⠀⠀⠀⢠⣾⠛⢻⡆⠀⠀",
		"⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣾⣿⣿⣿⡟⣴⣿⠟⠛⠻⣿⣷⣹⣿⣿⣿⣿⣷⡀⠀⠀⠀⢸⣿⡄⠀⠀⠀⠀",
		"⠀⠀⣰⡶⣶⣄⠀⠀⠀⢀⣿⣿⣿⣿⣿⡇⣿⡇⢀⠀⢀⢸⣿⣏⣿⣿⣿⣿⣿⣷⠀⠀⠀⠈⢿⡿⣄⠀⠀⠀",
		"⠀⠀⠛⠀⢀⣿⡆⠀⠀⢸⣿⣿⣿⣿⣿⣷⡹⣿⣮⣯⣵⣿⢟⣼⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠘⣿⣿⠀⠀⠀",
		"⠀⠀⠀⠀⡞⣿⠀⠀⠀⢿⣿⣿⣿⣿⣿⣋⠻⣶⣭⣭⣭⡷⠿⢃⣽⣿⣿⣿⣿⣿⡇⠀⠀⠀⢰⣿⣻⠃⠀⠀",
		"⠀⠀⠀⢸⢹⡏⠀⠀⠀⠸⣿⣿⣿⣿⣿⣿⣆⠀⢀⣀⣀⡀⢀⣼⣿⣿⣿⣿⣿⡿⠃⢀⣀⣴⡿⢣⡟⠀⠀⠀",
		"⠀⠀⠀⣾⢸⣧⠀⠀⠀⢰⣾⣿⣿⣿⣿⣿⣿⣷⣬⣭⣭⣴⣿⣿⣿⣿⣿⣿⡿⣿⠇⠘⠛⣉⡴⠋⠀⠀⠀⠀",
		"⠀⠀⠀⠘⣧⠻⣷⣤⣤⣤⡉⠀⠹⠿⠿⠿⢿⣿⣿⣿⠿⣿⣿⣿⠟⠛⠿⠟⠀⠒⠚⠛⠋⠁⠀⠀⠀⠀⠀⠀",
		"⠀⠀⠀⠀⠘⠛⢦⣍⣉⣡⡴⠊⢀⣄⠀⣠⡀⢉⠉⠀⠀⠀⠉⢁⡀⠀⢱⡀⠻⣶⣤⣄⣀⠀⠀⠀⠀⠀⠀⠀",
		"⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⡏⢰⣿⠃⡼⠀⠀⠀⠀⡄⢻⣷⠀⠀⠉⠑⠦⠭⠩⣝⠻⣦⠀⠀⠀⠀⠀",
		"⠀⠀⠀⠀⣠⠶⠶⢦⣄⣀⣀⣴⠟⢁⢸⣿⡀⡄⠀⠀⠀⠀⠘⡀⢿⣇⠀⠀⠀⠀⠀⠀⠀⠀⢸⠇⠀⠀⠀⠀",
		"⠀⠀⠀⠀⠁⠀⠀⠂⠨⣉⠉⠡⠔⠁⠘⣿⣧⢃⠀⠀⠀⠀⠀⢧⢸⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀",
		"⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢿⣧⠁⠀⠀⠀⠀⢸⢸⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀",
		"⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢿⡇⠀⠀⠀⠀⠈⣿⡏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀",
		"⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠁⠀⠀⠀⠀⣸⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀",
		"⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀",
	}

	// ASCII art title
	titleArt := []string{
		":::    ::: :::   ::: :::::::::  :::::::::      :::     ",
		":+:    :+: :+:   :+: :+:    :+: :+:    :+:   :+: :+:   ",
		"+:+    +:+  +:+ +:+  +:+    +:+ +:+    +:+  +:+   +:+  ",
		"+#++:++#++   +#++:   +#+    +:+ +#++:++#:  +#++:++#++: ",
		"+#+    +#+    +#+    +#+    +#+ +#+    +#+ +#+     +#+ ",
		"#+#    #+#    #+#    #+#    #+# #+#    #+# #+#     #+# ",
		"###    ###    ###    #########  ###    ### ###     ### ",
	}

	titleLines := []string{
		"",
		"",
		"",
		ui.Bold.Foreground(ui.ColorCyan).Render(titleArt[0]),
		ui.Bold.Foreground(ui.ColorCyan).Render(titleArt[1]),
		ui.Bold.Foreground(ui.ColorCyan).Render(titleArt[2]),
		ui.Bold.Foreground(ui.ColorCyan).Render(titleArt[3]),
		ui.Bold.Foreground(ui.ColorCyan).Render(titleArt[4]),
		ui.Bold.Foreground(ui.ColorCyan).Render(titleArt[5]),
		ui.Bold.Foreground(ui.ColorCyan).Render(titleArt[6]),
		"",
		ui.Highlight.Render(" Spin up test ScyllaDB clusters in seconds"),
		"",
		" " + ui.Muted.Render("github.com/tentacle-scylla/hydra") + "  " + ui.Cyan.Render("v"+Version),
		"",
		"",
		"",
		"",
		"",
	}

	// Print logo and title side by side
	for i, logoLine := range logoLines {
		fmt.Print(ui.Cyan2.Render(logoLine))
		if i < len(titleLines) {
			fmt.Print("  " + titleLines[i])
		}
		fmt.Println()
	}
	fmt.Println()
}

func printUsage() {
	printBanner()
	fmt.Println(`Usage: hydra <command> [options]

Commands:
  create    Create and start a new cluster
  start     Start a stopped cluster
  stop      Stop a running cluster (keeps data)
  destroy   Stop and remove cluster (deletes all data)
  list      List all clusters
  logs      Show cluster logs
  seed      Populate cluster with sample schema and data
  versions  List available ScyllaDB versions
  help      Show this help

Examples:
  hydra create
  hydra create --version 6.2 --nodes 3
  hydra create --multi-dc "east:2,west:2"
  hydra seed --name my-cluster
  hydra seed --rows 1000 --no-data
  hydra stop
  hydra destroy`)
}

func cmdCreate(args []string) {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	version := fs.String("version", "", "ScyllaDB version")
	name := fs.String("name", "", "Cluster name")
	nodes := fs.Int("nodes", 0, "Number of nodes (single DC)")
	multiDC := fs.String("multi-dc", "", "Multi-DC config: dc1:2,dc2:1")
	port := fs.Int("port", 0, "Base CQL port")
	noAuth := fs.Bool("no-auth", false, "Disable authentication")
	nonInteractive := fs.Bool("non-interactive", false, "Non-interactive mode (use defaults)")
	timeout := fs.Duration("timeout", 3*time.Minute, "Startup timeout")
	_ = fs.Parse(args)

	// Determine if we should run interactively
	// Interactive if: no flags provided AND not explicitly non-interactive
	hasFlags := *version != "" || *name != "" || *nodes != 0 || *multiDC != "" || *port != 0 || *noAuth
	interactive := !hasFlags && !*nonInteractive

	var cfg hydra.ClusterConfig

	if interactive {
		// Interactive mode - prompt user
		uiCfg, err := ui.PromptUpConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %s\n", ui.ErrorIcon, err)
			os.Exit(1)
		}

		cfg = hydra.ClusterConfig{
			Name:            uiCfg.Name,
			Version:         hydra.ScyllaVersion(uiCfg.Version),
			BasePort:        0, // Auto-assign port
			Authentication:  uiCfg.Auth,
			DefaultUsername: "cassandra",
			DefaultPassword: "cassandra",
			StartTimeout:    time.Duration(uiCfg.Timeout) * time.Second,
		}

		if uiCfg.MultiDC != "" {
			dcs, err := parseMultiDC(uiCfg.MultiDC)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s Invalid multi-dc config: %v\n", ui.ErrorIcon, err)
				os.Exit(1)
			}
			cfg.Topology = &hydra.ClusterTopology{Datacenters: dcs}
		} else {
			cfg.Topology = &hydra.ClusterTopology{
				Datacenters: map[string]hydra.DatacenterConfig{
					"dc1": {Nodes: uiCfg.Nodes},
				},
			}
		}
	} else {
		// Non-interactive mode - use flags with defaults
		clusterName := *name
		if clusterName == "" {
			clusterName = "tentacle-test"
		}
		clusterVersion := *version
		if clusterVersion == "" {
			clusterVersion = string(hydra.DefaultVersion)
		}
		clusterNodes := *nodes
		if clusterNodes == 0 {
			clusterNodes = 1
		}

		cfg = hydra.ClusterConfig{
			Name:            clusterName,
			Version:         hydra.ScyllaVersion(clusterVersion),
			BasePort:        *port, // 0 means auto-assign
			Authentication:  !*noAuth,
			DefaultUsername: "cassandra",
			DefaultPassword: "cassandra",
			StartTimeout:    *timeout,
		}

		if *multiDC != "" {
			dcs, err := parseMultiDC(*multiDC)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s Invalid multi-dc config: %v\n", ui.ErrorIcon, err)
				os.Exit(1)
			}
			cfg.Topology = &hydra.ClusterTopology{Datacenters: dcs}
		} else {
			cfg.Topology = &hydra.ClusterTopology{
				Datacenters: map[string]hydra.DatacenterConfig{
					"dc1": {Nodes: clusterNodes},
				},
			}
		}
	}

	// Count total nodes for port allocation
	nodeCount := 0
	for _, dc := range cfg.Topology.Datacenters {
		nodeCount += dc.Nodes
	}

	// Auto-assign port if not explicitly set (port == 0 means use auto)
	if cfg.BasePort == 0 {
		availablePort, err := hydra.FindAvailablePort(nodeCount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to find available port: %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
		cfg.BasePort = availablePort
		cfg.BaseShardAwarePort = availablePort + 10000
	} else {
		// Check for port conflicts if port was explicitly set
		conflict, msg, err := hydra.CheckPortConflict(cfg.BasePort, nodeCount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to check port conflicts: %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
		if conflict {
			fmt.Fprintf(os.Stderr, "%s Port conflict: %s\n", ui.ErrorIcon, msg)
			os.Exit(1)
		}
	}

	// Check if cluster already exists
	if hydra.IsClusterRunning(cfg.Name) {
		existingPort, _ := hydra.GetRunningClusterPort(cfg.Name)
		fmt.Printf("%s Cluster '%s' is already running (port %d)\n", ui.WarningIcon, cfg.Name, existingPort)
		return
	}

	// Clean up any stopped containers before starting
	if hydra.ClusterExists(cfg.Name) {
		fmt.Printf("%s Removing stopped cluster '%s'...\n", ui.InfoIcon, cfg.Name)
		ctx := context.Background()
		if err := hydra.DestroyClusterByName(ctx, cfg.Name); err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to clean up existing cluster: %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
	}

	fmt.Printf("%s Starting ScyllaDB %s cluster '%s'...\n", ui.SpinnerIcon, cfg.Version, cfg.Name)

	cluster := hydra.New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.StartTimeout+30*time.Second)
	defer cancel()

	// Set up spinner for progress updates
	spinner := ui.NewSpinner()
	var lastEvent hydra.ProgressEvent

	cluster.SetProgressCallback(func(event hydra.ProgressEvent) {
		lastEvent = event
		switch event.Type {
		case hydra.ProgressWaiting:
			spinner.Start(event.Message)
		case hydra.ProgressNodeReady:
			spinner.UpdateMessage(event.Message)
		case hydra.ProgressAllReady:
			spinner.StopWithSuccess(event.Message)
		}
	})

	if err := cluster.Start(ctx); err != nil {
		if lastEvent.Type != hydra.ProgressAllReady {
			spinner.StopWithError("Failed")
		}
		fmt.Fprintf(os.Stderr, "%s Failed to start cluster: %v\n", ui.ErrorIcon, err)
		os.Exit(1)
	}

	// Success output
	fmt.Printf("%s Cluster '%s' is ready!\n", ui.SuccessIcon, cfg.Name)
	fmt.Println()
	fmt.Println(ui.KeyValue("  CQL Port", fmt.Sprintf("%d", cluster.Info.FirstCQLPort())))
	fmt.Println(ui.KeyValue("  Nodes", fmt.Sprintf("%d", len(cluster.Info.Nodes))))
	if cfg.Authentication {
		fmt.Println(ui.KeyValue("  Username", "cassandra"))
		fmt.Println(ui.KeyValue("  Password", "cassandra"))
	}
	fmt.Println()
	fmt.Printf("  %s\n", ui.Muted.Render(fmt.Sprintf("cqlsh -u cassandra -p cassandra 127.0.0.1 %d", cluster.Info.FirstCQLPort())))
}

func cmdStart(args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	name := fs.String("name", "", "Cluster name")
	_ = fs.Parse(args)

	clusterName := *name

	// If no name provided, prompt for selection from stopped clusters
	if clusterName == "" {
		clusters, err := hydra.ListStoppedClusters()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to list clusters: %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
		if len(clusters) == 0 {
			fmt.Printf("%s No stopped clusters found\n", ui.WarningIcon)
			return
		}
		clusterName, err = ui.SelectCluster("Select cluster to start", clusters)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
	}

	if !hydra.ClusterExists(clusterName) {
		fmt.Printf("%s Cluster '%s' does not exist\n", ui.WarningIcon, clusterName)
		return
	}

	if hydra.IsClusterRunning(clusterName) {
		port, _ := hydra.GetRunningClusterPort(clusterName)
		fmt.Printf("%s Cluster '%s' is already running (port %d)\n", ui.WarningIcon, clusterName, port)
		return
	}

	fmt.Printf("%s Starting cluster '%s'...\n", ui.SpinnerIcon, clusterName)

	ctx := context.Background()

	// Set up spinner for progress updates
	spinner := ui.NewSpinner()
	var lastEvent hydra.ProgressEvent

	progressCb := func(event hydra.ProgressEvent) {
		lastEvent = event
		switch event.Type {
		case hydra.ProgressWaiting:
			spinner.Start(event.Message)
		case hydra.ProgressNodeReady:
			spinner.UpdateMessage(event.Message)
		case hydra.ProgressAllReady:
			spinner.StopWithSuccess(event.Message)
		}
	}

	if err := hydra.StartClusterByName(ctx, clusterName, progressCb); err != nil {
		if lastEvent.Type != hydra.ProgressAllReady {
			spinner.StopWithError("Failed")
		}
		fmt.Fprintf(os.Stderr, "%s Failed to start cluster: %v\n", ui.ErrorIcon, err)
		os.Exit(1)
	}

	port, _ := hydra.GetRunningClusterPort(clusterName)
	fmt.Printf("%s Cluster '%s' started (port %d)\n", ui.SuccessIcon, clusterName, port)
}

func cmdStop(args []string) {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	name := fs.String("name", "", "Cluster name")
	_ = fs.Parse(args)

	clusterName := *name

	// If no name provided, prompt for selection from running clusters
	if clusterName == "" {
		clusters, err := hydra.ListRunningClusters()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to list clusters: %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
		if len(clusters) == 0 {
			fmt.Printf("%s No running clusters found\n", ui.WarningIcon)
			return
		}
		clusterName, err = ui.SelectCluster("Select cluster to stop", clusters)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
	}

	if !hydra.ClusterExists(clusterName) {
		fmt.Printf("%s Cluster '%s' does not exist\n", ui.WarningIcon, clusterName)
		return
	}

	if !hydra.IsClusterRunning(clusterName) {
		fmt.Printf("%s Cluster '%s' is not running (use 'destroy' to remove stopped containers)\n", ui.WarningIcon, clusterName)
		return
	}

	fmt.Printf("%s Stopping cluster '%s'...\n", ui.SpinnerIcon, clusterName)

	ctx := context.Background()
	if err := hydra.StopClusterByName(ctx, clusterName); err != nil {
		fmt.Fprintf(os.Stderr, "%s Failed to stop cluster: %v\n", ui.ErrorIcon, err)
		os.Exit(1)
	}

	fmt.Printf("%s Cluster stopped\n", ui.SuccessIcon)
}

func cmdDestroy(args []string) {
	fs := flag.NewFlagSet("destroy", flag.ExitOnError)
	name := fs.String("name", "", "Cluster name")
	force := fs.Bool("force", false, "Skip confirmation prompt")
	_ = fs.Parse(args)

	clusterName := *name

	// If no name provided, prompt for selection from all clusters
	if clusterName == "" {
		clusters, err := hydra.ListClusters()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to list clusters: %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
		if len(clusters) == 0 {
			fmt.Printf("%s No clusters found\n", ui.WarningIcon)
			return
		}
		clusterName, err = ui.SelectCluster("Select cluster to destroy", clusters)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
	}

	// Interactive confirmation unless --force
	if !*force {
		confirm, err := ui.ConfirmDestroy(clusterName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
		if !confirm {
			fmt.Printf("%s Cancelled\n", ui.WarningIcon)
			return
		}
	}

	fmt.Printf("%s Destroying cluster '%s'...\n", ui.SpinnerIcon, clusterName)

	ctx := context.Background()
	if err := hydra.DestroyClusterByName(ctx, clusterName); err != nil {
		fmt.Fprintf(os.Stderr, "%s Failed to destroy cluster: %v\n", ui.ErrorIcon, err)
		os.Exit(1)
	}

	fmt.Printf("%s Cluster destroyed\n", ui.SuccessIcon)
}

func cmdList(args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	_ = fs.Parse(args)

	clusters, err := hydra.ListClusters()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Failed to list clusters: %v\n", ui.ErrorIcon, err)
		os.Exit(1)
	}
	if len(clusters) == 0 {
		fmt.Printf("%s No clusters found\n", ui.WarningIcon)
		return
	}
	for _, clusterName := range clusters {
		printClusterStatus(clusterName)
	}
}

func printClusterStatus(clusterName string) {
	if hydra.IsClusterRunning(clusterName) {
		port, _ := hydra.GetRunningClusterPort(clusterName)
		fmt.Printf("%s %s: %s (port %d)\n", ui.SuccessIcon, clusterName, ui.Success.Render("RUNNING"), port)
	} else if hydra.ClusterExists(clusterName) {
		fmt.Printf("%s %s: %s\n", ui.WarningIcon, clusterName, ui.Warning.Render("STOPPED"))
	} else {
		fmt.Printf("%s %s: %s\n", ui.ErrorIcon, clusterName, ui.Muted.Render("NOT FOUND"))
	}
}

func cmdLogs(args []string) {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	name := fs.String("name", "", "Cluster name")
	_ = fs.Parse(args)

	clusterName := *name

	// If no name provided, prompt for selection from running clusters
	if clusterName == "" {
		clusters, err := hydra.ListRunningClusters()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to list clusters: %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
		if len(clusters) == 0 {
			fmt.Printf("%s No running clusters found\n", ui.WarningIcon)
			return
		}
		clusterName, err = ui.SelectCluster("Select cluster", clusters)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
	}

	ctx := context.Background()
	logs, err := hydra.GetClusterLogs(ctx, clusterName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Failed to get logs: %v\n", ui.ErrorIcon, err)
		os.Exit(1)
	}

	fmt.Println(logs)
}

func cmdSeed(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	name := fs.String("name", "", "Cluster name")
	rows := fs.Int("rows", 100, "Number of rows per table")
	noData := fs.Bool("no-data", false, "Skip inserting sample data (schema only)")
	noServiceLevels := fs.Bool("no-service-levels", false, "Skip creating service levels")
	noUsers := fs.Bool("no-users", false, "Skip creating users and roles")
	_ = fs.Parse(args)

	clusterName := *name

	// If no name provided, prompt for selection from running clusters
	if clusterName == "" {
		clusters, err := hydra.ListRunningClusters()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to list clusters: %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
		if len(clusters) == 0 {
			fmt.Printf("%s No running clusters found\n", ui.WarningIcon)
			return
		}
		clusterName, err = ui.SelectCluster("Select cluster to seed", clusters)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %v\n", ui.ErrorIcon, err)
			os.Exit(1)
		}
	}

	if !hydra.IsClusterRunning(clusterName) {
		fmt.Printf("%s Cluster '%s' is not running\n", ui.WarningIcon, clusterName)
		return
	}

	cfg := hydra.SeedConfig{
		WithData:          !*noData,
		RowsPerTable:      *rows,
		WithServiceLevels: !*noServiceLevels,
		WithUsers:         !*noUsers,
	}

	fmt.Printf("%s Seeding cluster '%s' with kitchen sink data...\n", ui.SpinnerIcon, clusterName)
	fmt.Println()

	// Set up spinner for progress updates
	spinner := ui.NewSpinner()
	var currentStage string

	progressCb := func(progress hydra.SeedProgress) {
		if progress.Stage != currentStage {
			if currentStage != "" {
				spinner.StopWithSuccess(fmt.Sprintf("%s complete", currentStage))
			}
			currentStage = progress.Stage
			spinner.Start(progress.Message)
		} else {
			spinner.UpdateMessage(progress.Message)
		}
	}

	ctx := context.Background()
	if err := hydra.SeedClusterByName(ctx, clusterName, cfg, progressCb); err != nil {
		spinner.StopWithError("Failed")
		fmt.Fprintf(os.Stderr, "%s Failed to seed cluster: %v\n", ui.ErrorIcon, err)
		os.Exit(1)
	}

	if currentStage != "" {
		spinner.StopWithSuccess(fmt.Sprintf("%s complete", currentStage))
	}

	fmt.Println()
	fmt.Printf("%s Cluster '%s' seeded successfully!\n", ui.SuccessIcon, clusterName)
	fmt.Println()
	fmt.Println(ui.Bold.Render("Created:"))
	fmt.Println("  - 3 keyspaces (demo_simple, demo_nts, demo_analytics)")
	fmt.Println("  - 4 user-defined types (address, phone, contact_info, dimensions)")
	fmt.Println("  - 8 tables (users, orders, sensor_readings, page_views, etc.)")
	fmt.Println("  - 4 secondary indexes (including collection index)")
	fmt.Println("  - 2 materialized views")
	if !*noServiceLevels {
		fmt.Println("  - 3 service levels (sl_premium, sl_standard, sl_batch)")
	}
	if !*noUsers {
		fmt.Println("  - 4 users (demo_admin, demo_analyst, demo_reader, demo_app)")
		fmt.Println("  - 2 roles (analytics_role, readonly_role)")
	}
	if !*noData {
		fmt.Printf("  - %d rows of sample data per table\n", *rows)
	}
}

func cmdVersions() {
	fmt.Println(ui.Bold.Render("Available ScyllaDB versions:"))
	fmt.Println()
	fmt.Println(ui.Bold.Render("Open Source:"))
	for _, v := range hydra.OSSVersions() {
		version := string(v)
		tags := ""
		if v == hydra.DefaultVersion {
			tags += ui.Primary.Render(" (default)")
		}
		if v == hydra.Scylla5v4 {
			tags += ui.Highlight.Render(" [LTS]")
		}
		if v == hydra.Scylla6v2 {
			tags += ui.Muted.Render(" [last OSS]")
		}
		fmt.Printf("  %s%s\n", version, tags)
	}
	fmt.Println()
	fmt.Println(ui.Bold.Render("Enterprise:"))
	for _, v := range hydra.EnterpriseVersions() {
		fmt.Printf("  %s\n", v)
	}
}

func parseMultiDC(config string) (map[string]hydra.DatacenterConfig, error) {
	dcs := make(map[string]hydra.DatacenterConfig)

	// Parse format: "dc1:2,dc2:1"
	for _, part := range strings.Split(config, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		dcName, nodesStr, found := strings.Cut(part, ":")
		if !found {
			return nil, fmt.Errorf("invalid format '%s', expected 'dc:nodes'", part)
		}

		nodes, err := strconv.Atoi(strings.TrimSpace(nodesStr))
		if err != nil {
			return nil, fmt.Errorf("invalid node count '%s' in '%s'", nodesStr, part)
		}

		dcs[strings.TrimSpace(dcName)] = hydra.DatacenterConfig{Nodes: nodes}
	}

	if len(dcs) == 0 {
		return nil, fmt.Errorf("no datacenters specified")
	}

	return dcs, nil
}
