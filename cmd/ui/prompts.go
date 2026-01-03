package ui

import (
	"fmt"

	"github.com/charmbracelet/huh"
	"github.com/pierre-borckmans/hydra"
)

// UpConfig holds the configuration for the up command
type UpConfig struct {
	Name    string
	Version string
	Nodes   int
	MultiDC string
	Auth    bool
	Timeout int // seconds
}

// PromptUpConfig prompts the user for cluster configuration
func PromptUpConfig() (*UpConfig, error) {
	cfg := &UpConfig{
		Name:    "tentacle-test",
		Auth:    true,
		Timeout: 180,
	}

	// Build version options
	var versionOptions []huh.Option[string]
	for _, v := range hydra.OSSVersions() {
		label := string(v)
		if v == hydra.DefaultVersion {
			label += " (default)"
		}
		if v == hydra.Scylla5v4 {
			label += " [LTS]"
		}
		versionOptions = append(versionOptions, huh.NewOption(label, string(v)))
	}

	// Topology choice
	var topoChoice string
	topoOptions := []huh.Option[string]{
		huh.NewOption("Single node", "single"),
		huh.NewOption("Multi-node (single DC)", "multi"),
		huh.NewOption("Multi-datacenter", "multi-dc"),
	}

	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Cluster name").
				Value(&cfg.Name).
				Placeholder("tentacle-test"),

			huh.NewSelect[string]().
				Title("ScyllaDB version").
				Options(versionOptions...).
				Value(&cfg.Version),

			huh.NewSelect[string]().
				Title("Topology").
				Options(topoOptions...).
				Value(&topoChoice),
		),
	)

	err := form.Run()
	if err != nil {
		return nil, err
	}

	// Handle topology-specific prompts
	switch topoChoice {
	case "single":
		cfg.Nodes = 1
	case "multi":
		var nodeCount int
		nodeOptions := []huh.Option[int]{
			huh.NewOption("3 nodes (recommended)", 3),
			huh.NewOption("5 nodes", 5),
			huh.NewOption("7 nodes", 7),
		}
		err := huh.NewSelect[int]().
			Title("Number of nodes").
			Options(nodeOptions...).
			Value(&nodeCount).
			Run()
		if err != nil {
			return nil, err
		}
		cfg.Nodes = nodeCount
	case "multi-dc":
		err := huh.NewInput().
			Title("Datacenter configuration").
			Description("Format: dc1:nodes,dc2:nodes (e.g., east:2,west:2)").
			Value(&cfg.MultiDC).
			Placeholder("east:2,west:2").
			Run()
		if err != nil {
			return nil, err
		}
	}

	// Auth prompt
	err = huh.NewConfirm().
		Title("Enable authentication?").
		Value(&cfg.Auth).
		Affirmative("Yes").
		Negative("No").
		Run()
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// ConfirmDestroy prompts for confirmation before destroying a cluster
func ConfirmDestroy(clusterName string) (bool, error) {
	var confirm bool
	err := huh.NewConfirm().
		Title(fmt.Sprintf("Destroy cluster '%s'?", clusterName)).
		Description("This will remove all containers and data.").
		Value(&confirm).
		Affirmative("Yes, destroy").
		Negative("Cancel").
		Run()
	return confirm, err
}

// SelectCluster prompts user to select from a list of clusters
func SelectCluster(title string, clusters []string) (string, error) {
	if len(clusters) == 0 {
		return "", fmt.Errorf("no clusters found")
	}

	if len(clusters) == 1 {
		return clusters[0], nil
	}

	var selected string
	var options []huh.Option[string]
	for _, c := range clusters {
		options = append(options, huh.NewOption(c, c))
	}

	err := huh.NewSelect[string]().
		Title(title).
		Options(options...).
		Value(&selected).
		Run()

	return selected, err
}
