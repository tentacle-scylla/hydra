package hydra

import (
	"context"
	"fmt"
	"strings"

	"github.com/moby/moby/client"
)

// DockerClient wraps the Docker SDK for image operations with progress
type DockerClient struct {
	cli        *client.Client
	progressCb ImagePullCallback
}

// ImagePullEvent represents a simplified pull event for callbacks
type ImagePullEvent struct {
	ImageName string
	Status    string // "pulling", "downloading", "extracting", "complete", "error", "cached"
	Percent   int
	Message   string
	Error     error
	// Aggregated layer counts for stable UI display
	LayersDownloading int // Currently downloading
	LayersDownloaded  int // Downloaded, waiting to extract
	LayersExtracting  int // Currently extracting
	LayersComplete    int // Fully done (extracted or cached)
}

// ImagePullCallback is called during image pull operations
type ImagePullCallback func(event ImagePullEvent)

// NewDockerClient creates a new Docker client
func NewDockerClient() (*DockerClient, error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	return &DockerClient{cli: cli}, nil
}

// SetPullCallback sets the callback for image pull progress
func (d *DockerClient) SetPullCallback(cb ImagePullCallback) {
	d.progressCb = cb
}

func (d *DockerClient) reportPull(event ImagePullEvent) {
	if d.progressCb != nil {
		d.progressCb(event)
	}
}

// Close closes the Docker client
func (d *DockerClient) Close() error {
	return d.cli.Close()
}

// ImageExists checks if an image exists locally
func (d *DockerClient) ImageExists(ctx context.Context, imageName string) (bool, error) {
	_, err := d.cli.ImageInspect(ctx, imageName)
	if err != nil {
		// Check if it's a "not found" error
		if strings.Contains(err.Error(), "No such image") ||
			strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// layerState tracks the current state of a layer during pull
type layerState int

const (
	layerStateDownloading layerState = iota
	layerStateDownloaded
	layerStateExtracting
	layerStateComplete
)

// PullImage pulls a Docker image with progress reporting
func (d *DockerClient) PullImage(ctx context.Context, imageName string) error {
	d.reportPull(ImagePullEvent{
		ImageName: imageName,
		Status:    "pulling",
		Message:   fmt.Sprintf("Pulling image %s...", imageName),
	})

	response, err := d.cli.ImagePull(ctx, imageName, client.ImagePullOptions{})
	if err != nil {
		d.reportPull(ImagePullEvent{
			ImageName: imageName,
			Status:    "error",
			Message:   fmt.Sprintf("Failed to pull image: %v", err),
			Error:     err,
		})
		return fmt.Errorf("failed to pull image %s: %w", imageName, err)
	}
	defer func() { _ = response.Close() }()

	// Track layer progress for percentage calculation
	layerProgress := make(map[string]struct {
		current int64
		total   int64
	})

	// Track layer states for aggregated counts
	layerStates := make(map[string]layerState)

	for msg, err := range response.JSONMessages(ctx) {
		if err != nil {
			d.reportPull(ImagePullEvent{
				ImageName: imageName,
				Status:    "error",
				Message:   fmt.Sprintf("Error reading pull output: %v", err),
				Error:     err,
			})
			return fmt.Errorf("error reading pull output: %w", err)
		}

		if msg.Error != nil {
			d.reportPull(ImagePullEvent{
				ImageName: imageName,
				Status:    "error",
				Message:   msg.Error.Message,
				Error:     fmt.Errorf("%s", msg.Error.Message),
			})
			return fmt.Errorf("pull error: %s", msg.Error.Message)
		}

		// Track layer progress for percentage
		if msg.ID != "" && msg.Progress != nil {
			layerProgress[msg.ID] = struct {
				current int64
				total   int64
			}{
				current: msg.Progress.Current,
				total:   msg.Progress.Total,
			}
		}

		// Update layer state based on Docker status
		if msg.ID != "" {
			switch {
			case strings.Contains(msg.Status, "Downloading"):
				layerStates[msg.ID] = layerStateDownloading
			case strings.Contains(msg.Status, "Download complete"):
				layerStates[msg.ID] = layerStateDownloaded
			case strings.Contains(msg.Status, "Extracting"):
				layerStates[msg.ID] = layerStateExtracting
			case strings.Contains(msg.Status, "Pull complete"), strings.Contains(msg.Status, "Already exists"):
				layerStates[msg.ID] = layerStateComplete
			}
		}

		// Calculate overall progress percentage
		var totalCurrent, totalSize int64
		for _, lp := range layerProgress {
			if lp.total > 0 {
				totalCurrent += lp.current
				totalSize += lp.total
			}
		}

		percent := 0
		if totalSize > 0 {
			percent = int(float64(totalCurrent) / float64(totalSize) * 100)
		}

		// Count layers in each state
		var downloading, downloaded, extracting, complete int
		for _, state := range layerStates {
			switch state {
			case layerStateDownloading:
				downloading++
			case layerStateDownloaded:
				downloaded++
			case layerStateExtracting:
				extracting++
			case layerStateComplete:
				complete++
			}
		}

		// Determine overall status based on what's happening
		status := "pulling"
		if downloading > 0 {
			status = "downloading"
		}
		if extracting > 0 {
			status = "extracting"
		}

		d.reportPull(ImagePullEvent{
			ImageName:         imageName,
			Status:            status,
			Percent:           percent,
			Message:           msg.Status,
			LayersDownloading: downloading,
			LayersDownloaded:  downloaded,
			LayersExtracting:  extracting,
			LayersComplete:    complete,
		})
	}

	// Count final layer states
	var complete int
	for _, state := range layerStates {
		if state == layerStateComplete {
			complete++
		}
	}

	d.reportPull(ImagePullEvent{
		ImageName:      imageName,
		Status:         "complete",
		Percent:        100,
		Message:        fmt.Sprintf("Successfully pulled %s", imageName),
		LayersComplete: complete,
	})

	return nil
}


// PullImageIfNotExists pulls an image only if it doesn't exist locally
func (d *DockerClient) PullImageIfNotExists(ctx context.Context, imageName string) (bool, error) {
	exists, err := d.ImageExists(ctx, imageName)
	if err != nil {
		return false, err
	}

	if exists {
		d.reportPull(ImagePullEvent{
			ImageName: imageName,
			Status:    "cached",
			Percent:   100,
			Message:   fmt.Sprintf("Image %s already exists locally", imageName),
		})
		return false, nil // Didn't need to pull
	}

	return true, d.PullImage(ctx, imageName)
}

// Ping checks if Docker daemon is accessible
func (d *DockerClient) Ping(ctx context.Context) error {
	_, err := d.cli.Ping(ctx, client.PingOptions{})
	return err
}
