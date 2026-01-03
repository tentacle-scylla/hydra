// Package ui provides styling and interactive prompts for the cluster CLI.
package ui

import (
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss"
)

var (
	// Colors
	ColorPrimary   = lipgloss.Color("#7C3AED") // Purple
	ColorCyan      = lipgloss.Color("#06B6D4") // Cyan (ScyllaDB mascot color)
	ColorCyan2     = lipgloss.Color("#56D2E5") // Cyan (ScyllaDB mascot color)
	ColorSuccess   = lipgloss.Color("#10B981") // Green
	ColorWarning   = lipgloss.Color("#F59E0B") // Amber
	ColorError     = lipgloss.Color("#EF4444") // Red
	ColorMuted     = lipgloss.Color("#6B7280") // Gray
	ColorHighlight = lipgloss.Color("#3B82F6") // Blue

	// Text styles
	Bold      = lipgloss.NewStyle().Bold(true)
	Muted     = lipgloss.NewStyle().Foreground(ColorMuted)
	Success   = lipgloss.NewStyle().Foreground(ColorSuccess)
	Warning   = lipgloss.NewStyle().Foreground(ColorWarning)
	Error     = lipgloss.NewStyle().Foreground(ColorError)
	Highlight = lipgloss.NewStyle().Foreground(ColorHighlight)
	Primary   = lipgloss.NewStyle().Foreground(ColorPrimary)
	Cyan      = lipgloss.NewStyle().Foreground(ColorCyan)
	Cyan2     = lipgloss.NewStyle().Foreground(ColorCyan2)

	// Status indicators
	SuccessIcon = Success.Render("✓")
	ErrorIcon   = Error.Render("✗")
	WarningIcon = Warning.Render("!")
	InfoIcon    = Highlight.Render("ℹ")
	SpinnerIcon = Primary.Render("◌")

	// Box styles
	InfoBox = lipgloss.NewStyle().
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(ColorHighlight).
		Padding(0, 1)

	SuccessBox = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(ColorSuccess).
			Padding(0, 1)

	ErrorBox = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(ColorError).
			Padding(0, 1)

	// Title style
	Title = lipgloss.NewStyle().
		Bold(true).
		Foreground(ColorPrimary).
		MarginBottom(1)

	// Key-value styles
	Key   = lipgloss.NewStyle().Foreground(ColorMuted)
	Value = lipgloss.NewStyle().Bold(true)
)

// StatusLine formats a status line with icon and message
func StatusLine(icon, message string) string {
	return icon + " " + message
}

// KeyValue formats a key-value pair
func KeyValue(key, value string) string {
	return Key.Render(key+":") + " " + Value.Render(value)
}

// Section creates a titled section
func Section(title string, content string) string {
	return Bold.Render(title) + "\n" + content
}

// Spinner provides a simple animated spinner
type Spinner struct {
	frames  []string
	current int
	stop    chan struct{}
	done    chan struct{}
	message string
}

// NewSpinner creates a new spinner
func NewSpinner() *Spinner {
	return &Spinner{
		frames: []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"},
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Start begins the spinner animation with the given message
func (s *Spinner) Start(message string) {
	s.message = message
	go func() {
		ticker := time.NewTicker(80 * time.Millisecond)
		defer ticker.Stop()
		defer close(s.done)

		for {
			select {
			case <-s.stop:
				// Clear the line
				fmt.Print("\r\033[K")
				return
			case <-ticker.C:
				frame := Primary.Render(s.frames[s.current])
				fmt.Printf("\r%s %s", frame, s.message)
				s.current = (s.current + 1) % len(s.frames)
			}
		}
	}()
}

// UpdateMessage updates the spinner message
func (s *Spinner) UpdateMessage(message string) {
	s.message = message
}

// Stop stops the spinner
func (s *Spinner) Stop() {
	close(s.stop)
	<-s.done
}

// StopWithSuccess stops the spinner and shows a success message
func (s *Spinner) StopWithSuccess(message string) {
	s.Stop()
	fmt.Printf("%s %s\n", SuccessIcon, message)
}

// StopWithError stops the spinner and shows an error message
func (s *Spinner) StopWithError(message string) {
	s.Stop()
	fmt.Printf("%s %s\n", ErrorIcon, message)
}
