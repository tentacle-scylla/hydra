package hydra

import (
	"bufio"
	"context"
	"embed"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

//go:embed seed/*.cql
var seedFiles embed.FS

// CQL file execution order
var cqlFileOrder = []string{
	"seed/00_keyspaces.cql",
	"seed/01_types.cql",
	"seed/02_tables.cql",
	"seed/03_counter_tables.cql",
	"seed/04_indexes.cql",
	"seed/05_materialized_views.cql",
}

// Optional CQL files (may fail on some ScyllaDB versions)
var optionalCQLFiles = []string{
	"seed/06_service_levels.cql",
	"seed/07_users_roles.cql",
}

// Data file
var dataCQLFile = "seed/08_sample_data.cql"

// SeedConfig configures the kitchen sink seeding
type SeedConfig struct {
	// Whether to create sample data (not just schema)
	WithData bool
	// Number of rows to insert per table (default 100) - for generated data
	RowsPerTable int
	// Whether to create service levels (requires ScyllaDB Enterprise or OSS 6.x+)
	WithServiceLevels bool
	// Whether to create additional users/roles
	WithUsers bool
}

// DefaultSeedConfig returns default seeding configuration
func DefaultSeedConfig() SeedConfig {
	return SeedConfig{
		WithData:          true,
		RowsPerTable:      100,
		WithServiceLevels: true,
		WithUsers:         true,
	}
}

// SeedProgress reports seeding progress
type SeedProgress struct {
	Stage   string // "keyspaces", "types", "tables", "indexes", "views", "service_levels", "users", "data"
	Message string
	Current int
	Total   int
}

// SeedProgressCallback is called during seeding to report progress
type SeedProgressCallback func(progress SeedProgress)

// Seed populates the cluster with a comprehensive set of ScyllaDB features
func (c *Cluster) Seed(ctx context.Context, cfg SeedConfig, progressCb SeedProgressCallback) error {
	session, err := c.Session()
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	return SeedSession(ctx, session, cfg, progressCb)
}

// SeedSession populates an existing session with kitchen sink data
func SeedSession(ctx context.Context, session *gocql.Session, cfg SeedConfig, progressCb SeedProgressCallback) error {
	report := func(p SeedProgress) {
		if progressCb != nil {
			progressCb(p)
		}
	}

	// Execute required schema files
	for _, file := range cqlFileOrder {
		stage := getStageFromFile(file)
		report(SeedProgress{Stage: stage, Message: fmt.Sprintf("Executing %s...", file)})

		if err := executeCQLFile(session, file); err != nil {
			return fmt.Errorf("failed to execute %s: %w", file, err)
		}

		report(SeedProgress{Stage: stage, Message: fmt.Sprintf("Completed %s", file)})
	}

	// Execute optional files (service levels, users)
	for _, file := range optionalCQLFiles {
		stage := getStageFromFile(file)

		// Skip based on config
		if strings.Contains(file, "service_levels") && !cfg.WithServiceLevels {
			report(SeedProgress{Stage: stage, Message: "Skipping service levels (disabled)"})
			continue
		}
		if strings.Contains(file, "users") && !cfg.WithUsers {
			report(SeedProgress{Stage: stage, Message: "Skipping users/roles (disabled)"})
			continue
		}

		report(SeedProgress{Stage: stage, Message: fmt.Sprintf("Executing %s...", file)})

		if err := executeCQLFile(session, file); err != nil {
			// Optional files can fail - log but continue
			report(SeedProgress{Stage: stage, Message: fmt.Sprintf("Skipped %s: %v", file, err)})
			continue
		}

		report(SeedProgress{Stage: stage, Message: fmt.Sprintf("Completed %s", file)})
	}

	// Execute sample data if enabled
	if cfg.WithData {
		report(SeedProgress{Stage: "data", Message: "Inserting sample data..."})

		if err := executeCQLFile(session, dataCQLFile); err != nil {
			return fmt.Errorf("failed to execute sample data: %w", err)
		}

		// Generate additional random data based on RowsPerTable
		if cfg.RowsPerTable > 0 {
			report(SeedProgress{Stage: "data", Message: "Generating additional data..."})
			if err := generateAdditionalData(session, cfg.RowsPerTable, func(current, total int) {
				report(SeedProgress{
					Stage:   "data",
					Message: fmt.Sprintf("Generating data (%d/%d)...", current, total),
					Current: current,
					Total:   total,
				})
			}); err != nil {
				return fmt.Errorf("failed to generate additional data: %w", err)
			}
		}

		report(SeedProgress{Stage: "data", Message: "Sample data inserted"})
	}

	return nil
}

// getStageFromFile extracts the stage name from a CQL file path
func getStageFromFile(file string) string {
	switch {
	case strings.Contains(file, "keyspaces"):
		return "keyspaces"
	case strings.Contains(file, "types"):
		return "types"
	case strings.Contains(file, "tables"):
		return "tables"
	case strings.Contains(file, "counter"):
		return "tables"
	case strings.Contains(file, "indexes"):
		return "indexes"
	case strings.Contains(file, "views"):
		return "views"
	case strings.Contains(file, "service_levels"):
		return "service_levels"
	case strings.Contains(file, "users"):
		return "users"
	case strings.Contains(file, "data"):
		return "data"
	default:
		return "schema"
	}
}

// executeCQLFile reads and executes all statements from a CQL file
func executeCQLFile(session *gocql.Session, filename string) error {
	content, err := seedFiles.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	statements := parseCQLStatements(string(content))

	for _, stmt := range statements {
		if stmt == "" {
			continue
		}

		if err := session.Query(stmt).Exec(); err != nil {
			// Include the statement in the error for debugging
			return fmt.Errorf("failed to execute statement: %w\nStatement: %s", err, truncateStatement(stmt))
		}
	}

	return nil
}

// parseCQLStatements parses a CQL file into individual statements
// Handles multi-line statements and comments
func parseCQLStatements(content string) []string {
	var statements []string
	var currentStmt strings.Builder

	scanner := bufio.NewScanner(strings.NewReader(content))
	inStatement := false

	for scanner.Scan() {
		line := scanner.Text()

		// Skip empty lines and comments when not in a statement
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "--") {
			if !inStatement {
				continue
			}
			// Inside a statement, preserve structure but skip comment-only lines
			if strings.HasPrefix(trimmedLine, "--") {
				continue
			}
		}

		// Check if this line starts a statement
		if !inStatement && trimmedLine != "" && !strings.HasPrefix(trimmedLine, "--") {
			inStatement = true
		}

		if inStatement {
			// Remove inline comments (be careful with strings)
			cleanLine := removeInlineComments(line)
			currentStmt.WriteString(cleanLine)
			currentStmt.WriteString("\n")

			// Check if statement ends with semicolon (outside of strings)
			if strings.HasSuffix(strings.TrimSpace(cleanLine), ";") {
				stmt := strings.TrimSpace(currentStmt.String())
				// Remove trailing semicolon for gocql
				stmt = strings.TrimSuffix(stmt, ";")
				stmt = strings.TrimSpace(stmt)
				if stmt != "" {
					statements = append(statements, stmt)
				}
				currentStmt.Reset()
				inStatement = false
			}
		}
	}

	// Handle any remaining statement without trailing semicolon
	if currentStmt.Len() > 0 {
		stmt := strings.TrimSpace(currentStmt.String())
		stmt = strings.TrimSuffix(stmt, ";")
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

// removeInlineComments removes -- comments from a line while preserving strings
func removeInlineComments(line string) string {
	// Simple approach: find -- that's not inside single quotes
	inString := false
	for i := 0; i < len(line); i++ {
		if line[i] == '\'' {
			inString = !inString
		} else if !inString && i < len(line)-1 && line[i] == '-' && line[i+1] == '-' {
			return line[:i]
		}
	}
	return line
}

// truncateStatement truncates a statement for error messages
func truncateStatement(stmt string) string {
	if len(stmt) > 200 {
		return stmt[:200] + "..."
	}
	return stmt
}

// generateAdditionalData generates random data rows
func generateAdditionalData(session *gocql.Session, rowCount int, progress func(current, total int)) error {
	total := rowCount * 3 // specimens, observations, sensor readings
	current := 0

	speciesIDs := []string{
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
		"33333333-3333-3333-3333-333333333333",
		"44444444-4444-4444-4444-444444444444",
		"55555555-5555-5555-5555-555555555555",
	}

	sensorIDs := []string{
		"10000000-1000-1000-1000-100000000001",
		"10000000-1000-1000-1000-100000000002",
		"10000000-1000-1000-1000-100000000003",
	}

	healthStatuses := []string{"healthy", "recovering", "under_observation", "critical", "unknown"}
	nicknames := []string{"Inky", "Blinky", "Pinky", "Clyde", "Otto", "Tentacles", "Kraken Jr", "Squish", "Jet", "Coral"}

	// Generate specimens
	for i := 0; i < rowCount; i++ {
		speciesID := speciesIDs[i%len(speciesIDs)]
		captureDate := time.Now().AddDate(0, 0, -i%365).Format("2006-01-02")
		nickname := nicknames[i%len(nicknames)]
		if i >= len(nicknames) {
			nickname = fmt.Sprintf("%s-%d", nickname, i/len(nicknames))
		}

		err := session.Query(`
			INSERT INTO ocean_catalog.specimens (
				species_id, specimen_id, tag_id, nickname, capture_date,
				health_status, registered_at, registered_by
			) VALUES (?, now(), ?, ?, ?, ?, toTimestamp(now()), ?)`,
			speciesID,
			fmt.Sprintf("TAG-%05d", i+1),
			nickname,
			captureDate,
			healthStatuses[i%len(healthStatuses)],
			"System Seeder",
		).Exec()
		if err != nil {
			return fmt.Errorf("failed to insert specimen: %w", err)
		}

		current++
		progress(current, total)
	}

	// Generate sensor readings
	for i := 0; i < rowCount; i++ {
		sensorID := sensorIDs[i%len(sensorIDs)]
		readingTime := time.Now().Add(-time.Duration(i) * time.Minute)
		bucket := readingTime.Format("2006-01-02")

		err := session.Query(`
			INSERT INTO marine_monitoring.sensor_readings (
				sensor_id, bucket, reading_time,
				water_quality, pressure_bar, light_level_lux, battery_percent
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			sensorID,
			bucket,
			readingTime,
			map[string]interface{}{
				"temperature_celsius":  15.0 + float64(i%10),
				"salinity_ppt":         35.0 + float64(i%5)*0.1,
				"ph_level":             float32(8.0 + float64(i%3)*0.1),
				"dissolved_oxygen_mgl": float32(6.0 + float64(i%4)*0.5),
				"turbidity_ntu":        float32(i % 10),
				"chlorophyll_ugl":      float32(i%5) * 0.5,
			},
			1.0+float64(i%50)*0.1,
			100+i%1000,
			int8(80+i%20),
		).Exec()
		if err != nil {
			return fmt.Errorf("failed to insert sensor reading: %w", err)
		}

		current++
		progress(current, total)
	}

	// Generate counter updates for page views
	pages := []string{"/species", "/specimens", "/habitats", "/facilities", "/expeditions", "/sensors", "/about"}
	for i := 0; i < rowCount; i++ {
		page := pages[i%len(pages)]
		date := time.Now().AddDate(0, 0, -i%30).Format("2006-01-02")
		hour := int8(i % 24)

		err := session.Query(`
			UPDATE ocean_analytics.page_views
			SET page_views = page_views + ?,
				unique_visitors = unique_visitors + ?
			WHERE page_path = ? AND view_date = ? AND hour = ?`,
			int64(100+i%500),
			int64(50+i%200),
			page,
			date,
			hour,
		).Exec()
		if err != nil {
			return fmt.Errorf("failed to update page views: %w", err)
		}

		current++
		progress(current, total)
	}

	return nil
}

// SeedClusterByName seeds a running cluster by name
func SeedClusterByName(ctx context.Context, name string, cfg SeedConfig, progressCb SeedProgressCallback) error {
	// Get port of running cluster
	port, err := GetRunningClusterPort(name)
	if err != nil {
		return fmt.Errorf("failed to get cluster port: %w", err)
	}

	// Create a gocql session
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Port = port
	cluster.Timeout = 60 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer session.Close()

	return SeedSession(ctx, session, cfg, progressCb)
}
