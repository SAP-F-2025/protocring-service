package timescale

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// ContinuousAggregateConfig represents configuration for continuous aggregate
type ContinuousAggregateConfig struct {
	ViewName       string
	SourceTable    string
	TimeColumn     string
	Bucket         time.Duration
	SelectQuery    string
	GroupBy        []string
	WithData       bool
	RefreshPolicy  *RefreshPolicyConfig
}

// RefreshPolicyConfig represents refresh policy configuration
type RefreshPolicyConfig struct {
	StartOffset  time.Duration
	EndOffset    time.Duration
	ScheduleInterval time.Duration
}

// CreateContinuousAggregate creates a continuous aggregate (materialized view)
func CreateContinuousAggregate(ctx context.Context, db *sqlx.DB, config ContinuousAggregateConfig) error {
	// Build the CREATE MATERIALIZED VIEW query
	query := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS %s
		WITH (timescaledb.continuous) AS
		SELECT
			time_bucket('%s', %s) AS bucket,
			%s
		FROM %s
		GROUP BY bucket%s
	`,
		config.ViewName,
		formatDuration(config.Bucket),
		config.TimeColumn,
		config.SelectQuery,
		config.SourceTable,
		buildGroupByClause(config.GroupBy),
	)

	if config.WithData {
		query += " WITH DATA"
	} else {
		query += " WITH NO DATA"
	}

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create continuous aggregate: %w", err)
	}

	// Add refresh policy if configured
	if config.RefreshPolicy != nil {
		err = AddRefreshPolicy(ctx, db, config.ViewName, *config.RefreshPolicy)
		if err != nil {
			return fmt.Errorf("failed to add refresh policy: %w", err)
		}
	}

	return nil
}

// AddRefreshPolicy adds a refresh policy to a continuous aggregate
func AddRefreshPolicy(ctx context.Context, db *sqlx.DB, viewName string, policy RefreshPolicyConfig) error {
	query := fmt.Sprintf(`
		SELECT add_continuous_aggregate_policy('%s',
			start_offset => INTERVAL '%s',
			end_offset => INTERVAL '%s',
			schedule_interval => INTERVAL '%s',
			if_not_exists => TRUE
		)
	`,
		viewName,
		formatDuration(policy.StartOffset),
		formatDuration(policy.EndOffset),
		formatDuration(policy.ScheduleInterval),
	)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to add refresh policy: %w", err)
	}

	return nil
}

// RemoveRefreshPolicy removes a refresh policy from a continuous aggregate
func RemoveRefreshPolicy(ctx context.Context, db *sqlx.DB, viewName string) error {
	query := fmt.Sprintf("SELECT remove_continuous_aggregate_policy('%s', if_exists => TRUE)", viewName)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to remove refresh policy: %w", err)
	}

	return nil
}

// RefreshContinuousAggregate manually refreshes a continuous aggregate
func RefreshContinuousAggregate(ctx context.Context, db *sqlx.DB, viewName string, windowStart, windowEnd time.Time) error {
	query := fmt.Sprintf(
		"CALL refresh_continuous_aggregate('%s', $1, $2)",
		viewName,
	)

	_, err := db.ExecContext(ctx, query, windowStart, windowEnd)
	if err != nil {
		return fmt.Errorf("failed to refresh continuous aggregate: %w", err)
	}

	return nil
}

// DropContinuousAggregate drops a continuous aggregate
func DropContinuousAggregate(ctx context.Context, db *sqlx.DB, viewName string, cascade bool) error {
	query := fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s", viewName)

	if cascade {
		query += " CASCADE"
	}

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop continuous aggregate: %w", err)
	}

	return nil
}

// GetContinuousAggregateInfo returns information about a continuous aggregate
func GetContinuousAggregateInfo(ctx context.Context, db *sqlx.DB, viewName string) (map[string]interface{}, error) {
	query := `
		SELECT
			view_name,
			view_owner,
			materialized_only,
			compression_enabled,
			materialization_hypertable_schema,
			materialization_hypertable_name
		FROM timescaledb_information.continuous_aggregates
		WHERE view_name = $1
	`

	var result struct {
		ViewName                       string `db:"view_name"`
		ViewOwner                      string `db:"view_owner"`
		MaterializedOnly               bool   `db:"materialized_only"`
		CompressionEnabled             bool   `db:"compression_enabled"`
		MaterializationHypertableSchema string `db:"materialization_hypertable_schema"`
		MaterializationHypertableName   string `db:"materialization_hypertable_name"`
	}

	err := db.GetContext(ctx, &result, query, viewName)
	if err != nil {
		return nil, fmt.Errorf("failed to get continuous aggregate info: %w", err)
	}

	return map[string]interface{}{
		"view_name":                         result.ViewName,
		"view_owner":                        result.ViewOwner,
		"materialized_only":                 result.MaterializedOnly,
		"compression_enabled":               result.CompressionEnabled,
		"materialization_hypertable_schema": result.MaterializationHypertableSchema,
		"materialization_hypertable_name":   result.MaterializationHypertableName,
	}, nil
}

// ListContinuousAggregates lists all continuous aggregates
func ListContinuousAggregates(ctx context.Context, db *sqlx.DB) ([]map[string]interface{}, error) {
	query := `
		SELECT
			view_name,
			view_owner,
			materialized_only,
			compression_enabled
		FROM timescaledb_information.continuous_aggregates
		ORDER BY view_name
	`

	rows, err := db.QueryxContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list continuous aggregates: %w", err)
	}
	defer rows.Close()

	var aggregates []map[string]interface{}
	for rows.Next() {
		agg := make(map[string]interface{})
		if err := rows.MapScan(agg); err != nil {
			return nil, err
		}
		aggregates = append(aggregates, agg)
	}

	return aggregates, nil
}

// EnableCompressionForContinuousAggregate enables compression for a continuous aggregate
func EnableCompressionForContinuousAggregate(ctx context.Context, db *sqlx.DB, viewName string) error {
	// Get the materialization hypertable name
	info, err := GetContinuousAggregateInfo(ctx, db, viewName)
	if err != nil {
		return err
	}

	hypertableName := info["materialization_hypertable_name"].(string)

	// Enable compression on the materialization hypertable
	query := fmt.Sprintf("ALTER MATERIALIZED VIEW %s SET (timescaledb.compress = true)", viewName)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to enable compression: %w", err)
	}

	// Add compression policy (compress data older than 7 days)
	policyQuery := fmt.Sprintf(
		"SELECT add_compression_policy('%s', INTERVAL '7 days', if_not_exists => TRUE)",
		hypertableName,
	)

	_, err = db.ExecContext(ctx, policyQuery)
	if err != nil {
		return fmt.Errorf("failed to add compression policy: %w", err)
	}

	return nil
}

// Helper functions

func buildGroupByClause(columns []string) string {
	if len(columns) == 0 {
		return ""
	}

	return ", " + strings.Join(columns, ", ")
}
