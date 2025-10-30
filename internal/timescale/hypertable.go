package timescale

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// HypertableConfig represents hypertable configuration
type HypertableConfig struct {
	TableName            string
	TimeColumn           string
	ChunkTimeInterval    time.Duration
	PartitioningColumn   *string // Optional space partitioning
	NumPartitions        *int    // Number of space partitions
	CreateDefaultIndexes bool
}

// CreateHypertable converts a regular PostgreSQL table to a TimescaleDB hypertable
func CreateHypertable(ctx context.Context, db *sqlx.DB, config HypertableConfig) error {
	query := fmt.Sprintf(
		"SELECT create_hypertable('%s', '%s', chunk_time_interval => INTERVAL '%s', if_not_exists => TRUE",
		config.TableName,
		config.TimeColumn,
		formatDuration(config.ChunkTimeInterval),
	)

	// Add space partitioning if configured
	if config.PartitioningColumn != nil && config.NumPartitions != nil {
		query += fmt.Sprintf(
			", partitioning_column => '%s', number_partitions => %d",
			*config.PartitioningColumn,
			*config.NumPartitions,
		)
	}

	// Add create_default_indexes option
	if !config.CreateDefaultIndexes {
		query += ", create_default_indexes => FALSE"
	}

	query += ")"

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create hypertable: %w", err)
	}

	return nil
}

// SetChunkTimeInterval changes the chunk time interval for a hypertable
func SetChunkTimeInterval(ctx context.Context, db *sqlx.DB, tableName string, interval time.Duration) error {
	query := fmt.Sprintf(
		"SELECT set_chunk_time_interval('%s', INTERVAL '%s')",
		tableName,
		formatDuration(interval),
	)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to set chunk time interval: %w", err)
	}

	return nil
}

// AddRetentionPolicy adds a retention policy to drop old data automatically
func AddRetentionPolicy(ctx context.Context, db *sqlx.DB, tableName string, retentionPeriod time.Duration) error {
	query := fmt.Sprintf(
		"SELECT add_retention_policy('%s', INTERVAL '%s', if_not_exists => TRUE)",
		tableName,
		formatDuration(retentionPeriod),
	)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to add retention policy: %w", err)
	}

	return nil
}

// RemoveRetentionPolicy removes a retention policy
func RemoveRetentionPolicy(ctx context.Context, db *sqlx.DB, tableName string) error {
	query := fmt.Sprintf("SELECT remove_retention_policy('%s', if_exists => TRUE)", tableName)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to remove retention policy: %w", err)
	}

	return nil
}

// EnableCompression enables compression for a hypertable
func EnableCompression(ctx context.Context, db *sqlx.DB, tableName string, segmentBy []string, orderBy []string) error {
	// First, alter the table to enable compression settings
	alterQuery := fmt.Sprintf("ALTER TABLE %s SET (timescaledb.compress", tableName)

	if len(segmentBy) > 0 {
		alterQuery += fmt.Sprintf(", timescaledb.compress_segmentby = '%s'", joinColumns(segmentBy))
	}

	if len(orderBy) > 0 {
		alterQuery += fmt.Sprintf(", timescaledb.compress_orderby = '%s'", joinColumns(orderBy))
	}

	alterQuery += ")"

	_, err := db.ExecContext(ctx, alterQuery)
	if err != nil {
		return fmt.Errorf("failed to set compression settings: %w", err)
	}

	return nil
}

// AddCompressionPolicy adds automatic compression policy
func AddCompressionPolicy(ctx context.Context, db *sqlx.DB, tableName string, compressAfter time.Duration) error {
	query := fmt.Sprintf(
		"SELECT add_compression_policy('%s', INTERVAL '%s', if_not_exists => TRUE)",
		tableName,
		formatDuration(compressAfter),
	)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to add compression policy: %w", err)
	}

	return nil
}

// RemoveCompressionPolicy removes compression policy
func RemoveCompressionPolicy(ctx context.Context, db *sqlx.DB, tableName string) error {
	query := fmt.Sprintf("SELECT remove_compression_policy('%s', if_exists => TRUE)", tableName)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to remove compression policy: %w", err)
	}

	return nil
}

// CompressChunk manually compresses a specific chunk
func CompressChunk(ctx context.Context, db *sqlx.DB, chunkName string) error {
	query := fmt.Sprintf("SELECT compress_chunk('%s')", chunkName)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to compress chunk: %w", err)
	}

	return nil
}

// DecompressChunk manually decompresses a specific chunk
func DecompressChunk(ctx context.Context, db *sqlx.DB, chunkName string) error {
	query := fmt.Sprintf("SELECT decompress_chunk('%s')", chunkName)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to decompress chunk: %w", err)
	}

	return nil
}

// GetHypertableInfo returns information about a hypertable
func GetHypertableInfo(ctx context.Context, db *sqlx.DB, tableName string) (map[string]interface{}, error) {
	query := `
		SELECT
			h.table_name,
			h.schema_name,
			d.column_name as time_column,
			d.interval_length,
			h.num_dimensions,
			h.compression_state
		FROM timescaledb_information.hypertables h
		JOIN timescaledb_information.dimensions d ON h.hypertable_name = d.hypertable_name
		WHERE h.table_name = $1
	`

	var result struct {
		TableName        string `db:"table_name"`
		SchemaName       string `db:"schema_name"`
		TimeColumn       string `db:"time_column"`
		IntervalLength   int64  `db:"interval_length"`
		NumDimensions    int    `db:"num_dimensions"`
		CompressionState int    `db:"compression_state"`
	}

	err := db.GetContext(ctx, &result, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get hypertable info: %w", err)
	}

	return map[string]interface{}{
		"table_name":        result.TableName,
		"schema_name":       result.SchemaName,
		"time_column":       result.TimeColumn,
		"interval_length":   result.IntervalLength,
		"num_dimensions":    result.NumDimensions,
		"compression_state": result.CompressionState,
	}, nil
}

// ListChunks lists all chunks for a hypertable
func ListChunks(ctx context.Context, db *sqlx.DB, tableName string) ([]map[string]interface{}, error) {
	query := `
		SELECT
			chunk_name,
			range_start,
			range_end,
			is_compressed
		FROM timescaledb_information.chunks
		WHERE hypertable_name = $1
		ORDER BY range_start DESC
	`

	rows, err := db.QueryxContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to list chunks: %w", err)
	}
	defer rows.Close()

	var chunks []map[string]interface{}
	for rows.Next() {
		chunk := make(map[string]interface{})
		if err := rows.MapScan(chunk); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// Helper functions

func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	if hours >= 24 {
		days := hours / 24
		return fmt.Sprintf("%d days", days)
	}
	return fmt.Sprintf("%d hours", hours)
}

func joinColumns(cols []string) string {
	result := ""
	for i, col := range cols {
		if i > 0 {
			result += ", "
		}
		result += col
	}
	return result
}
