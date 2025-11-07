# Protocring Service

A modern Go microservice for **real-time violation detection and proctoring** using MediaPipe, optimized for **time-series data** with TimescaleDB and **event streaming** with Redis Streams.

## Features

### Core Functionality
- **Violation Detection System**: Real-time proctoring violation detection and analysis
  - MediaPipe integration for face, hand, and pose detection
  - Automated violation classification and severity assessment
  - Comprehensive analytics and reporting

### Technical Stack
- **Dependency Injection**: Uber-FX for clean dependency management
- **Web Framework**: Gin for high-performance HTTP routing
- **Interface-Based Design**: Repository and Service patterns for testability
- **Time-Series Database**:
  - **TimescaleDB** (PostgreSQL extension) with sqlx
  - Hypertables with 1-day chunks for optimal performance
  - Continuous Aggregates for pre-computed analytics
  - Automatic compression (90-95% storage savings after 7 days)
  - Data retention policies (90-day automatic cleanup)
- **Event Streaming**:
  - **Redis Streams** for message queue and event-driven architecture
  - Consumer Groups with at-least-once delivery
- **Configuration**: Viper for flexible configuration management
- **Logging**: Zap for structured, high-performance logging
- **Testing**: Comprehensive unit tests with mock repositories

## Project Structure

```
protocring-service/
├── cmd/server/               # Application entrypoint
│   └── main.go
├── internal/
│   ├── config/               # Configuration management
│   ├── handler/              # HTTP handlers (Gin routes)
│   │   ├── health.go         # Health check endpoints
│   │   ├── violation.go      # Violation API endpoints
│   │   └── routes.go         # Route registration
│   ├── service/              # Business logic layer
│   │   ├── health.go         # Health service
│   │   ├── violation.go      # Violation processing service
│   │   ├── violation_test.go # Service unit tests
│   │   └── module.go         # FX module
│   ├── repository/           # Data access layer (sqlx)
│   │   ├── interfaces.go     # Repository interfaces
│   │   ├── violation.go      # Violation repository implementation
│   │   ├── mocks/            # Mock repositories for testing
│   │   └── module.go         # FX module
│   ├── model/                # Domain models
│   │   └── violation.go      # Violation detection models
│   ├── dto/                  # Data Transfer Objects
│   │   └── violation.go      # Violation API DTOs
│   └── timescale/            # TimescaleDB helpers
│       ├── hypertable.go              # Hypertable operations
│       └── continuous_aggregate.go    # Continuous aggregates
├── pkg/
│   ├── database/             # Database connections
│   │   ├── sqlx.go           # TimescaleDB connection
│   │   └── redis.go          # Redis connection
│   ├── streams/              # Redis Streams utilities
│   │   ├── producer.go       # Stream producer
│   │   └── consumer.go       # Stream consumer
│   └── server/               # HTTP server setup
├── migrations/               # Database migrations
│   ├── 000001_enable_timescaledb.up.sql
│   ├── 000002_create_violations_table.up.sql
│   └── 000003_create_violation_aggregates.up.sql
├── examples/                 # Code examples
├── docker-compose.yml        # TimescaleDB + Redis
├── API_GUIDE.md             # Comprehensive API documentation
├── INTERFACES_AND_TESTING.md # Testing guide
└── TIMESCALE_REDIS_GUIDE.md # TimescaleDB & Redis guide
```

## Prerequisites

- Go 1.21 or higher
- Docker and Docker Compose (for local development)

## Getting Started

### 1. Clone the repository

```bash
git clone <repository-url>
cd protocring-service
```

### 2. Start dependencies

Start PostgreSQL and Redis using Docker Compose:

```bash
docker-compose up -d
```

### 3. Configure the application

Copy the example config file:

```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml` if needed for your local setup.

### 4. Run migrations

```bash
make migrate-up
```

### 5. Run the application

```bash
go run cmd/server/main.go
```

The server will start on `http://localhost:8080`

## Quick Examples

### 1. Ingest Violation (HTTP API)

```bash
curl -X POST http://localhost:8080/api/v1/violations \
  -H "Content-Type: application/json" \
  -d '{
    "attempt_id": 12345,
    "violation_type": "looking_away",
    "severity": "high",
    "confidence_score": 0.95,
    "detection_data": {
      "faces": [...],
      "analysis": {...}
    },
    "frame_metadata": {...},
    "browser_info": {...},
    "client_timestamp": "2025-10-31T10:00:00Z"
  }'
```

### 2. Get Violation Analytics

```bash
curl "http://localhost:8080/api/v1/violations/analytics/12345?bucket_size=5m"
```

Response:
```json
{
  "total_count": 25,
  "count_by_type": {
    "looking_away": 10,
    "hand_detected": 5,
    "mouth_open": 8
  },
  "severity_distribution": {
    "critical": 2,
    "high": 8,
    "medium": 10,
    "low": 5
  },
  "timeline": [...]
}
```

### 3. Using Repository Pattern in Code

```go
// Service layer uses interface
type ViolationService struct {
    repo repository.ViolationRepositoryInterface
}

func (s *ViolationService) IngestViolation(ctx context.Context, req *dto.CreateViolationRequest) error {
    violation := convertToModel(req)
    return s.repo.Insert(ctx, violation)
}

// Easy to test with mocks
func TestViolationService(t *testing.T) {
    mockRepo := mocks.NewMockViolationRepository()
    service := NewViolationService(mockRepo, logger)

    err := service.IngestViolation(ctx, testRequest)
    // assertions...
}
```

### 4. TimescaleDB Continuous Aggregates

```sql
-- Query pre-computed hourly stats (instant results)
SELECT * FROM violation_stats_hourly
WHERE bucket >= NOW() - INTERVAL '24 hours'
ORDER BY bucket DESC;

-- Query per-attempt summary
SELECT * FROM violation_attempt_summary
WHERE attempt_id = 12345;
```

**Full documentation:**
- **[API_GUIDE.md](./API_GUIDE.md)** - Complete API reference
- **[INTERFACES_AND_TESTING.md](./INTERFACES_AND_TESTING.md)** - Testing guide
- **[TIMESCALE_REDIS_GUIDE.md](./TIMESCALE_REDIS_GUIDE.md)** - TimescaleDB & Redis guide

## API Endpoints

### Health Check

```bash
# Health check with dependency status
GET /health

# Readiness check
GET /ready
```

### Violation API

```bash
# Ingest single violation
POST /api/v1/violations

# Ingest batch (up to 50 violations)
POST /api/v1/violations/batch

# Get violations for an attempt
GET /api/v1/violations/attempt/:attempt_id?limit=20&offset=0

# Get latest violation for an attempt
GET /api/v1/violations/attempt/:attempt_id/latest

# Get violation analytics
GET /api/v1/violations/analytics/:attempt_id?bucket_size=5m
```

**See full API documentation:** [API_GUIDE.md](./API_GUIDE.md)

## Development

### Install dependencies

```bash
go mod download
```

### Run tests

```bash
go test ./...
```

### Build

```bash
go build -o bin/server cmd/server/main.go
```

## Configuration

The application can be configured using:

1. **config.yaml file**: Place in the project root or `./config` directory
2. **Environment variables**: Override any config value (uppercase with underscores)

Example environment variables:

```bash
export SERVER_PORT=8080
export DATABASE_HOST=localhost
export REDIS_HOST=localhost
```

## Docker Support

### Build Docker image

```bash
docker build -t protocring-service .
```

### Run with Docker Compose

```bash
docker-compose up
```

## Technologies

- [Uber-FX](https://uber-go.github.io/fx/) - Dependency injection framework
- [Gin](https://gin-gonic.com/) - Web framework
- [TimescaleDB](https://www.timescale.com/) - Time-series database (PostgreSQL extension)
- [sqlx](https://github.com/jmoiron/sqlx) - SQL toolkit
- [Redis Streams](https://redis.io/docs/data-types/streams/) - Event streaming & message queue
- [go-redis](https://github.com/redis/go-redis) - Redis client
- [Viper](https://github.com/spf13/viper) - Configuration management
- [Zap](https://github.com/uber-go/zap) - Structured logging

## Use Cases

This service is optimized for:

### 1. Online Proctoring & Exam Monitoring
- Real-time violation detection using MediaPipe
- Automated cheating detection (multiple faces, looking away, hands detected)
- Integrity scoring based on violation severity and frequency
- Session analytics and reporting

### 2. Time-Series Data Analysis
- Store millions of detection events per day
- Fast queries with TimescaleDB hypertables
- Automatic data compression and retention
- Pre-computed aggregates for instant analytics

### 3. Real-Time Analytics
- Per-second violation monitoring
- Configurable time buckets (30s, 1m, 5m, 1h)
- Severity distribution analysis
- User behavior pattern detection

### 4. Event Streaming
- Redis Streams for async processing
- Consumer groups for horizontal scaling
- At-least-once delivery guarantees
- Background jobs for video processing, notifications

### 5. Scalable Architecture
- Interface-based design for testability
- Mock repositories for unit testing
- Uber-FX dependency injection
- Clean separation of concerns

## License

MIT
