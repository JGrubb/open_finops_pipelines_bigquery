# Open FinOps Pipelines - BigQuery

Cloud billing data ingestion pipelines for AWS and Azure, loading from GCS into BigQuery.

## Overview

This project provides scheduled data pipelines that ingest multi-cloud billing data into BigQuery using manifest-based loading strategies. Built with DLT for state management and BigQuery native LOAD operations for performance.

### Supported Platforms

- **AWS Cost and Usage Reports (CUR)** - v1 format
- **Azure Cost Management Exports**

### Key Features

- **Manifest-based loading** - Tracks execution IDs to avoid duplicate loads
- **Schema evolution** - Handles dynamic schema changes in AWS billing data
- **Incremental processing** - Loads only new billing periods
- **Monthly regeneration** - Replaces entire months when source data updates
- **Zero-copy ingestion** - BigQuery loads directly from GCS

## Architecture

### Data Flow

1. **Manifest Discovery** - Scans GCS for billing manifest files
2. **Execution Tracking** - Checks if execution ID already loaded
3. **Schema Management** - Adapts to column additions and type changes
4. **Native Load** - BigQuery LOAD jobs read directly from GCS
5. **State Persistence** - DLT tracks loaded executions

### Design Decisions

**Partitioning & Clustering:**
- Partition by billing period start date (monthly)
- Cluster by usage start date

**Load Strategy:**
- Process most recent months first (newer manifests have better type information)
- Drop and reload entire months on regeneration (faster than deduplication)
- Support both CSV and Parquet formats

**AWS CUR Specifics:**
- Each execution represents a versioned billing dataset
- Schema can change between months
- Manifest contains column definitions and data types (v1.0+)

**Azure Specifics:**
- Stable schema across exports
- Manifest-based execution tracking

## Deployment

This project deploys to GCP Cloud Run as scheduled jobs.

### Quick Deploy

```bash
# 1. Configure
cp .env.example .env
# Edit .env with your GCP project and bucket details

# 2. Deploy
make setup-gcp
make setup-permissions
make deploy
```

See [QUICKSTART.md](QUICKSTART.md) for detailed instructions.

### Architecture

- **DLT** handles state management and deduplication
- **BigQuery native LOAD** ingests data directly from GCS (zero data copying)
- **Cloud Run Jobs** execute pipelines on schedule
- **Cloud Scheduler** triggers daily at configurable times

### Configuration

All configuration uses environment variables for customer-agnostic deployment:

- Copy `.env.example` to `.env`
- Set your GCP project, buckets, and dataset names
- Deploy with `make deploy`

See [DEPLOYMENT.md](DEPLOYMENT.md) for advanced configuration.

## Development

```bash
# Install dependencies
uv sync

# Run locally (requires .dlt/config.toml)
uv run python main.py aws
uv run python main.py azure

# Run tests
uv run pytest
```