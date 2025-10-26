# Quickstart: 5-Minute Deployment

Deploy cloud billing pipelines to GCP Cloud Run in 5 minutes.

## Prerequisites

- GCP project with billing enabled
- `gcloud` CLI installed and authenticated
- GCS buckets with AWS/Azure billing data
- BigQuery datasets created

## Quick Deploy

### 1. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your values
nano .env
```

Required values:
- `GCP_PROJECT_ID` - Your GCP project ID
- `AWS_GCS_BUCKET` - GCS bucket with AWS CUR data
- `AWS_DATASET` - BigQuery dataset for AWS data
- `AZURE_GCS_BUCKET` - GCS bucket with Azure billing data
- `AZURE_EXPORT_NAME` - Azure export name
- `AZURE_DATASET` - BigQuery dataset for Azure data

### 2. One-Time Setup

```bash
# Enable GCP APIs and create Artifact Registry
make setup-gcp

# Grant service account permissions
make setup-permissions
```

### 3. Deploy

```bash
# Build image and deploy all pipelines
make deploy
```

This will:
1. Build Docker image and push to Artifact Registry
2. Create Cloud Run jobs for AWS and Azure pipelines
3. Create Cloud Scheduler jobs for daily execution

## Verify Deployment

```bash
# Check deployment status
make status

# Manually trigger AWS pipeline
make run-aws

# View logs
make logs-aws
```

## Update After Code Changes

```bash
# Rebuild and update all jobs
make update
```

## Common Operations

```bash
# Show all available commands
make help

# Run specific pipeline manually
make run-aws
make run-azure

# View logs
make logs-aws
make logs-azure

# Delete everything
make destroy
```

## Scheduling

Default schedules (configurable in `.env`):
- AWS: Daily at 2 AM UTC
- Azure: Daily at 3 AM UTC

Update schedule:
```bash
# Edit .env
AWS_SCHEDULE="0 4 * * *"  # 4 AM UTC

# Redeploy
make deploy-aws
```

## Troubleshooting

**"Permission denied" errors:**
```bash
make setup-permissions
```

**Jobs failing:**
```bash
# Check logs
make logs-aws

# Increase resources in .env
MEMORY=4Gi
CPU=2
TIMEOUT=2h

# Redeploy
make deploy
```

**Configuration not taking effect:**
- Verify `.env` values are correct
- Redeploy: `make deploy`
- Environment variables override .dlt/config.toml

## Next Steps

- Read [DEPLOYMENT.md](DEPLOYMENT.md) for detailed documentation
- See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how it works
- Customize schedules, resources, or dataset names in `.env`
