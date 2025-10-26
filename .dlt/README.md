# DLT Configuration

This directory contains Data Load Tool (DLT) configuration files.

## Configuration Priority

DLT loads configuration in this order (later overrides earlier):
1. `config.toml` - Default values
2. Environment variables - Runtime overrides

## Environment Variable Format

Use double underscores (`__`) to represent nested config sections:

- `SOURCES__AWS_CUR__BUCKET` → `sources.aws_cur.bucket`
- `SOURCES__AZURE_BILLING__PROJECT_ID` → `sources.azure_billing.project_id`
- `DESTINATION__BIGQUERY__LOCATION` → `destination.bigquery.location`

## Local Development

1. Copy `config.toml.example` to `config.toml`
2. Update with your GCP project values
3. DLT will use Application Default Credentials (run `gcloud auth application-default login`)

## Cloud Run Deployment

Environment variables are set in Cloud Run job definition (see Makefile).
No config.toml needed - all values come from environment variables.

## Files

- `config.toml.example` - Template showing structure
- `config.toml` - Local development config (gitignored)
- `secrets.toml` - Empty (we use service account auth, gitignored)
