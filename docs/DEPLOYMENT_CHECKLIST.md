# Deployment Checklist

Use this checklist when deploying to a new GCP project.

## Pre-Deployment

- [ ] GCP project created with billing enabled
- [ ] `gcloud` CLI installed and authenticated
- [ ] GCS buckets created with billing data
- [ ] BigQuery datasets created
- [ ] `.env` file configured with project values

## Deployment Steps

- [ ] `make setup-gcp` completed successfully
- [ ] `make setup-permissions` completed successfully
- [ ] `make deploy` completed without errors
- [ ] `make status` shows both jobs and schedulers

## Verification

- [ ] Manually trigger AWS pipeline: `make run-aws`
- [ ] Check logs: `make logs-aws`
- [ ] Verify data loaded in BigQuery
- [ ] Manually trigger Azure pipeline: `make run-azure`
- [ ] Check logs: `make logs-azure`
- [ ] Verify data loaded in BigQuery

## Ongoing Operations

- [ ] Monitor scheduled executions
- [ ] Set up alerting for failures (optional)
- [ ] Document customer-specific configuration
