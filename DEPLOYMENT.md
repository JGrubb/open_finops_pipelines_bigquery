# Cloud Run Deployment Guide

> **Quick Start:** For fast deployment using automation, see [QUICKSTART.md](QUICKSTART.md)

This guide covers manual deployment of billing pipelines as Cloud Run jobs with daily Cloud Scheduler triggers. For automated deployment, use the Makefile (see QUICKSTART.md).

---

## Automated Deployment (Recommended)

The easiest way to deploy is using the provided Makefile:

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with your values

# 2. One-time setup
make setup-gcp
make setup-permissions

# 3. Deploy
make deploy
```

See [QUICKSTART.md](QUICKSTART.md) for details.

## Manual Deployment

If you prefer manual deployment or need to customize beyond what the Makefile provides, follow the steps below.

---

## Configuration

This project uses environment variables for customer-agnostic configuration.

**Option 1: Makefile (Recommended)**
- Set values in `.env` file
- Use `make deploy` to deploy with environment variables

**Option 2: Manual with Environment Variables**
- Export environment variables before running gcloud commands
- Use format: `SOURCES__AWS_CUR__BUCKET`, `SOURCES__AZURE_BILLING__PROJECT_ID`

**Option 3: Manual with Config File**
- Copy `.dlt/config.toml.example` to `.dlt/config.toml`
- Update with your values
- Include in Docker image build

For open-source deployments, use environment variables to avoid hard-coding customer data.

## Prerequisites

1. Authenticate with GCP:
```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

2. Enable required APIs:
```bash
gcloud services enable \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com
```

3. Create Artifact Registry repository (if not exists):
```bash
gcloud artifacts repositories create finops-pipelines \
  --repository-format=docker \
  --location=us-central1 \
  --description="FinOps billing pipelines"
```

4. Ensure the default compute service account has necessary permissions:
```bash
# Get the service account email
PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)")
SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.jobUser"

# Grant GCS permissions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectViewer"
```

## Build and Push Docker Image

```bash
# Set variables
PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
REPO="finops-pipelines"
IMAGE_NAME="billing-pipeline"
IMAGE_TAG="latest"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

# Build and push using Cloud Build (recommended)
gcloud builds submit --tag ${IMAGE_URI}

# Or build locally and push
docker build -t ${IMAGE_URI} .
docker push ${IMAGE_URI}
```

## Deploy Pipeline Cloud Run Jobs

Generic deployment function:
```bash
deploy_pipeline() {
  local PIPELINE_NAME=$1
  local SCHEDULE=$2
  local MEMORY=${3:-2Gi}
  local CPU=${4:-1}
  local TIMEOUT=${5:-1h}

  PROJECT_ID=$(gcloud config get-value project)
  PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)")
  SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

  # Create Cloud Run job
  gcloud run jobs create ${PIPELINE_NAME}-billing-pipeline \
    --image=${IMAGE_URI} \
    --region=${REGION} \
    --task-timeout=${TIMEOUT} \
    --max-retries=2 \
    --memory=${MEMORY} \
    --cpu=${CPU} \
    --command="uv" \
    --args="run,python,main.py,${PIPELINE_NAME}"

  # Create Cloud Scheduler job
  gcloud scheduler jobs create http ${PIPELINE_NAME}-billing-daily \
    --location=${REGION} \
    --schedule="${SCHEDULE}" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${PIPELINE_NAME}-billing-pipeline:run" \
    --http-method=POST \
    --oauth-service-account-email="${SERVICE_ACCOUNT}"
}
```

### Deploy AWS Pipeline
```bash
deploy_pipeline "aws" "0 2 * * *"  # Daily at 2 AM UTC
```

### Deploy Azure Pipeline
```bash
deploy_pipeline "azure" "0 3 * * *"  # Daily at 3 AM UTC
```

### Deploy with Custom Resources
```bash
# Deploy with 4GB memory, 2 CPUs, 2 hour timeout
deploy_pipeline "aws" "0 2 * * *" "4Gi" "2" "2h"
```

## Update Existing Jobs

When you make code changes:

```bash
PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
REPO="finops-pipelines"
IMAGE_NAME="billing-pipeline"
IMAGE_TAG="latest"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

# Rebuild and push image
gcloud builds submit --tag ${IMAGE_URI}

# Update jobs (replace PIPELINE_NAME with aws, azure, etc)
for PIPELINE in aws azure; do
  gcloud run jobs update ${PIPELINE}-billing-pipeline \
    --image=${IMAGE_URI} \
    --region=${REGION}
done
```

## Manual Execution

Trigger jobs manually for testing:

```bash
REGION="us-central1"

# Run specific pipeline (replace PIPELINE_NAME)
gcloud run jobs execute PIPELINE_NAME-billing-pipeline --region=${REGION}

# Examples:
gcloud run jobs execute aws-billing-pipeline --region=${REGION}
gcloud run jobs execute azure-billing-pipeline --region=${REGION}
```

## Monitoring

View job executions:
```bash
REGION="us-central1"
PIPELINE_NAME="aws"  # or azure, etc

# List executions
gcloud run jobs executions list \
  --job=${PIPELINE_NAME}-billing-pipeline \
  --region=${REGION}

# View logs for specific job
gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=${PIPELINE_NAME}-billing-pipeline" \
  --limit=100 \
  --format=json
```

View in Cloud Console:
```bash
PROJECT_ID=$(gcloud config get-value project)
echo "Jobs: https://console.cloud.google.com/run/jobs?project=${PROJECT_ID}"
echo "Scheduler: https://console.cloud.google.com/cloudscheduler?project=${PROJECT_ID}"
echo "Logs: https://console.cloud.google.com/logs/query?project=${PROJECT_ID}"
```

## DLT State Management

DLT automatically stores state in BigQuery tables within each dataset:
- `{dataset}._dlt_loads`
- `{dataset}._dlt_pipeline_state`

These tables track which manifest executions have been loaded to prevent duplicates.

## Troubleshooting

### Job fails with authentication error
Verify service account has required permissions:
```bash
PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)")
SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud projects get-iam-policy ${PROJECT_ID} \
  --flatten="bindings[].members" \
  --filter="bindings.members:${SERVICE_ACCOUNT}"
```

### Job times out
Increase timeout (max 24h):
```bash
REGION="us-central1"
PIPELINE_NAME="aws"

gcloud run jobs update ${PIPELINE_NAME}-billing-pipeline \
  --task-timeout=2h \
  --region=${REGION}
```

### Need more memory/CPU
Update resources:
```bash
REGION="us-central1"
PIPELINE_NAME="aws"

gcloud run jobs update ${PIPELINE_NAME}-billing-pipeline \
  --memory=4Gi \
  --cpu=2 \
  --region=${REGION}
```

### Test locally with your GCP credentials
Run the container locally:
```bash
docker run --rm \
  -v ~/.config/gcloud:/root/.config/gcloud \
  -e CLOUDSDK_CONFIG=/root/.config/gcloud \
  ${IMAGE_URI}
```

### Change schedule
Update Cloud Scheduler:
```bash
REGION="us-central1"
PIPELINE_NAME="aws"

gcloud scheduler jobs update http ${PIPELINE_NAME}-billing-daily \
  --location=${REGION} \
  --schedule="0 4 * * *"  # Change to 4 AM UTC
```

## Delete Resources

Remove jobs and schedulers:
```bash
REGION="us-central1"
PIPELINE_NAME="aws"  # or azure

# Delete scheduler
gcloud scheduler jobs delete ${PIPELINE_NAME}-billing-daily \
  --location=${REGION} \
  --quiet

# Delete Cloud Run job
gcloud run jobs delete ${PIPELINE_NAME}-billing-pipeline \
  --region=${REGION} \
  --quiet
```
