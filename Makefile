# Load environment variables from .env if present
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Default values (override via .env or command line)
GCP_PROJECT_ID ?= $(shell gcloud config get-value project 2>/dev/null)
GCP_REGION ?= us-central1
ARTIFACT_REGISTRY_REPO ?= finops-pipelines
IMAGE_NAME ?= billing-pipeline
IMAGE_TAG ?= latest

# Derived values
PROJECT_NUMBER = $(shell gcloud projects describe $(GCP_PROJECT_ID) --format="value(projectNumber)" 2>/dev/null)
SERVICE_ACCOUNT = $(PROJECT_NUMBER)-compute@developer.gserviceaccount.com
IMAGE_URI = $(GCP_REGION)-docker.pkg.dev/$(GCP_PROJECT_ID)/$(ARTIFACT_REGISTRY_REPO)/$(IMAGE_NAME):$(IMAGE_TAG)

# Pipeline-specific defaults
AWS_SCHEDULE ?= "0 2 * * *"
AZURE_SCHEDULE ?= "0 3 * * *"
MEMORY ?= 2Gi
CPU ?= 1
TIMEOUT ?= 1h
MAX_RETRIES ?= 2

.PHONY: help
help: ## Show this help message
	@echo "Billing Pipeline Deployment"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

.PHONY: check-env
check-env: ## Validate required environment variables
	@echo "Checking environment configuration..."
	@test -n "$(GCP_PROJECT_ID)" || (echo "ERROR: GCP_PROJECT_ID not set" && exit 1)
	@test -n "$(AWS_GCS_BUCKET)" || (echo "ERROR: AWS_GCS_BUCKET not set" && exit 1)
	@test -n "$(AWS_DATASET)" || (echo "ERROR: AWS_DATASET not set" && exit 1)
	@echo "✓ Environment configuration valid"

.PHONY: setup-gcp
setup-gcp: ## Enable required GCP APIs and create Artifact Registry
	@echo "Enabling required GCP APIs..."
	gcloud services enable \
		cloudbuild.googleapis.com \
		run.googleapis.com \
		cloudscheduler.googleapis.com \
		artifactregistry.googleapis.com \
		--project=$(GCP_PROJECT_ID)
	@echo "Creating Artifact Registry repository..."
	gcloud artifacts repositories create $(ARTIFACT_REGISTRY_REPO) \
		--repository-format=docker \
		--location=$(GCP_REGION) \
		--description="FinOps billing pipelines" \
		--project=$(GCP_PROJECT_ID) || true
	@echo "✓ GCP setup complete"

.PHONY: setup-permissions
setup-permissions: ## Grant service account required permissions
	@echo "Granting permissions to $(SERVICE_ACCOUNT)..."
	gcloud projects add-iam-policy-binding $(GCP_PROJECT_ID) \
		--member="serviceAccount:$(SERVICE_ACCOUNT)" \
		--role="roles/bigquery.dataEditor"
	gcloud projects add-iam-policy-binding $(GCP_PROJECT_ID) \
		--member="serviceAccount:$(SERVICE_ACCOUNT)" \
		--role="roles/bigquery.jobUser"
	gcloud projects add-iam-policy-binding $(GCP_PROJECT_ID) \
		--member="serviceAccount:$(SERVICE_ACCOUNT)" \
		--role="roles/storage.objectViewer"
	@echo "✓ Permissions granted"

.PHONY: build
build: ## Build and push Docker image to Artifact Registry
	@echo "Building and pushing $(IMAGE_URI)..."
	gcloud builds submit \
		--tag $(IMAGE_URI) \
		--project=$(GCP_PROJECT_ID) \
		--region=$(GCP_REGION)
	@echo "✓ Image built and pushed"

.PHONY: deploy-aws
deploy-aws: check-env ## Deploy AWS billing pipeline
	@echo "Deploying AWS billing pipeline..."
	gcloud run jobs create aws-billing-pipeline \
		--image=$(IMAGE_URI) \
		--region=$(GCP_REGION) \
		--task-timeout=$(TIMEOUT) \
		--max-retries=$(MAX_RETRIES) \
		--memory=$(MEMORY) \
		--cpu=$(CPU) \
		--service-account=$(SERVICE_ACCOUNT) \
		--set-env-vars="SOURCES__AWS_CUR__BUCKET=$(AWS_GCS_BUCKET),SOURCES__AWS_CUR__PREFIX=$(AWS_GCS_PREFIX),SOURCES__AWS_CUR__TABLE_NAME=$(AWS_TABLE_NAME),SOURCES__AWS_CUR__PROJECT_ID=$(GCP_PROJECT_ID),SOURCES__AWS_CUR__DATASET=$(AWS_DATASET),DESTINATION__BIGQUERY__PROJECT_ID=$(GCP_PROJECT_ID),DESTINATION__BIGQUERY__LOCATION=US" \
		--command="uv" \
		--args="run,python,main.py,aws" \
		--project=$(GCP_PROJECT_ID) || \
	gcloud run jobs update aws-billing-pipeline \
		--image=$(IMAGE_URI) \
		--region=$(GCP_REGION) \
		--task-timeout=$(TIMEOUT) \
		--max-retries=$(MAX_RETRIES) \
		--memory=$(MEMORY) \
		--cpu=$(CPU) \
		--service-account=$(SERVICE_ACCOUNT) \
		--set-env-vars="SOURCES__AWS_CUR__BUCKET=$(AWS_GCS_BUCKET),SOURCES__AWS_CUR__PREFIX=$(AWS_GCS_PREFIX),SOURCES__AWS_CUR__TABLE_NAME=$(AWS_TABLE_NAME),SOURCES__AWS_CUR__PROJECT_ID=$(GCP_PROJECT_ID),SOURCES__AWS_CUR__DATASET=$(AWS_DATASET),DESTINATION__BIGQUERY__PROJECT_ID=$(GCP_PROJECT_ID),DESTINATION__BIGQUERY__LOCATION=US" \
		--command="uv" \
		--args="run,python,main.py,aws" \
		--project=$(GCP_PROJECT_ID)
	@echo "Creating Cloud Scheduler job..."
	gcloud scheduler jobs create http aws-billing-daily \
		--location=$(GCP_REGION) \
		--schedule=$(AWS_SCHEDULE) \
		--uri="https://$(GCP_REGION)-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$(GCP_PROJECT_ID)/jobs/aws-billing-pipeline:run" \
		--http-method=POST \
		--oauth-service-account-email="$(SERVICE_ACCOUNT)" \
		--project=$(GCP_PROJECT_ID) || \
	gcloud scheduler jobs update http aws-billing-daily \
		--location=$(GCP_REGION) \
		--schedule=$(AWS_SCHEDULE) \
		--project=$(GCP_PROJECT_ID)
	@echo "✓ AWS pipeline deployed"

.PHONY: deploy-azure
deploy-azure: check-env ## Deploy Azure billing pipeline
	@echo "Deploying Azure billing pipeline..."
	gcloud run jobs create azure-billing-pipeline \
		--image=$(IMAGE_URI) \
		--region=$(GCP_REGION) \
		--task-timeout=$(TIMEOUT) \
		--max-retries=$(MAX_RETRIES) \
		--memory=$(MEMORY) \
		--cpu=$(CPU) \
		--service-account=$(SERVICE_ACCOUNT) \
		--set-env-vars="SOURCES__AZURE_BILLING__BUCKET=$(AZURE_GCS_BUCKET),SOURCES__AZURE_BILLING__PREFIX=$(AZURE_GCS_PREFIX),SOURCES__AZURE_BILLING__EXPORT_NAME=$(AZURE_EXPORT_NAME),SOURCES__AZURE_BILLING__TABLE_NAME=$(AZURE_TABLE_NAME),SOURCES__AZURE_BILLING__PROJECT_ID=$(GCP_PROJECT_ID),SOURCES__AZURE_BILLING__DATASET=$(AZURE_DATASET),DESTINATION__BIGQUERY__PROJECT_ID=$(GCP_PROJECT_ID),DESTINATION__BIGQUERY__LOCATION=US" \
		--command="uv" \
		--args="run,python,main.py,azure" \
		--project=$(GCP_PROJECT_ID) || \
	gcloud run jobs update azure-billing-pipeline \
		--image=$(IMAGE_URI) \
		--region=$(GCP_REGION) \
		--task-timeout=$(TIMEOUT) \
		--max-retries=$(MAX_RETRIES) \
		--memory=$(MEMORY) \
		--cpu=$(CPU) \
		--service-account=$(SERVICE_ACCOUNT) \
		--set-env-vars="SOURCES__AZURE_BILLING__BUCKET=$(AZURE_GCS_BUCKET),SOURCES__AZURE_BILLING__PREFIX=$(AZURE_GCS_PREFIX),SOURCES__AZURE_BILLING__EXPORT_NAME=$(AZURE_EXPORT_NAME),SOURCES__AZURE_BILLING__TABLE_NAME=$(AZURE_TABLE_NAME),SOURCES__AZURE_BILLING__PROJECT_ID=$(GCP_PROJECT_ID),SOURCES__AZURE_BILLING__DATASET=$(AZURE_DATASET),DESTINATION__BIGQUERY__PROJECT_ID=$(GCP_PROJECT_ID),DESTINATION__BIGQUERY__LOCATION=US" \
		--command="uv" \
		--args="run,python,main.py,azure" \
		--project=$(GCP_PROJECT_ID)
	@echo "Creating Cloud Scheduler job..."
	gcloud scheduler jobs create http azure-billing-daily \
		--location=$(GCP_REGION) \
		--schedule=$(AZURE_SCHEDULE) \
		--uri="https://$(GCP_REGION)-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$(GCP_PROJECT_ID)/jobs/azure-billing-pipeline:run" \
		--http-method=POST \
		--oauth-service-account-email="$(SERVICE_ACCOUNT)" \
		--project=$(GCP_PROJECT_ID) || \
	gcloud scheduler jobs update http azure-billing-daily \
		--location=$(GCP_REGION) \
		--schedule=$(AZURE_SCHEDULE) \
		--project=$(GCP_PROJECT_ID)
	@echo "✓ Azure pipeline deployed"

.PHONY: deploy
deploy: build deploy-aws deploy-azure ## Build image and deploy all pipelines

.PHONY: update
update: build ## Rebuild image and update all existing jobs
	@echo "Updating Cloud Run jobs with new image..."
	@for pipeline in aws azure; do \
		gcloud run jobs update $${pipeline}-billing-pipeline \
			--image=$(IMAGE_URI) \
			--region=$(GCP_REGION) \
			--project=$(GCP_PROJECT_ID) || true; \
	done
	@echo "✓ All jobs updated"

.PHONY: run-aws
run-aws: ## Manually trigger AWS pipeline
	gcloud run jobs execute aws-billing-pipeline \
		--region=$(GCP_REGION) \
		--project=$(GCP_PROJECT_ID)

.PHONY: run-azure
run-azure: ## Manually trigger Azure pipeline
	gcloud run jobs execute azure-billing-pipeline \
		--region=$(GCP_REGION) \
		--project=$(GCP_PROJECT_ID)

.PHONY: logs-aws
logs-aws: ## View AWS pipeline logs
	gcloud logging read \
		"resource.type=cloud_run_job AND resource.labels.job_name=aws-billing-pipeline" \
		--limit=100 \
		--format=json \
		--project=$(GCP_PROJECT_ID)

.PHONY: logs-azure
logs-azure: ## View Azure pipeline logs
	gcloud logging read \
		"resource.type=cloud_run_job AND resource.labels.job_name=azure-billing-pipeline" \
		--limit=100 \
		--format=json \
		--project=$(GCP_PROJECT_ID)

.PHONY: destroy-aws
destroy-aws: ## Delete AWS pipeline and scheduler
	gcloud scheduler jobs delete aws-billing-daily \
		--location=$(GCP_REGION) \
		--project=$(GCP_PROJECT_ID) \
		--quiet || true
	gcloud run jobs delete aws-billing-pipeline \
		--region=$(GCP_REGION) \
		--project=$(GCP_PROJECT_ID) \
		--quiet || true
	@echo "✓ AWS pipeline destroyed"

.PHONY: destroy-azure
destroy-azure: ## Delete Azure pipeline and scheduler
	gcloud scheduler jobs delete azure-billing-daily \
		--location=$(GCP_REGION) \
		--project=$(GCP_PROJECT_ID) \
		--quiet || true
	gcloud run jobs delete azure-billing-pipeline \
		--region=$(GCP_REGION) \
		--project=$(GCP_PROJECT_ID) \
		--quiet || true
	@echo "✓ Azure pipeline destroyed"

.PHONY: destroy
destroy: destroy-aws destroy-azure ## Delete all pipelines and schedulers

.PHONY: status
status: ## Show deployment status
	@echo "=== Cloud Run Jobs ==="
	@gcloud run jobs list --region=$(GCP_REGION) --project=$(GCP_PROJECT_ID) | grep billing || echo "No jobs found"
	@echo ""
	@echo "=== Cloud Scheduler ==="
	@gcloud scheduler jobs list --location=$(GCP_REGION) --project=$(GCP_PROJECT_ID) | grep billing || echo "No schedulers found"
