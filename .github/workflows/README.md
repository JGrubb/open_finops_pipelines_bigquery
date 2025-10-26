# GitHub Actions Workflows

## deploy.yml

Automatically builds and updates Cloud Run jobs when code is pushed to main.

### Setup

1. Create Workload Identity Federation for GitHub Actions:
   ```bash
   gcloud iam workload-identity-pools create github \
     --location=global

   gcloud iam workload-identity-pools providers create-oidc github-provider \
     --workload-identity-pool=github \
     --issuer-uri=https://token.actions.githubusercontent.com \
     --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository"
   ```

2. Add GitHub Secrets:
   - `GCP_PROJECT_ID`
   - `WIF_PROVIDER`
   - `WIF_SERVICE_ACCOUNT`
   - `AWS_GCS_BUCKET`, `AWS_DATASET`, etc.

3. Push to main branch to trigger deployment

### Manual Trigger

Use "Run workflow" button in GitHub Actions tab.
