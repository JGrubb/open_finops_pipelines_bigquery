# Cloud Billing Pipelines to BigQuery - Architecture

## Overview

This project loads AWS and Azure cloud billing data into BigQuery with minimal processing overhead. The key architectural principle is:

**Google Transfer Service moves the data, BigQuery loads it natively, DLT tracks state.**

### Data Flow
1. **Google Transfer Service** pulls billing exports from AWS S3 / Azure Storage into GCS buckets
2. **Manifest discovery** scans GCS for manifest files indicating what data is available
3. **BigQuery native LOAD** reads gzipped CSV/Parquet files directly from GCS URIs (zero data copying)
4. **DLT** manages state via `_dlt_pipeline_state` table to track what's been loaded

This avoids downloading/parsing massive files locally, letting BigQuery do what it does best while DLT handles only state tracking.

## Technology Stack
- **Data Movement**: Google Transfer Service (GCS Transfer)
- **ETL Framework**: DLT Python library (state management only)
- **Source**: GCS buckets (mirrored from AWS S3 / Azure Storage)
- **Destination**: BigQuery
- **Runtime**: Python with `uv`
- **Deployment Target**: GCP Cloud Run (planned)

## Key Design Decisions

### 1. Hybrid Architecture: DLT for State, BigQuery for Loading

**DLT handles**: State management via `_dlt_pipeline_state` table
**BigQuery handles**: Native LOAD from GCS URIs (zero data copying)

This avoids downloading/parsing massive CSV files locally, letting BigQuery do what it does best.

```python
# DLT tracks what's loaded
state = dlt.current.resource_state()
loaded_executions = state.setdefault("loaded_executions", {})

# BigQuery loads directly from GCS
load_job = bq_client.load_table_from_uri(
    gcs_uris,
    table_id,
    job_config=job_config,
)
```

### 2. Manifest-Driven Discovery

Use manifest files as the source of truth:
- **AWS CUR**: Scan for `*-Manifest.json` files containing `assemblyId` and `reportKeys`
- **Azure FOCUS**: Scan for `manifest.json` files containing `runId` and `blobs`
- Only load data referenced in manifests (never scan raw billing files)
- Manifests provide billing period, file locations, and schema information

### 3. State-Based Deduplication

DLT state tracks loaded execution IDs by billing month:

```python
{
  "loaded_executions": {
    "2025-09": ["assembly-id-1", "assembly-id-2"],
    "2025-10": ["assembly-id-3"]
  }
}
```

Before loading, check if execution ID already exists for that month - skip if found.

### 4. Partition Drop/Reload Strategy

Each AWS CUR export regenerates the entire month, so we:
1. Delete the existing month partition
2. Load all files from the new export
3. Mark execution ID as loaded in state

```python
# Delete partition before loading
DELETE FROM table
WHERE DATE(bill_billing_period_start_date) = '2025-09-01'
```

### 5. Schema Management

**AWS CUR Schema Evolution**:
- AWS changes column types and names frequently
- Missing type mappings caused 35 numeric fields to become STRING
- Solution: Complete type mapping in `AWS_TO_BIGQUERY_TYPES`

```python
AWS_TO_BIGQUERY_TYPES = {
    "String": "STRING",
    "BigDecimal": "BIGNUMERIC",           # High-precision decimals
    "OptionalBigDecimal": "BIGNUMERIC",   # Critical: was missing!
    "DateTime": "TIMESTAMP",
    "OptionalString": "STRING",
    "Interval": "STRING",
}
```

**BIGNUMERIC Required**: AWS cost data has up to 10+ decimal places (e.g., `0.0000000186`), exceeding NUMERIC's 9-decimal limit.

**Column Name Normalization**:
- AWS uses `category/name` format (e.g., `lineItem/UsageAmount`)
- Convert to snake_case: `line_item_usage_amount`
- Handle SQL reserved words and duplicates

### 6. Partitioning and Clustering Strategy

**Pre-create tables** with proper partitioning before loading:

```python
table = bigquery.Table(table_id, schema=[
    bigquery.SchemaField("bill_billing_period_start_date", "TIMESTAMP"),
    bigquery.SchemaField("line_item_usage_start_date", "TIMESTAMP"),
])

# MONTH partitioning for cost optimization
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.MONTH,
    field="bill_billing_period_start_date",
)

# Clustering for query performance
table.clustering_fields = ["line_item_usage_start_date"]
```

Then BigQuery's `ALLOW_FIELD_ADDITION` adds remaining columns during load with correct types from manifest.

## AWS CUR Pipeline Implementation

### Manifest Structure (v1)
```json
{
  "assemblyId": "b9c2f8d4-b77d-494c-afc7-93938aa7500a",
  "account": "661924842005",
  "columns": [
    {"category": "identity", "name": "LineItemId", "type": "String"},
    {"category": "lineItem", "name": "UsageAmount", "type": "OptionalBigDecimal"}
  ],
  "billingPeriod": {
    "start": "20250901T000000.000Z",
    "end": "20251001T000000.000Z"
  },
  "reportKeys": [
    "billing/aws-billing-csv/20250901-20251001/aws-billing-csv-00001.csv.gz"
  ],
  "compression": "GZIP",
  "contentType": "text/csv"
}
```

### Pipeline Flow

1. **Manifest Discovery** (`ManifestDiscovery` class)
   - List GCS objects matching `{prefix}/**/*-Manifest.json`
   - Parse JSON to extract metadata
   - Sort by billing period (newest first)

2. **Deduplication Check**
   ```python
   if assembly_id in loaded_executions.get(billing_month, []):
       print(f"Skipping {billing_month} - already loaded")
       continue
   ```

3. **Partition Deletion** (`PartitionManager` class)
   ```python
   partition_manager.delete_partition(
       table_name,
       "bill_billing_period_start_date",
       "2025-09-01"
   )
   ```

4. **Schema Building** (`_build_bigquery_schema()`)
   - Parse manifest columns
   - Normalize names (camelCase → snake_case)
   - Apply type mapping
   - Resolve duplicates

5. **BigQuery Load**
   ```python
   job_config = bigquery.LoadJobConfig(
       source_format=bigquery.SourceFormat.CSV,
       write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
       schema=schema,
       schema_update_options=[
           bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
       ],
       skip_leading_rows=1,
       allow_quoted_newlines=True,
   )

   load_job = bq_client.load_table_from_uri(
       gcs_uris,  # gs://bucket/path/*.csv.gz
       table_id,
       job_config=job_config,
   )
   load_job.result()  # Wait for completion
   ```

6. **State Update**
   ```python
   loaded_executions[billing_month].append(assembly_id)

   # Yield tracking record (DLT persists state after run)
   yield {
       "assembly_id": assembly_id,
       "billing_month": billing_month,
       "loaded_at": datetime.now(timezone.utc),
       "row_count": load_job.output_rows,
   }
   ```

### GCS Path Handling

AWS manifests contain S3 paths, but files are in GCS at different locations:

```python
# Manifest reportKey: "billing/aws-cur/file.csv.gz"
# Actual GCS path: "gcs-transfer/aws_cur/20250901-20251001/file.csv.gz"

# Extract filename from manifest, construct actual GCS path
manifest_dir = "/".join(manifest.manifest_path.split("/")[:-1])
filename = report_key.split("/")[-1]
gcs_uri = f"gs://{bucket}/{manifest_dir}/{filename}"
```

## Azure FOCUS Pipeline (Planned)

### Manifest Structure
```json
{
  "manifestVersion": "2024-04-01",
  "runInfo": {
    "runId": "aa7e4bed-5d34-4c7c-ba34-71bc153b3639",
    "startDate": "2025-10-01T00:00:00",
    "endDate": "2025-10-21T00:00:00+00:00"
  },
  "blobs": [
    {"blobName": "path/to/part_0_0001.snappy.parquet", "dataRowCount": 3}
  ],
  "deliveryConfig": {"fileFormat": "Parquet", "compressionMode": "Snappy"}
}
```

### Pipeline Flow
Similar to AWS with:
- Use `runInfo.runId` for deduplication
- Parquet files instead of CSV
- Schema is FOCUS standard (no monthly evolution)

## Configuration

### Local Development (.dlt/config.toml)
```toml
[sources.aws_cur]
bucket = "your-gcs-bucket"
prefix = "gcs-transfer/aws_cur"
table_name = "aws_cur_data"
project_id = "your-gcp-project"
dataset = "aws_billing_data"

[sources.azure_billing]
bucket = "your-gcs-bucket"
prefix = "gcs-transfer/azure/billingdata"
export_name = "your-billing-export"
table_name = "azure_focus_billing_data"
project_id = "your-gcp-project"
dataset = "azure_billing_data"

[destination.bigquery]
location = "US"
project_id = "your-gcp-project"
```

### Environment Variables (Cloud Run - Planned)
```bash
# AWS CUR
SOURCES__AWS_CUR__BUCKET="your-gcs-bucket"
SOURCES__AWS_CUR__PREFIX="gcs-transfer/aws_cur"
SOURCES__AWS_CUR__TABLE_NAME="aws_cur_data"
SOURCES__AWS_CUR__PROJECT_ID="your-gcp-project"
SOURCES__AWS_CUR__DATASET="aws_billing_data"

# Azure FOCUS
SOURCES__AZURE_BILLING__BUCKET="your-gcs-bucket"
SOURCES__AZURE_BILLING__PREFIX="gcs-transfer/azure/billingdata"
SOURCES__AZURE_BILLING__EXPORT_NAME="your-billing-export"
SOURCES__AZURE_BILLING__TABLE_NAME="azure_focus_billing_data"
SOURCES__AZURE_BILLING__PROJECT_ID="your-gcp-project"
SOURCES__AZURE_BILLING__DATASET="azure_billing_data"

# BigQuery destination
DESTINATION__BIGQUERY__LOCATION="US"
DESTINATION__BIGQUERY__PROJECT_ID="your-gcp-project"
```

## Project Structure

```
open_finops_pipelines_bigquery/
├── .dlt/                       # Gitignored: local dev only
│   ├── config.toml            # Non-sensitive config
│   └── secrets.toml           # Local credentials
├── .gitignore
├── pyproject.toml             # uv project config
├── README.md
├── main.py                    # Entry point
├── docs/
│   └── ARCHITECTURE.md        # This file
└── pipelines/
    ├── __init__.py
    ├── aws_cur.py             # AWS CUR pipeline
    ├── azure_billing.py       # Azure FOCUS pipeline (planned)
    └── common/
        ├── __init__.py
        ├── manifest.py        # Manifest discovery & parsing
        └── bigquery.py        # BigQuery partition operations
```

## Key Learnings

### 1. Type Mapping is Critical
Missing `OptionalBigDecimal` mapping caused 35 numeric fields to default to STRING. Always verify all AWS CUR types are mapped.

### 2. BIGNUMERIC for High Precision
AWS cost data requires BIGNUMERIC (38 decimal places). NUMERIC's 9-decimal limit causes load failures.

### 3. Pre-create Tables with Partitioning
DLT doesn't handle partitioning configuration. Pre-create tables with minimal schema + partitioning/clustering, then let BigQuery add columns during load.

### 4. Avoid Keyword-Based Type Inference
Don't guess field types by name (e.g., "rate" → BIGNUMERIC). Always use manifest type definitions.

### 5. BigQuery Native Loading is Fast
Loading 39 months (10M rows) took ~10 minutes using BigQuery native LOAD. Direct CSV loading from GCS is far more efficient than parsing locally.

## Performance Metrics

**Initial Full Load** (39 months, 2022-08 to 2025-10):
- **Total rows**: 9,984,968
- **Processing time**: ~10 minutes
- **Storage**: ~3.5 GB
- **Partitions**: 39 (one per month)

**Incremental Loads** (single month):
- Typically 30-60 seconds
- Delete partition + load + update state

## DLT System Tables

DLT creates these in BigQuery:

### `_dlt_loads`
Tracks each pipeline run with load_id, timestamp, and status.

### `_dlt_pipeline_state`
Stores custom state as JSON, including loaded execution IDs.

### `{table_name}_load_tracking`
Tracks individual load operations (assembly_id, row_count, timestamps).

## Future Enhancements

- [ ] Azure FOCUS pipeline implementation
- [ ] Cloud Run deployment
- [ ] Error handling and retry logic
- [ ] Monitoring and alerting
- [ ] AWS CUR v2 support
- [ ] Cost optimization (avoid re-scanning old manifests)
- [ ] Data validation and quality checks
