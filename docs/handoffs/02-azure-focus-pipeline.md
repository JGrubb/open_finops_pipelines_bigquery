# Azure FOCUS Pipeline Implementation - Handoff Document

**Date**: 2025-10-22
**Pipeline**: Azure FOCUS Billing to BigQuery
**Status**: ✅ Complete and running

## Overview

Successfully implemented an Azure FOCUS billing pipeline using a hybrid approach: DLT for state management + BigQuery native LOAD jobs for data transfer. This eliminates unnecessary data copying since Parquet files are already in GCS.

## Key Implementation Details

### Architecture
- **Source**: GCS bucket (`plotly-bigquery-billing-finops-data/gcs-transfer/azure/billingdata/plotly-billing-focus-cost/`)
- **Destination**: BigQuery (`plotly-bigquery-billing.plotly_azure_billing_data.azure_focus_billing_data`)
- **Framework**: Hybrid DLT + BigQuery native load
- **Data Format**: Parquet files with Snappy compression
- **State Management**: DLT state (persisted in `_dlt_pipeline_state` table)

### Pipeline Flow
1. Discover Azure manifest files from GCS (newest first)
2. Check DLT state to skip already-loaded run_ids
3. For each new manifest:
   - Delete existing partition (if table exists)
   - Build GCS URIs from manifest blob info
   - Use BigQuery `load_table_from_uri()` to load directly from GCS (zero data copying!)
   - Mark run_id as loaded in DLT state
   - Yield minimal tracking record to DLT
4. DLT persists state after `pipeline.run()` completes

## Hybrid Approach: DLT + BigQuery Native Load

### Why This Approach?

**Problem**: DLT's normal flow downloads Parquet from GCS → uploads to GCS staging → BigQuery loads from staging. This copies data unnecessarily since files are already in GCS.

**Solution**: Use DLT ONLY for state management, bypass data movement with BigQuery native load.

### How It Works

```python
@dlt.resource(name=f"{table_name}_load_tracking", write_disposition="append")
def azure_billing_resource(...) -> Iterator[dict]:
    # 1. Get DLT state
    state = dlt.current.resource_state()
    loaded_executions = state.setdefault("loaded_executions", {})

    # 2. Check if already loaded
    if run_id in loaded_executions.get(billing_month, []):
        continue  # Skip

    # 3. BigQuery native load (no data copying!)
    load_job = bq_client.load_table_from_uri(gcs_uris, table_id, job_config)
    load_job.result()

    # 4. Update state
    loaded_executions[billing_month].append(run_id)

    # 5. Yield minimal tracking record (triggers DLT state save)
    yield {"run_id": run_id, "billing_month": billing_month, ...}
```

### Benefits
- ✅ **Zero data copying**: BigQuery loads directly from existing GCS Parquet files
- ✅ **DLT state management**: Automatic deduplication via `_dlt_pipeline_state` table
- ✅ **Load tracking**: Optional tracking table records load history
- ✅ **Cost efficient**: No network transfer costs for data movement
- ✅ **Fast**: Direct load is faster than download → upload → load

## Azure FOCUS Edge Cases & Solutions

### 1. Manifest Path Mismatch
**Problem**: Azure manifest `blobName` paths don't match actual GCS paths.

**Manifest blobName**:
```
billingdata/plotly-billing-focus-cost/20251001-20251031/.../part_0_0001.snappy.parquet
```

**Actual GCS path**:
```
gcs-transfer/azure/billingdata/plotly-billing-focus-cost/20251001-20251031/.../part_0_0001.snappy.parquet
```

**Solution**: Extract filename from `blobName` and construct path relative to manifest location:
```python
manifest_dir = "/".join(manifest.manifest_path.split("/")[:-1])
filename = blob_info["blobName"].split("/")[-1]
gcs_path = f"{manifest_dir}/{filename}"
uri = f"gs://{bucket}/{gcs_path}"
```

**File**: `pipelines/azure_billing.py:152-190`

### 2. Regex Pattern for Manifest Discovery
**Problem**: Double-brace escaping `\d{{12}}` doesn't work in regex - it escapes the braces themselves instead of creating a repetition pattern.

**Wrong**:
```python
r"\d{{12}}/"  # Matches literal {12}
```

**Right**:
```python
r"\d{12}/"  # Matches 12 digits
```

**File**: `pipelines/common/manifest.py:126`

### 3. Decimal128 to BigQuery Type Mapping
**Problem**: Azure FOCUS uses `decimal128(38, 18)` in Parquet, which BigQuery defaults to `NUMERIC`, but values exceed NUMERIC range.

**Error**:
```
The value for column 'BilledCost' is out of valid NUMERIC range.
Consider specifying decimal target types to allow a larger range of values.
```

**Solution**: Configure load job to use `BIGNUMERIC` for decimal columns:
```python
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    decimal_target_types=[bigquery.DecimalTargetType.BIGNUMERIC],
)
```

**File**: `pipelines/azure_billing.py:116-123`

### 4. Column Name Normalization Not Needed
**Discovery**: Azure FOCUS uses PascalCase column names (`BillingPeriodStart`, `BilledCost`, etc.) which are valid BigQuery column names.

**Decision**: No normalization required. BigQuery's native Parquet load preserves column names as-is, and BigQuery is case-insensitive for column references.

**Benefit**: Simpler code, no risk of name collision issues.

## DLT State Management

### How State Persists

**In-memory state during execution**:
```python
state = dlt.current.resource_state()
loaded_executions = state.setdefault("loaded_executions", {})
loaded_executions[billing_month].append(run_id)
```

**State persistence after pipeline.run()**:
- DLT automatically saves state to `_dlt_pipeline_state` table
- State is compressed and stored as binary data
- Next run reads state from table before processing

### State Structure
```python
{
    "loaded_executions": {
        "2025-10": ["aa7e4bed-5d34-4c7c-ba34-71bc153b3639"],
        "2025-11": ["..."],
    }
}
```

**File**: `pipelines/azure_billing.py:77-79, 135-138`

## Configuration

### `.dlt/config.toml`
```toml
[sources.azure_billing]
bucket = "plotly-bigquery-billing-finops-data"
prefix = "gcs-transfer/azure/billingdata"
export_name = "plotly-billing-focus-cost"
table_name = "azure_focus_billing_data"
project_id = "plotly-bigquery-billing"
dataset = "plotly_azure_billing_data"
```

### Path Pattern
```
gs://{bucket}/{prefix}/{export_name}/YYYYMMDD-YYYYMMDD/YYYYMMDDHHmm/{run_id}/
  ├── manifest.json
  ├── part_0_0001.snappy.parquet
  └── part_1_0001.snappy.parquet
```

## Testing & Validation

### First Run Output
```
Discovered 1 Azure billing manifests
Processing 2025-10 (run_id: aa7e4bed-5d34-4c7c-ba34-71bc153b3639)
Table azure_focus_billing_data does not exist yet, skipping partition deletion
  Loading 2 Parquet files from GCS...
    gs://.../part_0_0001.snappy.parquet
    gs://.../part_1_0001.snappy.parquet
  Loaded 26694 rows to azure_focus_billing_data
Completed loading 2025-10 (run_id: aa7e4bed-5d34-4c7c-ba34-71bc153b3639)
```

### Second Run Output (State Management Working)
```
Discovered 1 Azure billing manifests
Skipping 2025-10 (run_id: aa7e4bed-5d34-4c7c-ba34-71bc153b3639) - already loaded
```

### Verification Queries

**Check billing data**:
```sql
SELECT COUNT(*) as row_count,
       MIN(BillingPeriodStart) as min_date,
       MAX(BillingPeriodStart) as max_date
FROM `plotly-bigquery-billing.plotly_azure_billing_data.azure_focus_billing_data`
```

**Check load tracking**:
```sql
SELECT *
FROM `plotly-bigquery-billing.plotly_azure_billing_data.azure_focus_billing_data_load_tracking`
ORDER BY loaded_at DESC
```

**Check DLT state**:
```sql
SELECT *
FROM `plotly-bigquery-billing.plotly_azure_billing_data._dlt_pipeline_state`
WHERE pipeline_name = 'azure_billing_pipeline'
ORDER BY _dlt_load_id DESC
```

## Tables Created

### 1. `azure_focus_billing_data` (Main Data Table)
- 96 columns from Azure FOCUS schema
- Partitioned by `BillingPeriodStart` (TIMESTAMP)
- Clustered by `ChargePeriodStart`
- Uses BIGNUMERIC for cost fields

### 2. `azure_focus_billing_data_load_tracking` (Load Tracking)
- Schema:
  - `run_id` (STRING): Azure export run ID
  - `billing_month` (STRING): YYYY-MM format
  - `loaded_at` (TIMESTAMP): When load completed
  - `row_count` (INTEGER): Rows loaded
  - `file_count` (INTEGER): Parquet files loaded
  - `_dlt_load_id`, `_dlt_id`: DLT metadata

### 3. `_dlt_pipeline_state` (DLT State Management)
- Stores compressed state data
- Updated automatically by DLT

## Comparison with AWS Pipeline

| Aspect | AWS CUR | Azure FOCUS |
|--------|---------|-------------|
| **Data Format** | Gzipped CSV | Parquet with Snappy |
| **Data Loading** | Download → PyArrow → DLT → BigQuery | BigQuery native load from GCS |
| **Column Normalization** | Yes (AWS uses `/` in names) | No (PascalCase is valid) |
| **Type Handling** | Explicit mapping from manifest | Automatic from Parquet schema |
| **Decimal Precision** | float64 | BIGNUMERIC (38 digits) |
| **Data Copying** | Downloads CSV content | Zero copying! |
| **Speed** | Slower (CSV parsing) | Faster (native Parquet load) |

## Known Issues & Future Improvements

### 1. Partition Column Case Sensitivity
**Current**: Using lowercase `"billingperiodstart"` in partition deletion.

**Issue**: BigQuery is case-insensitive, but being inconsistent with actual column name `BillingPeriodStart`.

**Future**: Update to use PascalCase for consistency:
```python
partition_manager.delete_partition(table_name, "BillingPeriodStart", partition_date)
```

**File**: `pipelines/azure_billing.py:106`

### 2. Load Tracking Table Utility
**Current**: Creates tracking table with load metadata.

**Question**: Is this table actually useful, or just state overhead?

**Future**: Consider removing tracking table if not used for monitoring/debugging.

### 3. Error Handling
**Current**: BigQuery load errors bubble up and crash the pipeline.

**Future**: Add retry logic and better error messages for common issues:
- File not found
- Schema mismatch
- Permission errors

### 4. Multi-Month Support
**Current**: Only tested with single billing month.

**Future**: Verify behavior when multiple months have manifests to process.

## Files Modified/Created

### Modified
- `pipelines/azure_billing.py` - Rewrote to use hybrid DLT + BigQuery native load approach
- `pipelines/common/manifest.py` - Fixed regex pattern for Azure manifest discovery

### Configuration
- `.dlt/config.toml` - Azure billing source configuration
- `main.py` - Already had Azure pipeline registered

## Running the Pipeline

```bash
# Run Azure pipeline only
uv run python main.py azure

# Run all pipelines (AWS + Azure)
uv run python main.py all
```

## Key Learnings

### 1. Hybrid Approach is Powerful
Combining DLT's state management with direct BigQuery operations gives you the best of both worlds:
- DLT handles deduplication and orchestration
- BigQuery handles efficient data loading

### 2. Native Load > Data Movement
When data is already in GCS, use BigQuery's `load_table_from_uri()` instead of downloading/uploading:
- Faster
- Cheaper (no network egress from GCS)
- Simpler (no data transformation needed for Parquet)

### 3. DLT State is Flexible
You can use DLT state for tracking without yielding actual data:
- State persists even if you yield minimal tracking records
- Generator function has full control over what actually happens
- Can mix DLT resources with direct BigQuery operations

### 4. Parquet Schema Preservation
BigQuery's Parquet load preserves column names and types:
- No need to normalize Azure FOCUS PascalCase columns
- Decimal128 automatically maps to NUMERIC (or BIGNUMERIC with config)
- Schema evolution handled automatically

## Next Steps

### Apply to AWS Pipeline?
**Question**: Should we refactor AWS pipeline to use the same hybrid approach?

**Benefits**:
- Eliminate CSV download/parsing overhead
- Faster loads
- Consistent architecture

**Challenges**:
- AWS CUR is CSV, not Parquet
- Would need to convert CSV to Parquet first OR use BigQuery CSV load
- Column normalization still needed (AWS uses `/` in column names)

**Recommendation**: Keep AWS as-is for now (PyArrow works well for CSV). Consider conversion to Parquet export if AWS supports it in future.
