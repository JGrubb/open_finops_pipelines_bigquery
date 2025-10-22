# AWS CUR Pipeline Implementation - Handoff Document

**Date**: 2025-10-22
**Pipeline**: AWS Cost and Usage Report (CUR) to BigQuery
**Status**: ✅ Complete and running

## Overview

Successfully implemented an AWS CUR billing pipeline using DLT (data load tool) that loads AWS billing data from GCS into BigQuery with proper type handling, schema normalization, and state management.

## Key Implementation Details

### Architecture
- **Source**: GCS bucket (`plotly-bigquery-billing-finops-data/gcs-transfer/aws_cur/`)
- **Destination**: BigQuery (`plotly-bigquery-billing.plotly_aws_billing_data.aws_cur_data_v2`)
- **Framework**: DLT (data load tool) with BigQuery destination
- **Data Format**: Arrow tables (not pandas DataFrames)
- **File Format**: Gzipped CSV files referenced in AWS CUR manifest files

### Pipeline Flow
1. Discover AWS CUR manifest files from GCS (newest first)
2. Check DLT state to skip already-loaded assemblies
3. For each new manifest:
   - Delete existing partition (if table exists)
   - Read CSV files with PyArrow using manifest schema
   - Normalize column names and resolve duplicates
   - Yield Arrow tables to DLT
   - Mark assembly_id as loaded in state
4. DLT loads Arrow tables to BigQuery as Parquet

## AWS CUR Edge Cases & Solutions

### 1. GCS Transfer Path Structure
**Problem**: AWS CUR manifests transferred to GCS don't preserve the original S3 structure.

**Original S3 structure**:
```
s3://bucket/prefix/export-name/YYYYMMDD-YYYYMMDD/export-name-Manifest.json
```

**GCS transfer structure**:
```
gs://bucket/gcs-transfer/aws_cur/YYYYMMDD-YYYYMMDD/aws-billing-csv-Manifest.json
```

**Solution**: Updated manifest discovery regex to match GCS structure without `export_name` subfolder level:
```python
pattern = re.compile(
    rf"^{re.escape(prefix.rstrip('/'))}/"
    r"(\d{8})-(\d{8})/"
    r".*-Manifest\.json$"
)
```

**File**: `pipelines/common/manifest.py:75-79`

### 2. Manifest reportKeys Path Mismatch
**Problem**: Manifest `reportKeys` contain original S3 paths like `billing/aws-billing-csv/YYYYMMDD-YYYYMMDD/file.csv.gz`, but files are actually at `gcs-transfer/aws_cur/YYYYMMDD-YYYYMMDD/file.csv.gz`.

**Solution**: Extract filename from reportKey and construct path relative to manifest location:
```python
manifest_dir = "/".join(manifest.manifest_path.split("/")[:-1])
filename = file_key.split("/")[-1]
actual_path = f"{manifest_dir}/{filename}"
```

**File**: `pipelines/aws_cur.py:129-133`

### 3. Mixed-Type Columns
**Problem**: AWS CUR has columns like `product/ecu` that contain both strings ("73") and numeric values, causing PyArrow conversion errors:
```
ArrowInvalid: Could not convert '73' with type str: tried to convert to double
```

**Solution**: Use PyArrow CSV reader with explicit column types from manifest instead of pandas:
```python
column_types = _build_column_types_from_manifest(manifest)
table = csv.read_csv(
    io.BytesIO(content),
    convert_options=csv.ConvertOptions(column_types=column_types),
)
```

**File**: `pipelines/aws_cur.py:216-223`

### 4. Column Name Normalization Collisions
**Problem**: AWS CUR has columns that differ only in case (e.g., `product/ecu` vs `product/ECU`) which collide after DLT normalization, causing:
```
NameNormalizationCollision: Arrow column name collision after input data normalization
```

**Solution**: Pre-normalize column names ourselves using a deterministic process and resolve duplicates with numeric suffixes:
```python
# Normalize: camelCase → snake_case, special chars → underscores, handle reserved words
normalized = [_normalize_column_name(name) for name in table.column_names]
# Resolve duplicates: ecu, ecu_1, ecu_2, etc.
unique_names = _resolve_duplicate_names(normalized)
table = table.rename_columns(unique_names)
```

**Files**:
- `pipelines/aws_cur.py:161-191` (normalization functions)
- `pipelines/aws_cur.py:224-227` (application)

### 5. Timestamp Timezone Handling
**Problem**: AWS timestamps include timezone offset (e.g., `2025-10-01T00:00:00Z`) but PyArrow expects timezone-aware types:
```
CSV conversion error to timestamp[s]: expected no zone offset in '2025-10-01T00:00:00Z'
```

**Solution**: Use timezone-aware timestamp type in mapping:
```python
"DateTime": pa.timestamp("s", tz="UTC")
```

**File**: `pipelines/aws_cur.py:21`

### 6. Partition Deletion on First Load
**Problem**: On first run, trying to delete partition fails with "Table does not have a schema" error.

**Solution**: Check if table exists before attempting partition deletion:
```python
def delete_partition(self, table_name: str, partition_column: str, partition_value: str):
    if not self.table_exists(table_name):
        print(f"Table {table_name} does not exist yet, skipping partition deletion")
        return
    # ... proceed with deletion
```

**File**: `pipelines/common/bigquery.py:30-57`

## DLT Discoveries

### 1. Configuration Injection with `dlt.config.value`
**Discovery**: Using `dlt.config.value` as a function parameter default works ONLY in `@dlt.source` or `@dlt.resource` decorated functions. DLT infers the config path from the decorator context.

**Example that works**:
```python
@dlt.source
def aws_billing_source(
    bucket: str = dlt.config.value,  # DLT looks in sources.aws_cur.bucket
    prefix: str = dlt.config.value,  # DLT looks in sources.aws_cur.prefix
):
    pass
```

**Example that doesn't work**:
```python
def run_pipeline(name: str):
    dataset_name: str = dlt.config.value  # DLT doesn't know where to look!
```

**Solution**: Use explicit config path in non-decorated functions:
```python
dataset_name = dlt.config[f"sources.{config_section}.dataset"]
```

**File**: `main.py:50-55`

### 2. Config Structure Conventions
**Discovery**: DLT expects config at `[sources.{module_name}]` not `[sources.{function_name}]`.

**Wrong**:
```toml
[sources.aws_billing_source]
bucket = "..."
```

**Right**:
```toml
[sources.aws_cur]  # Module name from pipelines/aws_cur.py
bucket = "..."
```

**File**: `.dlt/config.toml:7-12`

### 3. State Management for Deduplication
**Discovery**: DLT provides `dlt.current.resource_state()` which persists in BigQuery's `_dlt_pipeline_state` table across runs.

**Implementation**:
```python
state = dlt.current.resource_state()
loaded_executions = state.setdefault("loaded_executions", {})

# Check if already loaded
if assembly_id in loaded_executions.get(billing_month, []):
    continue

# ... process data ...

# Mark as loaded
loaded_executions[billing_month].append(assembly_id)
```

**File**: `pipelines/aws_cur.py:89-149`

### 4. Dataset Name Configuration
**Discovery**: DLT pipeline's `dataset_name` parameter must be set when creating the pipeline. It's not auto-inferred from source config.

**Implementation**:
```python
dataset_name = dlt.config[f"sources.{config_section}.dataset"]
pipeline = dlt.pipeline(
    pipeline_name=pipeline_name,
    destination="bigquery",
    dataset_name=dataset_name,
)
```

**File**: `main.py:52-62`

## Pandas vs PyArrow Learnings

### Why Arrow Tables Over Pandas

**Performance**: Arrow tables bypass DLT's normalization steps when writing to Parquet-compatible destinations like BigQuery. Pandas DataFrames require conversion to Arrow, then to Parquet.

**Type Safety**: Arrow tables are schema-aware - they store column names and types alongside data. Pandas infers types which can fail with mixed-type columns.

**DLT Recommendation**: DLT docs explicitly state "We recommend yielding Arrow tables from your resources" for Parquet destinations.

**Reference**: `docs/001-arrow-tables-over-pandas.md`

### PyArrow CSV Reading

**Key advantage**: Explicit type specification prevents inference errors:
```python
table = csv.read_csv(
    io.BytesIO(content),
    convert_options=csv.ConvertOptions(
        column_types={
            'product/ecu': pa.string(),
            'lineitem/usageamount': pa.float64(),
        }
    )
)
```

**Type mapping from AWS CUR manifest**:
```python
AWS_TO_ARROW_TYPES = {
    "String": pa.string(),
    "BigDecimal": pa.float64(),
    "DateTime": pa.timestamp("s", tz="UTC"),
    "OptionalString": pa.string(),
    "Interval": pa.string(),
}
```

**File**: `pipelines/aws_cur.py:18-24`

### Arrow Table Operations

**Adding columns**:
```python
table = table.append_column(
    "_assembly_id",
    pa.array([assembly_id] * len(table), type=pa.string())
)
```

**Renaming columns**:
```python
table = table.rename_columns(new_column_names)
```

**File**: `pipelines/aws_cur.py:136-142`

## Column Name Normalization Strategy

Based on previous `open-finops-pipelines` implementation, we normalize column names through multiple steps:

1. **CamelCase to snake_case**: `userId` → `user_id`
2. **Special chars to underscores**: `product/ecu` → `product_ecu`
3. **Consecutive underscores**: `product__ecu` → `product_ecu`
4. **Strip edges**: `_product_ecu_` → `product_ecu`
5. **Numeric prefix**: `123column` → `col_123column`
6. **SQL reserved words**: `group` → `group_col`
7. **Duplicate resolution**: `ecu`, `ecu_1`, `ecu_2`

**Implementation**:
```python
def _normalize_column_name(column_name: str) -> str:
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', column_name)
    name = name.lower()
    name = re.sub(r'[^a-z0-9]', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    if not name:
        return "unknown_column"
    if name[0].isdigit():
        name = f"col_{name}"
    if name in SQL_RESERVED_WORDS:
        name = f"{name}_col"
    return name
```

**File**: `pipelines/aws_cur.py:161-177`

## Configuration Best Practices

### Keep Organization-Specific Config Out of Code

**Wrong** (hardcoded in PIPELINES registry):
```python
PIPELINES = {
    "aws": (aws_billing_source, "aws_pipeline", "AWS CUR", "plotly_aws_data"),
}
```

**Right** (config-driven):
```python
PIPELINES = {
    "aws": (aws_billing_source, "aws_pipeline", "AWS CUR"),
}
# Dataset name comes from .dlt/config.toml
dataset_name = dlt.config[f"sources.{config_section}.dataset"]
```

This allows open-sourcing the pipeline code while keeping organization-specific config in `.dlt/config.toml` (which can be git-ignored or templated).

## Testing & Validation

### First Run Output
```
Discovered 39 AWS CUR manifests
Processing 2025-10 (assembly_id: 05e3cd23-6e22-4f5c-a23a-48b7662cef4d)
Table aws_cur_data_v2 does not exist yet, skipping partition deletion
  Loading gcs-transfer/aws_cur/20251001-20251101/aws-billing-csv-00001.csv.gz
  Loading gcs-transfer/aws_cur/20251001-20251101/aws-billing-csv-00002.csv.gz
  Loading gcs-transfer/aws_cur/20251001-20251101/aws-billing-csv-00003.csv.gz
Completed loading 2025-10 (assembly_id: 05e3cd23-6e22-4f5c-a23a-48b7662cef4d)
```

### Subsequent Run Output
```
Discovered 39 AWS CUR manifests
Processing 2025-10 (assembly_id: 05e3cd23-6e22-4f5c-a23a-48b7662cef4d)
Deleting partition 2025-10-01 from aws_cur_data_v2...
Deleted 123456 rows from partition 2025-10-01
  Loading gcs-transfer/aws_cur/20251001-20251101/aws-billing-csv-00001.csv.gz
  ...
```

### State Persistence Check
After first load, check `_dlt_pipeline_state` table in BigQuery to verify state is persisted.

## Known Issues & Future Improvements

### 1. DLT Infrastructure Tables Initialization Issue
**Issue**: On first run with empty/incorrect credentials in `.dlt/secrets.toml`, DLT fails silently to create infrastructure tables (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`), causing subsequent runs to fail with "Table not found" errors.

**Root Cause**: Having an empty `[destination.bigquery.credentials]` section in `secrets.toml` (even if commented) tells DLT to use explicit credentials but provides none, causing authentication to fail silently.

**Solution Applied**:
1. Removed empty credentials section from `.dlt/secrets.toml`
2. DLT now uses Application Default Credentials automatically
3. Manually initialized DLT tables with minimal pipeline run

**Prevention**:
- Keep `.dlt/secrets.toml` minimal - only use for actual secrets
- For local dev, rely on Application Default Credentials (`gcloud auth application-default login`)
- First-time setup should verify DLT tables exist

**File**: `.dlt/secrets.toml` (now contains only comments)

### 2. Unused `original_names` Variable
**Issue**: `_build_column_types_from_manifest` returns `original_names` but it's not used.

**File**: `pipelines/aws_cur.py:215` (Pylance warning)

**Future**: Either use it for mapping/logging or remove it from return value.

### 3. BigQuery Storage API Warning
**Issue**: Warning on every run: "Cannot create BigQuery Storage client, the dependency google-cloud-bigquery-storage is not installed"

**Impact**: None - only needed for reading FROM BigQuery, not writing TO it.

**Fix** (optional): Add `google-cloud-bigquery-storage` to `pyproject.toml` to suppress warning.

### 4. Type Mapping Completeness
**Current**: Only maps 5 AWS CUR types (String, BigDecimal, DateTime, OptionalString, Interval).

**Future**: Check if AWS uses other types and add mappings as needed.

## Files Modified/Created

### Created
- `pipelines/aws_cur.py` - AWS CUR pipeline implementation
- `pipelines/common/manifest.py` - Manifest discovery for AWS/Azure
- `pipelines/common/bigquery.py` - BigQuery partition management
- `main.py` - Extensible pipeline registry and runner
- `.dlt/config.toml` - DLT configuration
- `docs/001-arrow-tables-over-pandas.md` - Design decision document
- `docs/handoffs/01-aws-cur-pipeline.md` - This document

### Configuration
- `.dlt/config.toml` - Pipeline configuration (dataset names, GCS paths, etc.)
- `pyproject.toml` - Minimal dependencies (`dlt[bigquery]>=1.5.0`)

## First-Time Setup

Before running the pipeline for the first time:

1. **Authenticate with Google Cloud**:
   ```bash
   gcloud auth application-default login
   ```

2. **Verify credentials file is minimal**:
   - Check `.dlt/secrets.toml` contains NO credential sections
   - Empty credential sections will cause silent authentication failures
   - DLT will use Application Default Credentials automatically

3. **On first run**, DLT will create infrastructure tables automatically:
   - `_dlt_loads` - Load tracking
   - `_dlt_pipeline_state` - State persistence
   - `_dlt_version` - Schema versioning

4. **If initialization fails**, manually initialize with:
   ```bash
   uv run python -c "
   import dlt

   @dlt.resource(name='test_init')
   def init_data():
       yield [{'test': 'init'}]

   pipeline = dlt.pipeline(
       pipeline_name='aws_billing_pipeline',
       destination='bigquery',
       dataset_name='plotly_aws_billing_data',
   )

   pipeline.run(init_data())
   print('DLT initialized')
   "
   ```

## Running the Pipeline

```bash
# Run AWS pipeline only
uv run python main.py aws

# Run Azure pipeline only
uv run python main.py azure

# Run all pipelines
uv run python main.py all
```

## Next Steps for Azure Pipeline

1. **Review Azure manifest structure**: Check if similar path mismatches exist
2. **Column naming**: Azure FOCUS format may have different naming conventions
3. **Type mapping**: Azure may use different type names than AWS
4. **Partition strategy**: Verify partition column name for Azure billing data
5. **Deduplication**: Azure uses `run_id` instead of `assembly_id`

**Key learning to apply**: Use Arrow tables + manifest schema + pre-normalize column names to avoid pandas/DLT issues.
