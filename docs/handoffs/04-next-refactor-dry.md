# Next Refactor: DRY Up AWS + Azure Pipelines

**Date**: 2025-10-22
**Status**: Planning - handoff for next session
**Goal**: Extract common patterns from AWS and Azure pipelines into reusable modules

## Current State

Both pipelines use the **same hybrid pattern** but have duplicated code:
1. DLT state management (check/update loaded executions)
2. Partition deletion before load
3. GCS URI building from manifests
4. BigQuery native load job execution
5. Tracking record yielding

## Code Duplication Analysis

### Pattern 1: State Management
**Both pipelines do this:**
```python
state = dlt.current.resource_state()
loaded_executions = state.setdefault("loaded_executions", {})

if execution_id in loaded_executions.get(billing_month, []):
    continue  # Skip

# ... process ...

loaded_executions[billing_month].append(execution_id)
yield {"execution_id": execution_id, "loaded_at": datetime.now(timezone.utc), ...}
```

**Locations:**
- `pipelines/aws_cur.py:89-98, 155-168`
- `pipelines/azure_billing.py:77-79, 135-152`

### Pattern 2: Partition Management
**Both pipelines do this:**
```python
partition_manager = PartitionManager(project_id, dataset)
partition_date = f"{billing_month}-01"
partition_manager.delete_partition(table_name, partition_column, partition_date)
```

**Locations:**
- `pipelines/aws_cur.py:100-117`
- `pipelines/azure_billing.py:87-106`

### Pattern 3: GCS URI Building
**Both pipelines do this:**
```python
manifest_dir = "/".join(manifest.manifest_path.split("/")[:-1])
filename = file_key.split("/")[-1]
gcs_path = f"{manifest_dir}/{filename}"
uri = f"gs://{bucket}/{gcs_path}"
```

**Locations:**
- `pipelines/aws_cur.py:204-232` (`_build_gcs_uris`)
- `pipelines/azure_billing.py:152-190` (`_build_gcs_uris`)

### Pattern 4: BigQuery Load Execution
**Both pipelines do this:**
```python
bq_client = bigquery.Client(project=project_id)
job_config = bigquery.LoadJobConfig(...)
load_job = bq_client.load_table_from_uri(gcs_uris, table_id, job_config)
load_job.result()
print(f"  Loaded {load_job.output_rows} rows")
```

**Locations:**
- `pipelines/aws_cur.py:100, 131-151`
- `pipelines/azure_billing.py:88, 118-133`

### Pattern 5: DLT Source/Resource Boilerplate
**Both pipelines have:**
```python
@dlt.source
def billing_source(
    bucket: str = dlt.config.value,
    prefix: str = dlt.config.value,
    ...
):
    resource_func = dlt.resource(
        billing_resource,
        name=f"{table_name}_load_tracking",
        write_disposition="append",
    )
    return resource_func(...)
```

**Locations:**
- `pipelines/aws_cur.py:29-63`
- `pipelines/azure_billing.py:14-48`

## Differences to Preserve

### AWS-Specific
- CSV format with explicit schema
- `skip_leading_rows=1` (CSV header)
- `schema_update_options=[ALLOW_FIELD_ADDITION]`
- Schema builder from manifest (`_build_bigquery_schema`)
- Column name normalization logic

### Azure-Specific
- Parquet format (auto-schema)
- `decimal_target_types=[BIGNUMERIC]`
- No schema normalization needed

## Proposed Module Structure

```
pipelines/
├── common/
│   ├── bigquery.py          # ✅ Already exists (PartitionManager)
│   ├── manifest.py           # ✅ Already exists (ManifestDiscovery)
│   ├── state.py             # NEW - DLT state management wrapper
│   ├── loader.py            # NEW - BigQuery native load abstraction
│   └── schema.py            # NEW - Schema building utilities
├── aws_cur.py               # Slimmed down - uses common modules
└── azure_billing.py         # Slimmed down - uses common modules
```

## Proposed New Modules

### 1. `pipelines/common/state.py`
```python
class DLTStateManager:
    """Manages DLT state for tracking loaded executions."""

    def __init__(self, resource_state):
        self.state = resource_state
        self.loaded_executions = state.setdefault("loaded_executions", {})

    def is_loaded(self, billing_month: str, execution_id: str) -> bool:
        """Check if execution already loaded."""

    def mark_loaded(self, billing_month: str, execution_id: str):
        """Mark execution as loaded."""

    def build_tracking_record(self, execution_id, billing_month, row_count, file_count):
        """Build standard tracking record for DLT."""
```

### 2. `pipelines/common/loader.py`
```python
class BigQueryNativeLoader:
    """Loads data from GCS to BigQuery using native load jobs."""

    def __init__(self, project_id: str, dataset: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset = dataset

    def load_csv(self, table_name, gcs_uris, schema, **options):
        """Load gzipped CSV files with explicit schema."""

    def load_parquet(self, table_name, gcs_uris, **options):
        """Load Parquet files (auto-schema)."""

    def wait_for_completion(self, load_job) -> dict:
        """Wait for load job and return metadata."""
```

### 3. `pipelines/common/schema.py`
```python
class SchemaBuilder:
    """Build BigQuery schemas from manifests."""

    def __init__(self, type_mapping: dict, normalizer: callable):
        self.type_mapping = type_mapping
        self.normalizer = normalizer

    def from_manifest(self, manifest) -> list[bigquery.SchemaField]:
        """Build schema with normalized column names."""

def normalize_column_name(name: str, reserved_words: set) -> str:
    """Normalize column name (shared logic)."""

def resolve_duplicate_names(names: list[str]) -> list[str]:
    """Resolve duplicate column names."""
```

### 4. Updated Pipeline Structure
```python
# aws_cur.py (simplified)
from pipelines.common.state import DLTStateManager
from pipelines.common.loader import BigQueryNativeLoader
from pipelines.common.schema import SchemaBuilder, normalize_column_name

@dlt.resource(name=f"{table_name}_load_tracking", write_disposition="append")
def aws_billing_resource(...):
    # State management
    state_mgr = DLTStateManager(dlt.current.resource_state())

    # Loader
    loader = BigQueryNativeLoader(project_id, dataset)

    for manifest in manifests:
        if state_mgr.is_loaded(billing_month, assembly_id):
            continue

        # AWS-specific: build schema
        schema = schema_builder.from_manifest(manifest)

        # Load with AWS-specific options
        result = loader.load_csv(
            table_name, gcs_uris, schema,
            skip_leading_rows=1,
            schema_update_options=[ALLOW_FIELD_ADDITION]
        )

        state_mgr.mark_loaded(billing_month, assembly_id)
        yield state_mgr.build_tracking_record(assembly_id, billing_month, result)
```

## Implementation Plan

### Phase 1: Extract State Management
1. Create `pipelines/common/state.py`
2. Implement `DLTStateManager` class
3. Update AWS pipeline to use it
4. Update Azure pipeline to use it
5. Test both pipelines

### Phase 2: Extract Loader
1. Create `pipelines/common/loader.py`
2. Implement `BigQueryNativeLoader` class
3. Support both CSV and Parquet formats
4. Update both pipelines
5. Test both pipelines

### Phase 3: Extract Schema Building
1. Create `pipelines/common/schema.py`
2. Move normalization functions
3. Implement `SchemaBuilder` class
4. Update AWS pipeline (Azure doesn't need this)
5. Test AWS pipeline

### Phase 4: Clean Up
1. Remove duplicate code
2. Verify both pipelines work
3. Update documentation
4. Compare code size before/after

## Success Metrics

- **Code reduction**: Expect ~40% less code in pipeline files
- **Shared logic**: 5+ common patterns extracted
- **No behavior change**: Pipelines produce identical results
- **Maintainability**: Changes to common patterns update both pipelines

## Questions to Answer

1. Should `_build_gcs_uris` be in `loader.py` or `manifest.py`?
2. Keep separate `aws_billing_source` and `azure_billing_source` or unify?
3. Make `PartitionManager` part of `BigQueryNativeLoader`?
4. Keep partition column names in config or hardcode?

## Testing Strategy

After each phase:
```bash
# Clear state
rm -rf ~/.dlt/pipelines/aws_billing_pipeline
rm -rf ~/.dlt/pipelines/azure_billing_pipeline

# Run both pipelines
uv run python main.py aws
uv run python main.py azure

# Verify row counts match previous runs
# Verify state management still works (second run skips data)
```
