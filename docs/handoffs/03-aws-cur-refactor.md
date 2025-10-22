# AWS CUR Pipeline Refactor - Hybrid Approach

**Date**: 2025-10-22
**Status**: ✅ Complete and running
**Change**: Refactored to match Azure's hybrid approach (DLT state + BigQuery native load)

## What Changed

**Before**: Download CSV → Parse with PyArrow → Upload to BigQuery via DLT
**After**: BigQuery loads CSV directly from GCS URIs → DLT tracks state only

**Result**: Zero data copying, faster loads, consistent architecture with Azure

## Key Implementation

### 1. Schema Builder (`_build_bigquery_schema`)
```python
# Reads AWS manifest → Builds BigQuery schema with normalized column names
AWS_TO_BIGQUERY_TYPES = {
    "String": "STRING",
    "BigDecimal": "FLOAT64",
    "DateTime": "TIMESTAMP",
    "OptionalString": "STRING",
    "Interval": "STRING",
}
```

### 2. CSV Header Handling
- CSV headers: `identity/LineItemId`, `bill/InvoiceId`
- Our schema: `identity_lineitemid`, `bill_invoiceid`
- **Solution**: `skip_leading_rows=1` - ignore CSV header, use our normalized schema

### 3. Schema Evolution
- AWS adds columns over time (e.g., `product_free_query_types`)
- **Solution**: `schema_update_options=[ALLOW_FIELD_ADDITION]`

### 4. Column Name Normalization
BigQuery auto-normalizes:
- `bill/BillingPeriodStartDate` → `bill_billing_period_start_date`
- NOT `bill_billingperiodstartdate` (adds underscores between camelCase words)

## Critical Changes

### pipelines/aws_cur.py
- Removed: PyArrow CSV parsing, Arrow table operations
- Added: `_build_bigquery_schema()`, `_build_gcs_uris()`
- Changed: Resource yields `dict` (tracking records) not `pa.Table`
- Load config:
  ```python
  job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.CSV,
      schema=schema,  # Our normalized schema
      skip_leading_rows=1,  # Skip CSV header
      schema_update_options=[ALLOW_FIELD_ADDITION],
  )
  ```

### Partition Column
- Old: `bill_billingperiodstartdate`
- New: `bill_billing_period_start_date`

## Files Modified

- `pipelines/aws_cur.py` - Complete rewrite using hybrid approach
- Type mapping changed from PyArrow to BigQuery types

## Running

```bash
uv run python main.py aws
```

## Verification

First run loads all 39 months from GCS directly to BigQuery.
Second run skips already-loaded assembly_ids (DLT state works).

## Benefits vs Old Approach

✅ No CSV download/parsing overhead
✅ No PyArrow dependency for AWS pipeline
✅ Faster - BigQuery native CSV load is optimized
✅ Consistent with Azure pipeline architecture
✅ Same state management pattern across both pipelines
