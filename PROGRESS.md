# Progress Log

## 2025-11-28: AWS CUR Parquet Pipeline

### What Changed
Added auto-format detection to the AWS billing pipeline to support the new Parquet export format alongside the existing CSV format.

### Background
- The old AWS CUR export (`doitintl-awsops-2740`) was deactivated in November 2025
- A new export (`plotly_cur_export_2025`) was created with Parquet format and Hive-style partitioning
- Data location: `gs://plotly-bigquery-billing-finops-data/gcs-transfer/aws_doit/CUR/doitintl-awsops-2740/plotly_cur_export_2025/`

### Implementation
The pipeline now auto-detects format from the manifest's `contentType` field:
- `"Parquet"` -> Uses BigQuery Parquet loader (no schema needed, auto-detected)
- `"text/csv"` -> Uses BigQuery CSV loader with explicit schema

### Files Modified
- `pipelines/aws_cur.py` - Added format detection and Parquet loading functions
- `.dlt/config.toml` - Updated to new export path and table name
- `.dlt/secrets.toml` - Added explicit project_id for BigQuery destination
- `.env` - Updated AWS config variables

### Configuration Changes
```
AWS_GCS_PREFIX=gcs-transfer/aws_doit/CUR/doitintl-awsops-2740/plotly_cur_export_2025
AWS_EXPORT_NAME=plotly_cur_export_2025
AWS_TABLE_NAME=aws_cur_data_2025
```

### Test Results
- Loaded 38 Parquet files
- 35,998,734 rows loaded to `aws_cur_data_2025` table
- Completed in ~9 seconds
