"""AWS Cost and Usage Report (CUR) pipeline to BigQuery."""

import re
from datetime import datetime, timezone
from typing import Iterator

import dlt
from google.cloud import bigquery

from pipelines.common.bigquery import PartitionManager
from pipelines.common.manifest import AWSManifest, ManifestDiscovery

# AWS CUR type mapping to BigQuery types
AWS_TO_BIGQUERY_TYPES = {
    "String": "STRING",
    "BigDecimal": "BIGNUMERIC",
    "OptionalBigDecimal": "BIGNUMERIC",
    "DateTime": "TIMESTAMP",
    "OptionalString": "STRING",
    "Interval": "STRING",
}

# SQL reserved words that need suffix
SQL_RESERVED_WORDS = {
    "group", "order", "user", "table", "index", "key", "value", "timestamp",
    "date", "year", "month", "day", "hour", "minute", "second", "from", "to",
}


@dlt.source
def aws_billing_source(
    bucket: str = dlt.config.value,
    prefix: str = dlt.config.value,
    export_name: str = dlt.config.value,
    table_name: str = dlt.config.value,
    project_id: str = dlt.config.value,
    dataset: str = dlt.config.value,
):
    """
    AWS billing data source.

    Hybrid approach: Uses DLT for state management ONLY.
    Actual data loading happens via BigQuery native LOAD from GCS URIs.
    This avoids downloading/parsing CSV files locally.

    Config is read from sources.aws_cur section.

    Args:
        bucket: GCS bucket name
        prefix: GCS prefix path (e.g., "gcs-transfer/aws_cur/report-name")
        export_name: AWS CUR export name (e.g., "report-name")
        table_name: BigQuery table name
        project_id: BigQuery project ID
        dataset: BigQuery dataset

    Yields:
        DLT resources for state tracking
    """
    # Apply DLT resource decorator - this creates a tracking table
    # The actual billing data is loaded directly via BigQuery
    resource_func = dlt.resource(
        aws_billing_resource,
        name=f"{table_name}_load_tracking",
        write_disposition="append",
    )
    return resource_func(bucket, prefix, export_name, table_name, project_id, dataset)


def aws_billing_resource(
    bucket: str,
    prefix: str,
    export_name: str,
    table_name: str,
    project_id: str,
    dataset: str,
) -> Iterator[dict]:
    """
    Load AWS billing data using BigQuery native LOAD from GCS.

    Uses DLT state to track loaded executions and avoid duplicates.
    Auto-detects format (CSV or Parquet) from manifest contentType field.
    BigQuery loads files directly from GCS URIs (zero data copying).

    Args:
        bucket: GCS bucket name
        prefix: GCS prefix path
        export_name: AWS CUR export name
        table_name: BigQuery table name (actual billing data table)
        project_id: BigQuery project ID
        dataset: BigQuery dataset name

    Yields:
        Minimal tracking records for DLT state management
    """
    # Access DLT state for this resource
    state = dlt.current.resource_state()
    loaded_executions = state.setdefault("loaded_executions", {})

    # Discover manifests (newest first)
    discovery = ManifestDiscovery(bucket)
    manifests = list(discovery.discover_aws_manifests(prefix, export_name))

    print(f"Discovered {len(manifests)} AWS CUR manifests")

    # Initialize BigQuery client and partition manager
    bq_client = bigquery.Client(project=project_id)
    partition_manager = PartitionManager(project_id, dataset)

    for manifest in manifests:
        billing_month = manifest.billing_month
        assembly_id = manifest.assembly_id

        # Check if already loaded (DLT state check)
        # New state structure: {"2025-10": {"assembly_id": "...", "loaded_at": "..."}}
        # Old state structure: {"2025-10": ["assembly_id1", "assembly_id2", ...]}
        if billing_month in loaded_executions:
            existing_entry = loaded_executions[billing_month]

            # Handle backward compatibility with old list-based state
            if isinstance(existing_entry, list):
                # Old format: check if assembly_id is in the list
                if assembly_id in existing_entry:
                    print(f"Skipping {billing_month} (assembly_id: {assembly_id}) - already loaded")
                    continue
                else:
                    print(f"Found newer manifest for {billing_month} (assembly_id: {assembly_id}), will reload")
            else:
                # New format: dict with assembly_id and loaded_at
                if existing_entry["assembly_id"] == assembly_id:
                    print(f"Skipping {billing_month} (assembly_id: {assembly_id}) - already loaded")
                    continue
                else:
                    print(f"Found newer manifest for {billing_month} (assembly_id: {assembly_id}), will reload")

        print(f"Processing {billing_month} (assembly_id: {assembly_id})")

        # Delete existing partition before loading
        partition_date = f"{billing_month}-01"
        partition_manager.delete_partition(
            table_name, "bill_billing_period_start_date", partition_date
        )

        # Auto-detect format from manifest contentType
        is_parquet = manifest.content_type == "Parquet"

        if is_parquet:
            # Parquet format: build URIs and config for Parquet files
            gcs_uris = _build_parquet_gcs_uris(bucket, manifest)
            print(f"  Loading {len(gcs_uris)} Parquet files from GCS...")
            for uri in gcs_uris[:5]:  # Show first 5
                print(f"    {uri}")
            if len(gcs_uris) > 5:
                print(f"    ... and {len(gcs_uris) - 5} more")
            job_config = _build_parquet_job_config()
        else:
            # CSV format: build URIs, schema, and config for CSV files
            gcs_uris = _build_csv_gcs_uris(bucket, manifest)
            print(f"  Loading {len(gcs_uris)} CSV files from GCS...")
            for uri in gcs_uris:
                print(f"    {uri}")
            schema = _build_bigquery_schema(manifest)
            job_config = _build_csv_job_config(schema)

        # Load directly from GCS URIs (no data download!)
        table_id = f"{project_id}.{dataset}.{table_name}"
        load_job = bq_client.load_table_from_uri(
            gcs_uris,
            table_id,
            job_config=job_config,
        )

        # Wait for load to complete
        load_job.result()

        print(f"  Loaded {load_job.output_rows} rows to {table_name}")

        # Mark as loaded in DLT state (hybrid approach: track assembly_id + timestamp)
        loaded_executions[billing_month] = {
            "assembly_id": assembly_id,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }

        print(f"Completed loading {billing_month} (assembly_id: {assembly_id})")

        # Yield minimal tracking record (DLT will persist state after run completes)
        yield {
            "assembly_id": assembly_id,
            "billing_month": billing_month,
            "loaded_at": datetime.now(timezone.utc),
            "row_count": load_job.output_rows,
            "file_count": len(gcs_uris),
            "format": "parquet" if is_parquet else "csv",
        }


def _normalize_column_name(column_name: str) -> str:
    """Normalize AWS CUR column name to BigQuery-compatible format."""
    # Convert camelCase to snake_case
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', column_name)
    # Lowercase and replace non-alphanumeric with underscore
    name = name.lower()
    name = re.sub(r'[^a-z0-9]', '_', name)
    # Remove consecutive underscores and strip edges
    name = re.sub(r'_+', '_', name).strip('_')
    # Handle edge cases
    if not name:
        return "unknown_column"
    if name[0].isdigit():
        name = f"col_{name}"
    if name in SQL_RESERVED_WORDS:
        name = f"{name}_col"
    return name


def _resolve_duplicate_names(column_names: list[str]) -> list[str]:
    """Resolve duplicate column names by appending numeric suffixes."""
    seen = {}
    resolved = []
    for name in column_names:
        if name not in seen:
            seen[name] = 0
            resolved.append(name)
        else:
            seen[name] += 1
            resolved.append(f"{name}_{seen[name]}")
    return resolved


def _build_csv_gcs_uris(bucket: str, manifest: AWSManifest) -> list[str]:
    """
    Build GCS URIs from AWS CUR manifest report keys for CSV format.

    AWS manifests contain reportKeys with S3-style paths. The GCS Transfer Service
    replicates the directory structure, so we need to map the reportKey path to GCS.

    The reportKeys include the versioned subdirectory (assemblyId timestamp):
    "CUR/account/YYYYMMDD-YYYYMMDD/YYYYMMDDTHHmmssZ/file.csv.gz"

    Args:
        bucket: GCS bucket name
        manifest: AWS manifest object with manifest_path and report_keys

    Returns:
        List of GCS URIs (gs://bucket/path/file.csv.gz)
    """
    # Get base directory containing the manifest
    # manifest_path format: gcs-transfer/aws_doit/CUR/account/YYYYMMDD-YYYYMMDD/account-Manifest.json
    manifest_dir = "/".join(manifest.manifest_path.split("/")[:-1])

    uris = []
    for file_key in manifest.report_keys:
        # reportKey format: CUR/account/YYYYMMDD-YYYYMMDD/assemblyId/file.csv.gz
        # We need to extract the path after the date range to get: assemblyId/file.csv.gz

        # Find the versioned subdirectory and filename (everything after the date range)
        # Split by "/" and take the last 2 parts (assemblyId/filename)
        parts = file_key.split("/")
        if len(parts) >= 2:
            # Take last 2 parts: assemblyId and filename
            relative_path = "/".join(parts[-2:])
        else:
            # Fallback: just use the filename
            relative_path = parts[-1]

        # Construct actual GCS path
        gcs_path = f"{manifest_dir}/{relative_path}"
        uri = f"gs://{bucket}/{gcs_path}"
        uris.append(uri)

    return uris


def _build_parquet_gcs_uris(bucket: str, manifest: AWSManifest) -> list[str]:
    """
    Build GCS URIs from AWS CUR manifest report keys for Parquet format.

    Parquet exports use Hive-style partitioning with a different path structure:
    reportKey: "billing/aws-billing-cur/{export}/{export}/year=YYYY/month=MM/file.snappy.parquet"

    The manifest_path is at: {prefix}/YYYYMMDD-YYYYMMDD/{export}-Manifest.json
    The data files are at: {prefix}/{export}/year=YYYY/month=MM/file.snappy.parquet

    Args:
        bucket: GCS bucket name
        manifest: AWS manifest object with manifest_path and report_keys

    Returns:
        List of GCS URIs (gs://bucket/path/file.snappy.parquet)
    """
    # Get prefix directory (parent of the date-range directory)
    # manifest_path format: gcs-transfer/.../plotly_cur_export_2025/20251101-20251201/plotly_cur_export_2025-Manifest.json
    manifest_parts = manifest.manifest_path.split("/")
    # Remove the last 2 parts (date-range dir and manifest filename) to get prefix
    prefix_dir = "/".join(manifest_parts[:-2])

    uris = []
    for file_key in manifest.report_keys:
        # reportKey format: billing/aws-billing-cur/plotly_cur_export_2025/plotly_cur_export_2025/year=2025/month=11/file.snappy.parquet
        # We need: {export}/year=YYYY/month=MM/filename (last 4 parts)
        parts = file_key.split("/")
        if len(parts) >= 4:
            # Take last 4 parts: export_name/year=YYYY/month=MM/filename
            relative_path = "/".join(parts[-4:])
        else:
            # Fallback: just use the filename
            relative_path = parts[-1]

        # Construct actual GCS path
        gcs_path = f"{prefix_dir}/{relative_path}"
        uri = f"gs://{bucket}/{gcs_path}"
        uris.append(uri)

    return uris


def _build_csv_job_config(schema: list[bigquery.SchemaField]) -> bigquery.LoadJobConfig:
    """
    Build BigQuery load job config for gzipped CSV files.

    Args:
        schema: BigQuery schema fields with normalized column names

    Returns:
        LoadJobConfig for CSV format
    """
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        skip_leading_rows=1,  # Skip CSV header row
        allow_jagged_rows=False,
        allow_quoted_newlines=True,
        field_delimiter=",",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="bill_billing_period_start_date",
        ),
        clustering_fields=["line_item_usage_start_date"],
    )


def _build_parquet_job_config() -> bigquery.LoadJobConfig:
    """
    Build BigQuery load job config for Parquet files.

    Parquet is self-describing so no schema needed.
    BigQuery auto-detects column names and types from the Parquet file metadata.

    Returns:
        LoadJobConfig for Parquet format
    """
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="bill_billing_period_start_date",
        ),
        clustering_fields=["line_item_usage_start_date"],
    )


def _build_bigquery_schema(manifest: AWSManifest) -> list[bigquery.SchemaField]:
    """
    Build BigQuery schema from AWS CUR manifest.

    Applies column name normalization and duplicate resolution to match
    what BigQuery will see in the CSV headers.

    Args:
        manifest: AWS manifest object with column definitions

    Returns:
        List of BigQuery SchemaField objects
    """
    # First pass: collect original column names and types
    original_names = []
    types_map = {}

    for col in manifest.columns:
        # AWS CUR column names are formatted as "category/name"
        col_name = f"{col['category']}/{col['name']}"
        original_names.append(col_name)
        aws_type = col["type"]
        bq_type = AWS_TO_BIGQUERY_TYPES.get(aws_type, "STRING")
        types_map[col_name] = bq_type

    # Second pass: normalize column names and resolve duplicates
    normalized = [_normalize_column_name(name) for name in original_names]
    unique_names = _resolve_duplicate_names(normalized)

    # Third pass: build schema with normalized names
    schema = []
    for i, original_name in enumerate(original_names):
        normalized_name = unique_names[i]
        bq_type = types_map[original_name]

        schema.append(
            bigquery.SchemaField(
                name=normalized_name,
                field_type=bq_type,
                mode="NULLABLE",
            )
        )

    return schema
