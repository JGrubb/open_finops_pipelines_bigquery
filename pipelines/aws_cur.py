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
        prefix: GCS prefix path (e.g., "gcs-transfer/aws_cur")
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
    return resource_func(bucket, prefix, table_name, project_id, dataset)


def aws_billing_resource(
    bucket: str,
    prefix: str,
    table_name: str,
    project_id: str,
    dataset: str,
) -> Iterator[dict]:
    """
    Load AWS billing data using BigQuery native LOAD from GCS.

    Uses DLT state to track loaded executions and avoid duplicates.
    BigQuery loads gzipped CSV files directly from GCS URIs (zero data copying).

    Args:
        bucket: GCS bucket name
        prefix: GCS prefix path
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
    manifests = list(discovery.discover_aws_manifests(prefix))

    print(f"Discovered {len(manifests)} AWS CUR manifests")

    # Initialize BigQuery client and partition manager
    bq_client = bigquery.Client(project=project_id)
    partition_manager = PartitionManager(project_id, dataset)

    for manifest in manifests:
        billing_month = manifest.billing_month
        assembly_id = manifest.assembly_id

        # Check if already loaded (DLT state check)
        if assembly_id in loaded_executions.get(billing_month, []):
            print(f"Skipping {billing_month} (assembly_id: {assembly_id}) - already loaded")
            continue

        print(f"Processing {billing_month} (assembly_id: {assembly_id})")

        # Delete existing partition before loading
        partition_date = f"{billing_month}-01"
        partition_manager.delete_partition(
            table_name, "bill_billing_period_start_date", partition_date
        )

        # Build GCS URIs from manifest - handle path mismatch
        gcs_uris = _build_gcs_uris(bucket, manifest)

        print(f"  Loading {len(gcs_uris)} CSV files from GCS...")
        for uri in gcs_uris:
            print(f"    {uri}")

        # Build BigQuery schema from manifest with normalized column names
        schema = _build_bigquery_schema(manifest)

        # Configure BigQuery load job for gzipped CSV
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,  # Use our normalized schema
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,  # AWS adds columns over time
            ],
            skip_leading_rows=1,  # Skip CSV header row (has original names with slashes)
            allow_jagged_rows=False,
            allow_quoted_newlines=True,
            field_delimiter=",",
        )

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

        # Mark as loaded in DLT state
        if billing_month not in loaded_executions:
            loaded_executions[billing_month] = []
        loaded_executions[billing_month].append(assembly_id)

        print(f"Completed loading {billing_month} (assembly_id: {assembly_id})")

        # Yield minimal tracking record (DLT will persist state after run completes)
        yield {
            "assembly_id": assembly_id,
            "billing_month": billing_month,
            "loaded_at": datetime.now(timezone.utc),
            "row_count": load_job.output_rows,
            "file_count": len(gcs_uris),
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


def _build_gcs_uris(bucket: str, manifest: AWSManifest) -> list[str]:
    """
    Build GCS URIs from AWS CUR manifest report keys.

    AWS manifests contain reportKeys with original S3 paths, but files are
    actually in the same directory as the manifest in GCS.

    Args:
        bucket: GCS bucket name
        manifest: AWS manifest object with manifest_path and report_keys

    Returns:
        List of GCS URIs (gs://bucket/path/file.csv.gz)
    """
    # Get directory containing the manifest
    # manifest_path format: gcs-transfer/aws_cur/YYYYMMDD-YYYYMMDD/aws-billing-csv-Manifest.json
    manifest_dir = "/".join(manifest.manifest_path.split("/")[:-1])

    uris = []
    for file_key in manifest.report_keys:
        # Extract just the filename from the reportKey path
        filename = file_key.split("/")[-1]

        # Construct actual GCS path
        gcs_path = f"{manifest_dir}/{filename}"
        uri = f"gs://{bucket}/{gcs_path}"
        uris.append(uri)

    return uris


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
