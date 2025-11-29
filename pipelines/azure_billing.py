"""Azure billing pipeline to BigQuery."""

from datetime import datetime, timezone
from typing import Iterator

import dlt
from google.cloud import bigquery

from pipelines.common.bigquery import PartitionManager
from pipelines.common.manifest import AzureManifest, ManifestDiscovery


@dlt.source
def azure_billing_source(
    bucket: str = dlt.config.value,
    prefix: str = dlt.config.value,
    export_name: str = dlt.config.value,
    table_name: str = dlt.config.value,
    project_id: str = dlt.config.value,
    dataset: str = dlt.config.value,
):
    """
    Azure billing data source.

    Hybrid approach: Uses DLT for state management ONLY.
    Actual data loading happens via BigQuery native LOAD from GCS URIs.
    This avoids unnecessary data copying since Parquet files are already in GCS.

    Config is read from sources.azure_billing section.

    Args:
        bucket: GCS bucket name
        prefix: GCS prefix path
        export_name: Azure export name
        table_name: BigQuery table name
        project_id: BigQuery project ID
        dataset: BigQuery dataset

    Yields:
        DLT resources for state tracking
    """
    # Apply DLT resource decorator - this creates a tracking table
    # The actual billing data is loaded directly via BigQuery
    resource_func = dlt.resource(
        azure_billing_resource,
        name=f"{table_name}_load_tracking",
        write_disposition="append",
    )
    return resource_func(bucket, prefix, export_name, table_name, project_id, dataset)


def azure_billing_resource(
    bucket: str,
    prefix: str,
    export_name: str,
    table_name: str,
    project_id: str,
    dataset: str,
) -> Iterator[dict]:
    """
    Load Azure billing data using BigQuery native LOAD from GCS.

    Uses DLT state to track loaded executions and avoid duplicates.
    BigQuery loads Parquet files directly from GCS URIs (zero data copying).

    Args:
        bucket: GCS bucket name
        prefix: GCS prefix path
        export_name: Azure export name
        table_name: BigQuery table name (actual billing data table)
        project_id: BigQuery project ID
        dataset: BigQuery dataset name

    Yields:
        Minimal tracking records for DLT state management
    """
    # Access DLT state for this resource
    state = dlt.current.resource_state()
    loaded_executions = state.setdefault("loaded_executions", {})

    # Track run statistics for data quality monitoring
    run_stats = {
        "total_rows": 0,
        "manifests_processed": 0,
        "manifests_skipped": 0,
    }

    # Discover manifests (newest first)
    discovery = ManifestDiscovery(bucket)
    manifests = list(discovery.discover_azure_manifests(prefix, export_name))

    print(f"Discovered {len(manifests)} Azure billing manifests")

    # Initialize BigQuery client and partition manager
    bq_client = bigquery.Client(project=project_id)
    partition_manager = PartitionManager(project_id, dataset)

    for manifest in manifests:
        billing_month = manifest.billing_month
        run_id = manifest.run_id
        submitted_time = manifest.submitted_time

        # Check if already loaded (DLT state check)
        # New state structure: {"2025-10": {"run_id": "...", "submitted_time": "..."}}
        # Old state structure: {"2025-10": ["run_id1", "run_id2", ...]}
        if billing_month in loaded_executions:
            existing_entry = loaded_executions[billing_month]

            # Handle backward compatibility with old list-based state
            if isinstance(existing_entry, list):
                # Old format: check if run_id is in the list
                if run_id in existing_entry:
                    print(f"Skipping {billing_month} (run_id: {run_id}) - already loaded")
                    run_stats["manifests_skipped"] += 1
                    continue
                else:
                    print(f"Found newer manifest for {billing_month} (run_id: {run_id}), will reload")
            else:
                # New format: dict with run_id and submitted_time
                if existing_entry["run_id"] == run_id:
                    print(f"Skipping {billing_month} (run_id: {run_id}) - already loaded")
                    run_stats["manifests_skipped"] += 1
                    continue
                else:
                    print(f"Found newer manifest for {billing_month} (run_id: {run_id}), will reload")

        print(f"Processing {billing_month} (run_id: {run_id}, submitted: {submitted_time})")

        # Delete existing partition before loading
        # Azure FOCUS uses BillingPeriodStart column
        partition_date = f"{billing_month}-01"
        partition_manager.delete_partition(
            table_name, "billingperiodstart", partition_date
        )

        # Build GCS URIs from manifest - handle path mismatch
        gcs_uris = _build_gcs_uris(bucket, manifest)

        print(f"  Loading {len(gcs_uris)} Parquet files from GCS...")
        for uri in gcs_uris:
            print(f"    {uri}")

        # Configure BigQuery load job
        # Use BIGNUMERIC for decimal128 columns (required for Azure FOCUS cost fields)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # Map Parquet decimal128(38,18) to BigQuery BIGNUMERIC instead of NUMERIC
            decimal_target_types=[bigquery.DecimalTargetType.BIGNUMERIC],
        )

        # Load directly from GCS URIs (no data download/upload!)
        table_id = f"{project_id}.{dataset}.{table_name}"
        load_job = bq_client.load_table_from_uri(
            gcs_uris,
            table_id,
            job_config=job_config,
        )

        # Wait for load to complete
        load_job.result()

        print(f"  Loaded {load_job.output_rows} rows to {table_name}")

        # Mark as loaded in DLT state (hybrid approach: track run_id + timestamp)
        loaded_executions[billing_month] = {
            "run_id": run_id,
            "submitted_time": submitted_time,
        }

        print(f"Completed loading {billing_month} (run_id: {run_id})")

        # Update run statistics
        run_stats["total_rows"] += load_job.output_rows
        run_stats["manifests_processed"] += 1

        # Yield minimal tracking record (DLT will persist state after run completes)
        yield {
            "run_id": run_id,
            "billing_month": billing_month,
            "loaded_at": datetime.now(timezone.utc),
            "row_count": load_job.output_rows,
            "file_count": len(gcs_uris),
        }

    # Always yield a run summary record for data quality monitoring
    # This ensures we have a record even when all manifests are skipped (0 rows loaded)
    run_date = datetime.now(timezone.utc).date().isoformat()
    yield {
        "run_date": run_date,
        "total_rows": run_stats["total_rows"],
        "manifests_processed": run_stats["manifests_processed"],
        "manifests_skipped": run_stats["manifests_skipped"],
        "is_run_summary": True,
        "loaded_at": datetime.now(timezone.utc),
    }

    print(f"\nRun summary: {run_stats['total_rows']} rows loaded, "
          f"{run_stats['manifests_processed']} processed, "
          f"{run_stats['manifests_skipped']} skipped")


def _build_gcs_uris(bucket: str, manifest: AzureManifest) -> list[str]:
    """
    Build GCS URIs from Azure manifest blob information.

    Azure manifests contain blobName paths like:
      billingdata/{export-name}/20251001-20251031/...

    But files in GCS are at:
      gcs-transfer/azure/billingdata/{export-name}/20251001-20251031/...

    This handles the path mismatch by constructing URIs relative to manifest location.

    Args:
        bucket: GCS bucket name
        manifest: Azure manifest object with manifest_path and blobs

    Returns:
        List of GCS URIs (gs://bucket/path/file.parquet)
    """
    # Get directory containing the manifest
    # manifest_path format: gcs-transfer/azure/billingdata/export-name/YYYYMMDD-YYYYMMDD/timestamp/run_id/manifest.json
    manifest_dir = "/".join(manifest.manifest_path.split("/")[:-1])

    uris = []
    for blob_info in manifest.blobs:
        # blobName format: billingdata/export-name/YYYYMMDD-YYYYMMDD/timestamp/run_id/part_X_XXXX.snappy.parquet
        # Extract just the filename
        filename = blob_info["blobName"].split("/")[-1]

        # Construct actual GCS path
        gcs_path = f"{manifest_dir}/{filename}"
        uri = f"gs://{bucket}/{gcs_path}"
        uris.append(uri)

    return uris
