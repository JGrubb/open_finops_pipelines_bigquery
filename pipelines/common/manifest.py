"""Manifest discovery and parsing for cloud billing data in GCS buckets."""

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

from google.cloud import storage


@dataclass
class AWSManifest:
    """AWS CUR manifest metadata."""

    assembly_id: str
    billing_month: str  # YYYY-MM format
    report_keys: list[str]
    columns: list[dict]
    manifest_path: str
    compression: str
    content_type: str

    @property
    def billing_date(self) -> datetime:
        """Parse billing month as datetime."""
        return datetime.strptime(self.billing_month, "%Y-%m")


@dataclass
class AzureManifest:
    """Azure billing manifest metadata."""

    run_id: str
    billing_month: str  # YYYY-MM format
    blobs: list[dict]
    manifest_path: str
    file_format: str
    submitted_time: str  # ISO timestamp from runInfo.submittedTime

    @property
    def billing_date(self) -> datetime:
        """Parse billing month as datetime."""
        return datetime.strptime(self.billing_month, "%Y-%m")

    @property
    def submitted_datetime(self) -> datetime:
        """Parse submitted_time as datetime."""
        # Handle both formats: with and without fractional seconds
        # "2025-10-16T03:48:05.0084806Z" or "2025-10-16T03:48:05Z"
        time_str = self.submitted_time.rstrip('Z')
        if '.' in time_str:
            # Azure timestamps have 7 decimal places, but Python's %f only handles 6
            # Truncate to 6 digits
            parts = time_str.split('.')
            if len(parts[1]) > 6:
                time_str = f"{parts[0]}.{parts[1][:6]}"
            return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f")
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")


class ManifestDiscovery:
    """Discover and parse billing manifest files from GCS buckets."""

    def __init__(self, bucket_name: str):
        """
        Initialize manifest discovery.

        Args:
            bucket_name: GCS bucket name (without gs:// prefix)
        """
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def discover_aws_manifests(self, prefix: str, export_name: str = None) -> Iterator[AWSManifest]:
        """
        Discover AWS CUR v1 manifest files in GCS.

        GCS transfer path pattern:
        {prefix}/YYYYMMDD-YYYYMMDD/*-Manifest.json

        Args:
            prefix: GCS prefix path (e.g., "gcs-transfer/aws_cur")
            export_name: Legacy parameter, not used for GCS transfers

        Yields:
            AWSManifest objects sorted by billing period (newest first)
        """
        # Pattern for GCS-transferred files: gcs-transfer/aws_cur/YYYYMMDD-YYYYMMDD/*-Manifest.json
        # Match any file ending in -Manifest.json in a date-range directory
        pattern = re.compile(
            rf"^{re.escape(prefix.rstrip('/'))}/"
            r"(\d{8})-(\d{8})/"
            r".*-Manifest\.json$"
        )

        manifests = []
        for blob in self.bucket.list_blobs(prefix=prefix):
            if match := pattern.match(blob.name):
                manifest_data = json.loads(blob.download_as_text())

                # Extract billing month from billingPeriod.start
                billing_period_start = manifest_data["billingPeriod"]["start"]
                # Format: "20250901T000000.000Z" -> "2025-09"
                billing_month = f"{billing_period_start[:4]}-{billing_period_start[4:6]}"

                manifests.append(
                    AWSManifest(
                        assembly_id=manifest_data["assemblyId"],
                        billing_month=billing_month,
                        report_keys=manifest_data["reportKeys"],
                        columns=manifest_data.get("columns", []),
                        manifest_path=blob.name,
                        compression=manifest_data.get("compression", "GZIP"),
                        content_type=manifest_data.get("contentType", "text/csv"),
                    )
                )

        # Sort by billing period, newest first
        manifests.sort(key=lambda m: m.billing_date, reverse=True)
        yield from manifests

    def discover_azure_manifests(self, prefix: str, export_name: str) -> Iterator[AzureManifest]:
        """
        Discover Azure billing manifest files.

        Azure path pattern:
        {prefix}/{export_name}/YYYYMMDD-YYYYMMDD/YYYYMMDDHHmm/{run_id}/manifest.json

        For each billing month, returns ONLY the most recent manifest based on
        runInfo.submittedTime. This handles the case where Azure exports don't
        delete old versions and multiple manifests exist for the same month.

        Args:
            prefix: GCS prefix path
            export_name: Azure export name

        Yields:
            AzureManifest objects sorted by billing period (newest first),
            with only the most recent manifest per month
        """
        # Pattern: gcs-transfer/azure/billingdata/{export-name}/
        #          20251001-20251031/202510210349/aa7e.../manifest.json
        pattern = re.compile(
            rf"^{re.escape(prefix)}/{re.escape(export_name)}/"
            r"(\d{8})-(\d{8})/"  # date range
            r"\d{12}/"  # timestamp (YYYYMMDDHHmm)
            r"[a-f0-9\-]+/"  # run_id (UUID)
            r"manifest\.json$"
        )

        all_manifests = []
        for blob in self.bucket.list_blobs(prefix=f"{prefix}/{export_name}/"):
            if pattern.match(blob.name):
                manifest_data = json.loads(blob.download_as_text())

                # Extract billing month from runInfo.startDate
                start_date = manifest_data["runInfo"]["startDate"]
                # Format: "2025-10-01T00:00:00" -> "2025-10"
                billing_month = start_date[:7]

                all_manifests.append(
                    AzureManifest(
                        run_id=manifest_data["runInfo"]["runId"],
                        billing_month=billing_month,
                        blobs=manifest_data["blobs"],
                        manifest_path=blob.name,
                        file_format=manifest_data["deliveryConfig"]["fileFormat"],
                        submitted_time=manifest_data["runInfo"]["submittedTime"],
                    )
                )

        # Group by billing month and select most recent per month
        from itertools import groupby

        # Sort by billing month (newest first), then by submitted time (newest first)
        all_manifests.sort(
            key=lambda m: (m.billing_date, m.submitted_datetime),
            reverse=True
        )

        # Group by billing month and take the first (most recent) from each group
        manifests = []
        for billing_month, group in groupby(all_manifests, key=lambda m: m.billing_month):
            most_recent = next(group)  # First item is most recent due to sorting
            manifests.append(most_recent)

            # Log skipped manifests for visibility
            skipped = list(group)
            if skipped:
                print(f"  Found {len(skipped)} older manifest(s) for {billing_month}, using most recent (submitted: {most_recent.submitted_time})")

        yield from manifests
