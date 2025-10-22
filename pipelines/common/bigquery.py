"""BigQuery partition management utilities."""

from google.cloud import bigquery


class PartitionManager:
    """Manage BigQuery table partitions for billing data."""

    def __init__(self, project_id: str, dataset: str):
        """
        Initialize partition manager.

        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset = dataset

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the dataset."""
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        try:
            self.client.get_table(table_id)
            return True
        except Exception:
            return False

    def delete_partition(
        self, table_name: str, partition_column: str, partition_value: str
    ) -> None:
        """
        Delete all rows in a specific partition using DELETE statement.

        This is more performant than DROP TABLE for partition deletion.
        If the DELETE covers all rows in a partition, BigQuery removes the
        entire partition without scanning bytes (free operation).

        Args:
            table_name: Table name (without project/dataset prefix)
            partition_column: Name of the partitioning column
            partition_value: Partition value (e.g., '2025-09-01' for monthly partition)
        """
        # Check if table exists first
        if not self.table_exists(table_name):
            print(f"Table {table_name} does not exist yet, skipping partition deletion")
            return

        query = f"""
        DELETE FROM `{self.project_id}.{self.dataset}.{table_name}`
        WHERE DATE({partition_column}) = '{partition_value}'
        """
        print(f"Deleting partition {partition_value} from {table_name}...")
        query_job = self.client.query(query)
        result = query_job.result()
        print(f"Deleted {result.total_rows} rows from partition {partition_value}")

    def partition_exists(
        self, table_name: str, partition_column: str, partition_value: str
    ) -> bool:
        """
        Check if a partition has data.

        Args:
            table_name: Table name (without project/dataset prefix)
            partition_column: Name of the partitioning column
            partition_value: Partition value to check

        Returns:
            True if partition exists with data, False otherwise
        """
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.project_id}.{self.dataset}.{table_name}`
        WHERE DATE({partition_column}) = '{partition_value}'
        """

        try:
            query_job = self.client.query(query)
            result = query_job.result()
            row = next(result)
            return row.count > 0
        except Exception:
            # Table might not exist yet
            return False
