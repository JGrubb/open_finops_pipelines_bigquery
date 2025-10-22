"""Main entry point for cloud billing pipelines."""

import sys

import dlt

from pipelines.aws_cur import aws_billing_source
from pipelines.azure_billing import azure_billing_source

# Registry of available pipelines
# Format: 'name': (source_function, pipeline_name, description)
PIPELINES = {
    "aws": (
        aws_billing_source,
        "aws_billing_pipeline",
        "AWS Cost and Usage Report pipeline",
    ),
    "azure": (
        azure_billing_source,
        "azure_billing_pipeline",
        "Azure billing FOCUS export pipeline",
    ),
}


def run_pipeline(name: str) -> None:
    """
    Run a specific billing pipeline by name.

    Args:
        name: Pipeline name (e.g., 'aws', 'azure')

    Raises:
        KeyError: If pipeline name not found in registry
    """
    if name not in PIPELINES:
        available = ", ".join(PIPELINES.keys())
        raise ValueError(f"Unknown pipeline '{name}'. Available: {available}, all")

    source_func, pipeline_name, description = PIPELINES[name]

    print(f"\n{'=' * 60}")
    print(f"Starting: {description}")
    print(f"Pipeline: {pipeline_name}")
    print(f"{'=' * 60}\n")

    # Create source - DLT auto-injects config from sources.{module_name}
    source = source_func()

    # Get dataset name from source config using explicit path
    # e.g., sources.aws_cur.dataset or sources.azure_billing.dataset
    config_section = name if name != "aws" else "aws_cur"
    if name == "azure":
        config_section = "azure_billing"
    dataset_name = dlt.config[f"sources.{config_section}.dataset"]

    # Create pipeline with BigQuery destination
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="bigquery",
        dataset_name=dataset_name,
    )

    # Run the pipeline
    load_info = pipeline.run(source)

    print(f"\n{'=' * 60}")
    print(f"Completed: {description}")
    print(f"Load info: {load_info}")
    print(f"{'=' * 60}\n")


def run_all_pipelines() -> None:
    """Run all registered billing pipelines sequentially."""
    print(f"\nRunning all {len(PIPELINES)} billing pipelines...\n")

    failed = []
    for name in PIPELINES:
        try:
            run_pipeline(name)
        except Exception as e:
            print(f"\n❌ Pipeline '{name}' failed: {e}\n")
            failed.append(name)
            # Continue with other pipelines instead of stopping

    if failed:
        print(f"\n⚠️  {len(failed)} pipeline(s) failed: {', '.join(failed)}")
        sys.exit(1)
    else:
        print(f"\n✅ All {len(PIPELINES)} pipelines completed successfully!")


def main():
    """
    Main entry point.

    Usage:
        python main.py              # Run all pipelines
        python main.py all          # Run all pipelines
        python main.py aws          # Run only AWS pipeline
        python main.py azure        # Run only Azure pipeline
    """
    if len(sys.argv) > 1:
        pipeline_name = sys.argv[1]
    else:
        pipeline_name = "all"

    try:
        if pipeline_name == "all":
            run_all_pipelines()
        else:
            run_pipeline(pipeline_name)
    except ValueError as e:
        print(f"\nError: {e}\n")
        print("Usage:")
        print("  python main.py [pipeline_name]")
        print("\nAvailable pipelines:")
        for name, (_, _, description) in PIPELINES.items():
            print(f"  {name:10s} - {description}")
        print(f"  {'all':10s} - Run all pipelines (default)")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    main()
