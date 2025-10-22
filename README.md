- I want to build a couple of data pipelines to ingest some cloud billing data from a GCS bucket into BigQuery.
- I want to use the DLT python library to handle as much of this as possible
- The tricky part of this job will be avoiding the loading of data unless necessary, but DLT might make that easier.
- In particular, we will be loading AWS billing data.  the tricks for this are as follows:
    - For each run of the full pipeline, I want to check for the existence of "billing manifest" files.  These are used to uniquely identify a versioned set of billing data.  You can get the idea of how this works by reading this file: https://raw.githubusercontent.com/JGrubb/open-finops-pipelines/refs/heads/main/finops/services/manifest_discovery.py
    - If this is a new execution, I want to load the billing data that is listed in the manifest file itself, appending the execution_id or assembly_id depending on the version
    - If this execution has already been loaded into the target DB, we will identify that by either using some DLT primitive that you'll need to discover for me, or by simply doing a mapping query of something like
    ```
    SELECT
        execution_id,
        DATE(billing_period_start_date) as month,
        count(*)
    from {dest table}
    group by all
    ```
    - I'd prefer to use DLT primitives where possible.
    - the schema of the monthly billing data can change at random, so we must be prepared to handle that.  Again DLT should have built in primitives for handling this.  The manifest file also lists out columns and in more recent versions the data types of the columns.  you'll need to ensure that you manage BigQuery's schema accordingly - possibly easy because of "allow column additions" configs.
    - We will partition the underlying table by billing month start date, and cluster by usage start date.
    - We should load from most recent to oldest, because of the data typing not being present in older manifests
    - the source format could be CSV or it could be Parquet.  The manifest may tell us, or it may not.
    - There are two different versions of the AWS CUR export, we'll only be working with v1 to begin with, but you should plan on implementing v2 down the road.
    - Each month is entirely regenerated each day, so an entire month's billing data can be dropped and reloaded from each new execution.  Ths will likely be more performant than DLT's native deduplication behavior.
- Azure follows largely the same plan, but the schema doesn't change.  It also proivides a manifest file that we should inspect.

Because we will be using entirely GCP infra for this, the confiuguration should be fairly minimal:
- for each vendor, a source bucket, directory, and name of the export.  
From there you should be able to pull the manifest and proceed.

Use `uv run` and keep the footprint as minimal as possible.  Ideally we'll extend this to AWS and Azure billing data, setting up native GCP data transfers into GCS buckets, with Bigquery as the destination.