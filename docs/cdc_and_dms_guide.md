# CDC & AWS DMS Guide

This document explains a practical approach to implement Change Data Capture (CDC) for a PostgreSQL `orders` table into S3 (streaming) and how to replicate back from S3 → PostgreSQL by batch. It also includes a sample AWS DMS JSON task config and operational recommendations.

## Goals
- Replicate changes on `orders` (INSERT/UPDATE/DELETE) from a PostgreSQL source to an S3 sink in near-real-time using CDC streaming.
- Periodically (e.g. hourly) load accumulated S3 files back into PostgreSQL as a batched upsert job.

## Architecture (high level)

1. PostgreSQL primary database (source). Table: `public.orders` (has a stable primary key column `order_id`).
2. AWS DMS Replication Instance: performs initial full load (optional) then CDC streaming.
3. Source Endpoint: PostgreSQL connection (user, password, host, port, database).
4. Target Endpoint: S3 (DMS writes change records to S3 in chosen format — e.g. Apache Parquet or CSV/JSON).
5. Downstream consumer: AWS Glue / EMR / Athena / Glue ETL jobs which convert S3 files into a canonical table or generate SQL upserts that an hourly job runs against Postgres.

## Why DMS → S3 for CDC?
- DMS supports continuous replication from many databases including Postgres using logical decoding and WAL streaming.
- S3 is durable, cheap, and integrates well with analytics (Athena, Redshift Spectrum, Glue).
- By writing CDC events to S3 you can both (a) keep a raw event log, and (b) create compact, partitioned files suitable for downstream batch processing.

## DMS Task Mode
- Use `Full load + CDC` for the first run to capture a consistent baseline and then stream incremental changes.
- Alternatively, use `CDC only` if you already have a baseline loaded by another process.

## Recommended S3 Layout
- Bucket: s3://your-bucket/dms/orders/
- Prefix by date/hour for easier batch processing and compaction:
  - s3://your-bucket/dms/orders/year=YYYY/month=MM/day=DD/hour=HH/part-xxxxx.parquet
- Use Parquet with Snappy compression for efficient storage and read performance (recommended for analytical consumers). For strict record-level replay you may choose JSON with one event per object.

## DMS Considerations
- Primary key: ensure source table has a stable PK. DMS uses PK to apply updates/deletes.
- DDL changes: DMS handles some schema changes, but plan for schema evolution (Glue Catalog, schema registry).
- Deletes: DMS can emit delete events; your downstream loader must handle them (e.g., run delete SQL or mark soft-deleted).
- Ordering & idempotency: downstream consumers should process events idempotently (use PK + source op type + LSN/sequence id) to avoid duplicates.
- Latency: replication latency depends on replication instance class, network, and WAL generation rate.

## Sample AWS DMS JSON Task (skeleton)

Replace placeholders (region, ARNs, bucket name) with real values. This file demonstrates the essentials: full load + CDC and an S3 target endpoint.

```json
{
  "ReplicationTaskIdentifier": "dms-orders-to-s3",
  "ReplicationInstanceArn": "arn:aws:dms:REGION:ACCOUNT:rep:REPL_INSTANCE_ID",
  "SourceEndpointArn": "arn:aws:dms:REGION:ACCOUNT:endpoint:SOURCE_ENDPOINT_ID",
  "TargetEndpointArn": "arn:aws:dms:REGION:ACCOUNT:endpoint:TARGET_ENDPOINT_ID",
  "MigrationType": "full-load-and-cdc",
  "TableMappings": {
    "rules": [
      {
        "rule-type": "selection",
        "rule-id": "1",
        "rule-name": "1",
        "object-locator": {
          "schema-name": "public",
          "table-name": "orders"
        },
        "rule-action": "include"
      }
    ]
  },
  "ReplicationTaskSettings": "{ \"TargetMetadata\": { \"TargetSchema\": \"\" }, \"FullLoadSettings\": { \"TargetTablePrepMode\": \"DO_NOTHING\" } }"
}
```

Note: The AWS Console or CLI is usually used to create endpoints and tasks; the JSON above shows the important fields and mapping rules. DMS also supports extra settings (e.g., `ParquetSettings`) for configuring S3 format.

## Processing/Loading Back from S3 → PostgreSQL (Batch, hourly)

Approach overview:

1. Hourly job triggers (Airflow) to catalog new S3 objects under the prefix `dms/orders/year=.../`.
2. Use AWS Glue / Athena to convert raw CDC event files into a canonical delta table (parquet) or a deduplicated snapshot per `order_id` (keeping latest op/LSN per id).
3. Create upsert SQL using INSERT ... ON CONFLICT (Postgres) or use a staging table + MERGE-like logic:
   - COPY stage: COPY staging_table FROM 's3://...' USING appropriate format (via `aws_s3.table_import_from_s3` in RDS/Redshift or using a client to stream parquet).
   - Upsert: INSERT INTO orders (...) SELECT ... FROM staging_table ON CONFLICT (order_id) DO UPDATE SET ...; handle deletes explicitly.

Simpler option (when dataset small):
- Glue job produces a CSV/Parquet of the latest rows per PK for the hour. The batch job connects to Postgres and runs parameterized upserts.

## Security and Permissions
- Ensure DMS replication instance has an IAM role that allows writing to the S3 bucket and reading from the source DB (network & security groups).
- Lock down the S3 prefix and use SSE (server-side encryption) for sensitive data.

## Monitoring & Cost
- Monitor DMS CloudWatch metrics (ReplicationLatency, FullLoadProgressPercent, CDCLatency).
- Costs: DMS replication instances (hourly), S3 storage and requests, Glue/Athena compute for transforms and queries.

## Example Operational Playbook
1. Create/validate Postgres logical replication slot and permission for the DMS user (REPLICATION role).  
2. Create DMS source endpoint (Postgres) with the logical replication enabled.  
3. Create DMS target endpoint (S3) with bucket, IAM role, and format settings (Parquet recommended).  
4. Create replication instance sized appropriately (start small: dms.t3.medium).  
5. Create and start replication task (Full load + CDC).  
6. Build hourly Airflow job that compacts and applies S3 delta files back to Postgres.

## Shortcomings / Caveats
- DMS is not a transactional replication tool — eventual consistency is expected.
- Schema evolution requires planning; DMS may fail or drop columns if unexpected changes occur.
- For very high change volumes (100K+ events/sec) consider streaming architecture (Debezium → Kafka → Kinesis Firehose) instead of DMS.

## References
- AWS DMS documentation: https://docs.aws.amazon.com/dms/latest/userguide/Welcome.html
- Best practices for CDC: https://aws.amazon.com/blogs/database/change-data-capture-with-debezium/
