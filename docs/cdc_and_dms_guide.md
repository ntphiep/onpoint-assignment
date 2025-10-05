# CDC & AWS DMS Implementation Guide

## Overview

Change Data Capture (CDC) and AWS Data Migration Service (DMS) are essential technologies for building real-time data pipelines and maintaining data synchronization between different systems. This document outlines how to implement bidirectional replication between PostgreSQL and S3 using CDC streaming and batch processing.

## Table of Contents

1. [CDC Streaming: PostgreSQL → S3](#cdc-streaming-postgresql--s3)
2. [Batch Replication: S3 → PostgreSQL](#batch-replication-s3--postgresql)
3. [AWS DMS Configuration](#aws-dms-configuration)
4. [Implementation Architecture](#implementation-architecture)
5. [Monitoring and Operations](#monitoring-and-operations)

---

## CDC Streaming: PostgreSQL → S3

### Architecture Overview

```
PostgreSQL → WAL Logical Replication → AWS DMS → Kinesis Data Streams → S3
```

### Implementation Steps

#### 1. PostgreSQL Configuration

First, configure PostgreSQL for logical replication:

```sql
-- Enable logical replication in postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- Create replication slot
SELECT pg_create_logical_replication_slot('orders_slot', 'pgoutput');

-- Grant necessary permissions
GRANT REPLICATION ON DATABASE your_database TO dms_user;
GRANT SELECT ON orders TO dms_user;
```

#### 2. AWS DMS Replication Instance

```json
{
  "ReplicationInstanceClass": "dms.t3.medium",
  "AllocatedStorage": 100,
  "VpcSecurityGroupIds": ["sg-12345678"],
  "ReplicationSubnetGroupIdentifier": "dms-subnet-group",
  "MultiAZ": false,
  "PubliclyAccessible": false,
  "EngineVersion": "3.4.7"
}
```

#### 3. Source Endpoint (PostgreSQL)

```json
{
  "EndpointIdentifier": "postgres-source",
  "EndpointType": "source",
  "EngineName": "postgres",
  "Username": "dms_user",
  "Password": "your_password",
  "ServerName": "your-postgres-host.rds.amazonaws.com",
  "Port": 5432,
  "DatabaseName": "your_database",
  "PostgreSQLSettings": {
    "DatabaseName": "your_database",
    "Username": "dms_user",
    "Password": "your_password",
    "ServerName": "your-postgres-host.rds.amazonaws.com",
    "Port": 5432,
    "SlotName": "orders_slot",
    "PluginName": "pglogical"
  }
}
```

#### 4. Target Endpoint (S3)

```json
{
  "EndpointIdentifier": "s3-target",
  "EndpointType": "target",
  "EngineName": "s3",
  "S3Settings": {
    "BucketName": "your-data-lake-bucket",
    "BucketFolder": "cdc/orders",
    "DataFormat": "parquet",
    "CompressionType": "gzip",
    "EnableStatistics": true,
    "IncludeOpForFullLoad": true,
    "TimestampColumnName": "cdc_timestamp",
    "DatePartitionEnabled": true,
    "DatePartitionSequence": "YYYYMMDD",
    "ParquetTimestampInMillisecond": true
  }
}
```

#### 5. Replication Task Configuration

```json
{
  "ReplicationTaskIdentifier": "orders-cdc-stream",
  "SourceEndpointArn": "arn:aws:dms:region:account:endpoint:postgres-source",
  "TargetEndpointArn": "arn:aws:dms:region:account:endpoint:s3-target", 
  "ReplicationInstanceArn": "arn:aws:dms:region:account:rep:instance",
  "MigrationType": "cdc",
  "TableMappings": {
    "rules": [
      {
        "rule-type": "selection",
        "rule-id": "1",
        "rule-name": "orders-table",
        "object-locator": {
          "schema-name": "public",
          "table-name": "orders"
        },
        "rule-action": "include"
      }
    ]
  },
  "ReplicationTaskSettings": {
    "TargetMetadata": {
      "TargetSchema": "",
      "SupportLobs": true,
      "FullLobMode": false,
      "LobChunkSize": 64,
      "LimitedSizeLobMode": true,
      "LobMaxSize": 32
    },
    "FullLoadSettings": {
      "TargetTablePrepMode": "DO_NOTHING",
      "CreatePkAfterFullLoad": false,
      "StopTaskCachedChangesApplied": false,
      "StopTaskCachedChangesNotApplied": false,
      "MaxFullLoadSubTasks": 8,
      "TransactionConsistencyTimeout": 600,
      "CommitRate": 10000
    },
    "Logging": {
      "EnableLogging": true,
      "LogComponents": [
        {
          "Id": "SOURCE_UNLOAD",
          "Severity": "LOGGER_SEVERITY_DEFAULT"
        },
        {
          "Id": "TARGET_LOAD",
          "Severity": "LOGGER_SEVERITY_DEFAULT"
        }
      ]
    },
    "ChangeProcessingTuning": {
      "BatchApplyEnabled": true,
      "BatchApplyPreserveTransaction": true,
      "BatchSplitSize": 0,
      "BatchApplyTimeoutMin": 1,
      "BatchApplyTimeoutMax": 30,
      "BatchApplyMemoryLimit": 500,
      "StatementCacheSize": 50
    }
  }
}
```

### Data Format in S3

The CDC stream will create files in S3 with the following structure:

```
s3://your-data-lake-bucket/
├── cdc/orders/2024/01/15/
│   ├── 20240115-101530-001.parquet
│   ├── 20240115-101531-002.parquet
│   └── ...
└── cdc/orders/2024/01/16/
    ├── 20240116-090000-001.parquet
    └── ...
```

Each Parquet file contains:
- Original table columns
- `Op` column: I (Insert), U (Update), D (Delete)
- `cdc_timestamp`: When the change was captured

---

## Batch Replication: S3 → PostgreSQL

### Architecture Overview

```
S3 → Lambda/Glue ETL Job → PostgreSQL (Hourly Batch)
```

### Implementation Options

#### Option 1: AWS Lambda + S3 Event Triggers

```python
import json
import boto3
import psycopg2
import pandas as pd
from io import BytesIO

def lambda_handler(event, context):
    """
    Lambda function triggered every hour to process S3 data
    """
    s3_client = boto3.client('s3')
    
    # Get files from the last hour
    bucket = 'your-data-lake-bucket'
    prefix = f'processed/orders/{datetime.now().strftime("%Y/%m/%d")}'
    
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    
    for obj in response.get('Contents', []):
        # Process each file
        process_s3_file(bucket, obj['Key'])

def process_s3_file(bucket, key):
    """Process individual S3 file and load to PostgreSQL"""
    s3_client = boto3.client('s3')
    
    # Read Parquet file from S3
    response = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(BytesIO(response['Body'].read()))
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD']
    )
    
    # Bulk insert with COPY
    df.to_sql('orders_staging', conn, if_exists='replace', index=False)
    
    # Execute merge operation
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO orders (id, customer_id, order_date, amount, status, updated_at)
            SELECT id, customer_id, order_date, amount, status, updated_at
            FROM orders_staging
            ON CONFLICT (id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                order_date = EXCLUDED.order_date,
                amount = EXCLUDED.amount,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at;
        """)
    
    conn.commit()
    conn.close()
```

#### Option 2: AWS Glue ETL Job

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Read from S3
s3_path = "s3://your-data-lake-bucket/processed/orders/"
df = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [s3_path],
        "recurse": True,
        "groupFiles": "inPartition"
    },
    format="parquet"
)

# Transform data
transformed_df = df.apply_mapping([
    ("id", "long", "id", "long"),
    ("customer_id", "long", "customer_id", "long"),
    ("order_date", "timestamp", "order_date", "timestamp"),
    ("amount", "decimal", "amount", "decimal"),
    ("status", "string", "status", "string")
])

# Write to PostgreSQL
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=transformed_df,
    catalog_connection="postgresql-connection",
    connection_options={
        "dbtable": "orders",
        "database": "your_database",
        "preactions": "CREATE TABLE IF NOT EXISTS orders_temp AS SELECT * FROM orders WHERE 1=0"
    },
    transformation_ctx="write_to_postgres"
)

job.commit()
```

#### Option 3: EventBridge + Step Functions

```json
{
  "Comment": "Hourly S3 to PostgreSQL batch replication",
  "StartAt": "ProcessS3Data",
  "States": {
    "ProcessS3Data": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "s3-to-postgres-processor",
        "Payload": {
          "bucket": "your-data-lake-bucket",
          "prefix": "processed/orders/",
          "target_table": "orders"
        }
      },
      "Next": "ValidateLoad"
    },
    "ValidateLoad": {
      "Type": "Task", 
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "data-validation-function"
      },
      "End": true
    }
  }
}
```

---

## AWS DMS Configuration

### Complete DMS Setup Script

```bash
#!/bin/bash

# Create DMS subnet group
aws dms create-replication-subnet-group \\
    --replication-subnet-group-identifier dms-subnet-group \\
    --replication-subnet-group-description "DMS subnet group" \\
    --subnet-ids subnet-12345678 subnet-87654321

# Create replication instance
aws dms create-replication-instance \\
    --replication-instance-identifier orders-replication-instance \\
    --replication-instance-class dms.t3.medium \\
    --allocated-storage 100 \\
    --vpc-security-group-ids sg-12345678 \\
    --replication-subnet-group-identifier dms-subnet-group

# Create source endpoint
aws dms create-endpoint \\
    --endpoint-identifier postgres-source \\
    --endpoint-type source \\
    --engine-name postgres \\
    --username dms_user \\
    --password your_password \\
    --server-name your-postgres-host.rds.amazonaws.com \\
    --port 5432 \\
    --database-name your_database

# Create target endpoint  
aws dms create-endpoint \\
    --endpoint-identifier s3-target \\
    --endpoint-type target \\
    --engine-name s3 \\
    --s3-settings BucketName=your-data-lake-bucket,BucketFolder=cdc/orders,DataFormat=parquet

# Create replication task
aws dms create-replication-task \\
    --replication-task-identifier orders-cdc-task \\
    --source-endpoint-arn arn:aws:dms:region:account:endpoint:postgres-source \\
    --target-endpoint-arn arn:aws:dms:region:account:endpoint:s3-target \\
    --replication-instance-arn arn:aws:dms:region:account:rep:orders-replication-instance \\
    --migration-type cdc \\
    --table-mappings file://table-mappings.json \\
    --replication-task-settings file://task-settings.json
```

### Monitoring and Alerting

```json
{
  "CloudWatchMetrics": [
    "CDCLatencySource",
    "CDCLatencyTarget", 
    "FreshmanCBMLatency",
    "FreshmanCBMRecords"
  ],
  "CloudWatchAlarms": [
    {
      "AlarmName": "DMS-HighLatency",
      "MetricName": "CDCLatencyTarget",
      "Threshold": 300,
      "ComparisonOperator": "GreaterThanThreshold"
    },
    {
      "AlarmName": "DMS-TaskFailure",
      "MetricName": "ReplicationTaskStatus",
      "Threshold": 1,
      "ComparisonOperator": "LessThanThreshold"
    }
  ]
}
```

---

## Implementation Architecture

### High-Level Architecture Diagram

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   PostgreSQL    │───▶│   AWS DMS    │───▶│   S3 Data Lake  │
│   (Orders Table)│    │ (CDC Stream) │    │   (Parquet)     │
└─────────────────┘    └──────────────┘    └─────────────────┘
                                                     │
                                                     ▼
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   PostgreSQL    │◀───│ Lambda/Glue  │◀───│   EventBridge   │
│(Analytics Tables)│    │(Batch ETL)   │    │ (Hourly Trigger)│
└─────────────────┘    └──────────────┘    └─────────────────┘
```

### Data Flow Details

1. **Real-time CDC Stream (PostgreSQL → S3)**
   - PostgreSQL WAL captures all changes
   - DMS reads WAL via logical replication slot
   - Changes streamed to S3 in near real-time
   - Data partitioned by date for efficient querying

2. **Batch Processing (S3 → PostgreSQL)**
   - EventBridge triggers hourly processing
   - Lambda/Glue reads processed data from S3
   - Data transformed and loaded to analytics tables
   - UPSERT operations handle incremental updates

### Performance Considerations

#### CDC Streaming Optimization

```json
{
  "StreamBufferSettings": {
    "StreamBufferCount": 3,
    "StreamBufferSizeInMB": 8,
    "CtrlStreamBufferSizeInMB": 5
  },
  "ChangeProcessingTuning": {
    "BatchApplyEnabled": true,
    "BatchApplyPreserveTransaction": true,
    "BatchSplitSize": 0,
    "BatchApplyTimeoutMin": 1,
    "BatchApplyTimeoutMax": 30,
    "MinTransactionSize": 1000,
    "CommitTimeout": 1,
    "MemoryLimitTotal": 1024,
    "MemoryKeepTime": 60,
    "StatementCacheSize": 50
  }
}
```

#### Batch Processing Optimization

```python
# Optimized batch processing configuration
BATCH_PROCESSING_CONFIG = {
    "batch_size": 10000,
    "parallel_workers": 4,
    "memory_limit_mb": 2048,
    "connection_pool_size": 10,
    "retry_attempts": 3,
    "checkpoint_interval": 1000
}
```

---

## Monitoring and Operations

### Key Metrics to Monitor

1. **CDC Latency**: Time between source change and S3 availability
2. **Throughput**: Records per second processed
3. **Error Rate**: Failed replication attempts
4. **Data Quality**: Schema validation and data consistency checks

### Operational Procedures

#### Daily Operations Checklist

```bash
#!/bin/bash
# Daily DMS health check script

echo "DMS Replication Task Status:"
aws dms describe-replication-tasks \\
    --filters Name=replication-task-id,Values=orders-cdc-task \\
    --query 'ReplicationTasks[0].Status'

echo "Latest CDC Latency:"
aws cloudwatch get-metric-statistics \\
    --namespace AWS/DMS \\
    --metric-name CDCLatencyTarget \\
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \\
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \\
    --period 3600 \\
    --statistics Average

echo "Data Quality Validation:"
psql -h $DB_HOST -d $DB_NAME -c "
    SELECT 
        COUNT(*) as total_records,
        MAX(updated_at) as latest_update,
        COUNT(DISTINCT DATE(updated_at)) as days_with_data
    FROM orders;"
```

#### Disaster Recovery

```sql
-- Emergency CDC restart procedure
-- 1. Stop replication task
-- 2. Create new replication slot
SELECT pg_create_logical_replication_slot('orders_slot_backup', 'pgoutput');

-- 3. Update DMS task configuration
-- 4. Resume replication from backup slot

-- 5. Validate data consistency
SELECT 
    source_count,
    target_count,
    source_count - target_count as difference
FROM (
    SELECT COUNT(*) as source_count FROM orders
) source
CROSS JOIN (
    SELECT COUNT(*) as target_count 
    FROM s3_external_table  -- Via Spectrum/Athena
) target;
```

### Cost Optimization

1. **DMS Instance Sizing**: Start with t3.medium, scale based on throughput
2. **S3 Storage Classes**: Use Intelligent Tiering for automatic cost optimization
3. **Parquet Compression**: Use Snappy for balance of speed and compression
4. **Batch Processing**: Optimize Lambda/Glue job frequency based on SLA requirements

---

## Conclusion

This CDC and DMS implementation provides:

- **Real-time streaming** from PostgreSQL to S3 for immediate data availability
- **Hourly batch processing** from S3 back to PostgreSQL for analytics workloads
- **Fault tolerance** with monitoring and alerting
- **Scalability** to handle growing data volumes
- **Cost optimization** through appropriate service sizing and storage classes

The bidirectional replication ensures data consistency while enabling both operational and analytical use cases.