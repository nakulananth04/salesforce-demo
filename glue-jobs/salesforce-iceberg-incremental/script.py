from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, expr
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import boto3
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice

# Parse required args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
if '--iceberg_warehouse_path' in sys.argv:
    args.update(getResolvedOptions(sys.argv, ['iceberg_warehouse_path']))
else:
    args['iceberg_warehouse_path'] = 's3://salesforce-marketing-processed/iceberg/'

# Fetch ICEBERG_LAST_UPDATED_TIMESTAMP from AWS SSM Parameter Store
ssm = boto3.client('ssm')
param_name = 'ICEBERG_LAST_UPDATED_TIMESTAMP'
try:
    response = ssm.get_parameter(Name=param_name)
    ICEBERG_LAST_UPDATED_TIMESTAMP = response['Parameter']['Value'].strip()
    print(f"INFO: Fetched ICEBERG_LAST_UPDATED_TIMESTAMP from SSM: {ICEBERG_LAST_UPDATED_TIMESTAMP}")
except ssm.exceptions.ParameterNotFound:
    ICEBERG_LAST_UPDATED_TIMESTAMP = ''
    print("WARN: ICEBERG_LAST_UPDATED_TIMESTAMP not found in SSM, proceeding with full load")

# Initialize Spark
conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_warehouse_path'])
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.catalog.glue_catalog.write.distribution-mode", "none")
conf.set("spark.sql.iceberg.fanout-enabled", "true")



sc = SparkContext(conf=conf)
spark = SparkSession(sc)
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def log(message, level="info"):
    print(f"{level.upper()}: {message}")

import boto3
s3 = boto3.client('s3')

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch										  
																  
def process_table_for_timestamp(table, timestamp):
    table_name = str(table['table_name'])
    log(f"  Processing table: {table_name}")

    try:
        s3_path = f"s3://{source_bucket}/{timestamp}/{table_name}.parquet"
        df = spark.read.parquet(s3_path)

        # Rename and cast
        actual_columns = [c.lower() for c in df.columns]
        for c in df.columns:
            df = df.withColumnRenamed(c, c.lower())

        for field, dtype in table["fields"].items():
            if field.lower() in actual_columns:
                df = df.withColumn(field.lower(), col(field.lower()).cast(dtype))
            else:
                df = df.withColumn(field.lower(), lit(None).cast(dtype))

        if "partition_exprs" in table:
            for col_name, expr_str in table["partition_exprs"].items():
                df = df.withColumn(col_name.lower(), expr(expr_str))

        if "sort_cols" in table:
            sort_cols = [c.lower() for c in table["sort_cols"]]
            df = df.orderBy(*sort_cols)

        iceberg_path = f"glue_catalog.salesforce.{str(table_name)}"
        df.writeTo(iceberg_path) \
          .partitionedBy(*[c.lower() for c in table["partition_cols"]]) \
          .createOrReplace()

        log(f"  Successfully processed {table_name} for {timestamp}")
    except Exception as e:
        log(f"  ERROR processing {table_name} in {timestamp}: {e}", "error")

def list_timestamp_folders(bucket, prefix):
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter='/'
    )

    timestamps = set()
    for cp in response.get("CommonPrefixes", []):
        folder = cp["Prefix"].strip("/").split("/")[-1]
        timestamps.add(folder)
    return sorted(list(timestamps))

source_bucket = 'salesforce-marketing-cleaned-data'
timestamp_folders = list_timestamp_folders(source_bucket, '')
log("Timestamp Folders")
log(timestamp_folders)

if len(ICEBERG_LAST_UPDATED_TIMESTAMP)>1:
    log(f"Found LAST_UPDATED_TIMESTAMP: {ICEBERG_LAST_UPDATED_TIMESTAMP}")
    timestamp_folders = [f for f in timestamp_folders if f > ICEBERG_LAST_UPDATED_TIMESTAMP]
    log(timestamp_folders)

if not timestamp_folders:
    log("No new timestamp folders to process. Exiting.")
    sys.exit(0)

											 
tables = [
    {
        "table_name": "account",
        "fields": {
            "ACCOUNT_ID": "STRING",
            "ACCOUNT_NAME": "STRING",
            "INDUSTRY": "STRING",
            "WEBSITE": "STRING",
            "ANNUAL_REVENUE": "DECIMAL(15,2)",
            "CREATED_DATE": "STRING"
        },
        "partition_cols": ["created_year"],
        "partition_exprs": {
            "created_year": "date_trunc('year', from_unixtime(CAST(CREATED_DATE AS BIGINT)/1000000000))"
        },
        "sort_cols": ["CREATED_DATE"],
        "primary_key": "ACCOUNT_ID"
    },
    {
        "table_name": "campaign",
        "fields": {
            "CAMPAIGN_ID": "STRING",
            "CAMPAIGN_NAME": "STRING",
            "START_DATE": "STRING",
            "END_DATE": "STRING",
            "STATUS": "STRING",
            "BUDGET": "DECIMAL(15,2)"
        },
        "partition_cols": ["status", "start_month"],
        "partition_exprs": {
            "start_month": "date_trunc('month', from_unixtime(CAST(START_DATE AS BIGINT)/1000000000))"
        },
        "sort_cols": ["status","START_DATE"],
        "primary_key": "CAMPAIGN_ID"
    },
    {
        "table_name": "contact",
        "fields": {
            "CONTACT_ID": "STRING",
            "FIRST_NAME": "STRING",
            "LAST_NAME": "STRING",
            "EMAIL": "STRING",
            "PHONE": "STRING",
            "CREATED_DATE": "STRING"
        },
        "partition_cols": ["created_month"],
        "partition_exprs": {
            "created_month": "date_trunc('month', from_unixtime(CAST(CREATED_DATE AS BIGINT)/1000000000))"
        },
        "sort_cols": ["CREATED_DATE"],
        "primary_key": "CONTACT_ID"
    },
    {
        "table_name": "lead",
        "fields": {
            "LEAD_ID": "STRING",
            "FIRST_NAME": "STRING",
            "LAST_NAME": "STRING",
            "COMPANY": "STRING",
            "EMAIL": "STRING",
            "PHONE": "STRING",
            "STATUS": "STRING",
            "CONVERTED_CONTACT_ID": "STRING",
            "CREATED_DATE": "STRING"
        },
        "partition_cols": ["status", "created_month"],
        "partition_exprs": {
            "created_month": "date_trunc('month', from_unixtime(CAST(CREATED_DATE AS BIGINT)/1000000000))"
        },
        "sort_cols": ["STATUS", "CREATED_DATE"],
        "primary_key": "LEAD_ID"
    },
    {
        "table_name": "campaign_member",
        "fields": {
            "MEMBER_ID": "STRING",
            "CAMPAIGN_ID": "STRING",
            "CONTACT_ID": "STRING",
            "LEAD_ID": "STRING",
            "STATUS": "STRING",
            "FIRST_RESPONDED_DATE": "STRING",
            "CREATED_DATE": "STRING"
        },
        "partition_cols": ["status", "created_month"],
        "partition_exprs": {
            "created_month": "date_trunc('month', from_unixtime(CAST(CREATED_DATE AS BIGINT)/1000000000))"
        },
        "sort_cols": ["STATUS", "CREATED_DATE"],
        "primary_key": "MEMBER_ID"
    },
    {
        "table_name": "email_template",
        "fields": {
            "TEMPLATE_ID": "STRING",
            "TEMPLATE_NAME": "STRING",
            "SUBJECT": "STRING",
            "HTML_CONTENT": "STRING",
            "CREATED_DATE": "STRING"
        },
        "partition_cols": ["created_year"],
        "partition_exprs": {
            "created_year": "date_trunc('year', from_unixtime(CAST(CREATED_DATE AS BIGINT)/1000000000))"
        },
        "sort_cols": ["CREATED_DATE"],
        "primary_key": "TEMPLATE_ID"
    },
    {
        "table_name": "email_send",
        "fields": {
            "SEND_ID": "STRING",
            "CAMPAIGN_ID": "STRING",
            "EMAIL_TEMPLATE_ID": "STRING",
            "SEND_DATE": "STRING",
            "SUBJECT_LINE": "STRING",
            "TOTAL_SENT": "INT"
        },
        "partition_cols": ["SEND_DATE"],
        "sort_cols": ["SEND_DATE"],
        "primary_key": "SEND_ID"
    },
    {
        "table_name": "email_engagement",
        "fields": {
            "ENGAGEMENT_ID": "STRING",
            "SEND_ID": "STRING",
            "CONTACT_ID": "STRING",
            "CONTACT_SK": "STRING",
            "ENGAGEMENT_TYPE": "STRING",
            "ENGAGEMENT_TIMESTAMP": "STRING",
            "LINK_URL": "STRING"
        },
        "partition_cols": ["engagement_day"],
        "partition_exprs": {
            "engagement_day": "date_trunc('day', from_unixtime(CAST(ENGAGEMENT_TIMESTAMP AS BIGINT)/1000000000))"
        },
        "sort_cols": ["ENGAGEMENT_TIMESTAMP"],
        "primary_key": "ENGAGEMENT_ID"
    },
    {
        "table_name": "event",
        "fields": {
            "EVENT_ID": "STRING",
            "SUBJECT": "STRING",
            "START_DATE_TIME": "STRING",
            "END_DATE_TIME": "STRING",
            "TYPE": "STRING",
            "RELATED_CAMPAIGN_ID": "STRING",
            "RELATED_CONTACT_ID": "STRING",
            "RELATED_LEAD_ID": "STRING",
            "CREATED_DATE": "STRING"
        },
        "partition_cols": ["TYPE"],
        "sort_cols": ["TYPE"],
        "primary_key": "EVENT_ID"
    },
    {
        "table_name": "opportunity",
        "fields": {
            "OPPORTUNITY_ID": "STRING",
            "OPPORTUNITY_NAME": "STRING",
            "ACCOUNT_ID": "STRING",
            "STAGE_NAME": "STRING",
            "AMOUNT": "DECIMAL(15,2)",
            "CLOSE_DATE": "STRING",
            "CAMPAIGN_ID": "STRING",
            "CREATED_DATE": "STRING"
        },
        "partition_cols": ["STAGE_NAME"],
        "sort_cols": ["STAGE_NAME"],
        "primary_key": "OPPORTUNITY_ID"
    }
]


latest_processed = ''

for timestamp in timestamp_folders:
    log(f"Processing timestamp folder: {timestamp}")
    for table_batch in chunked(tables, 10):  # 10 threads max at a time
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(process_table_for_timestamp, table, timestamp)
                for table in table_batch
            ]
            for future in as_completed(futures):
                future.result()
                
    latest_processed = max(latest_processed, timestamp)

# Update the parameter store with the latest processed folder
if latest_processed:
    try:
        ssm.put_parameter(
            Name=param_name,
            Value=latest_processed,
            Type='String',
            Overwrite=True
        )
        log(f"Updated SSM Parameter: {param_name} = {latest_processed}")
    except Exception as e:
        log(f"Failed to update SSM parameter: {e}", "ERROR")

job.commit()
log("Job completed successfully")