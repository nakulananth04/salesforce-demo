import sys
import re
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace
from pyspark.sql.types import TimestampType


# ---- Logging setup ----
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger("ParquetCleaner")

logger = setup_logging()

def log(msg, level="info"):
    getattr(logger, level.lower())(msg)

# ---- AWS clients ----
ssm_client = boto3.client('ssm', region_name='ap-south-1')
s3 = boto3.resource('s3', region_name='ap-south-1')

# ---- Schema definitions ----
schemas = {
    "account": {"ACCOUNT_ID": "string", "ACCOUNT_NAME": "string", "INDUSTRY": "string", "WEBSITE": "string", "ANNUAL_REVENUE": "float", "CREATED_DATE": "timestamp"},
    "campaign": {"CAMPAIGN_ID": "string", "CAMPAIGN_NAME": "string", "START_DATE": "timestamp", "END_DATE": "timestamp", "STATUS": "string", "BUDGET": "float"},
    "contact": {"CONTACT_ID": "string", "FIRST_NAME": "string", "LAST_NAME": "string", "EMAIL": "string", "PHONE": "string", "CREATED_DATE": "timestamp"},
    "lead": {"LEAD_ID": "string", "FIRST_NAME": "string", "LAST_NAME": "string", "COMPANY": "string", "EMAIL": "string", "PHONE": "string", "STATUS": "string", "CONVERTED_CONTACT_ID": "string", "CREATED_DATE": "timestamp"},
    "campaign_member": {"MEMBER_ID": "string", "CAMPAIGN_ID": "string", "CONTACT_ID": "string", "LEAD_ID": "string", "STATUS": "string", "FIRST_RESPONDED_DATE": "timestamp", "CREATED_DATE": "timestamp"},
    "email_template": {"TEMPLATE_ID": "string", "TEMPLATE_NAME": "string", "SUBJECT": "string", "HTML_CONTENT": "string", "CREATED_DATE": "timestamp"},
    "email_send": {"SEND_ID": "string", "CAMPAIGN_ID": "string", "EMAIL_TEMPLATE_ID": "string", "SEND_DATE": "timestamp", "SUBJECT_LINE": "string", "TOTAL_SENT": "int"},
    "email_engagement": {"ENGAGEMENT_ID": "string", "SEND_ID": "string", "CONTACT_ID": "string", "CONTACT_SK": "string", "ENGAGEMENT_TYPE": "string", "ENGAGEMENT_TIMESTAMP": "timestamp", "LINK_URL": "string"},
    "event": {"EVENT_ID": "string", "SUBJECT": "string", "START_DATE_TIME": "timestamp", "END_DATE_TIME": "timestamp", "TYPE": "string", "RELATED_CAMPAIGN_ID": "string", "RELATED_CONTACT_ID": "string", "RELATED_LEAD_ID": "string", "CREATED_DATE": "timestamp"},
    "opportunity": {"OPPORTUNITY_ID": "string", "OPPORTUNITY_NAME": "string", "ACCOUNT_ID": "string", "STAGE_NAME": "string", "AMOUNT": "float", "CLOSE_DATE": "timestamp", "CAMPAIGN_ID": "string", "CREATED_DATE": "timestamp"}
}

# ---- Utility Functions ----

def parse_timestamp(ts_str):
    """Parse timestamp strings of format YYYY-MM-DD_HHMMSS to datetime, else None."""
    if not re.match(r"^\d{4}-\d{2}-\d{2}_\d{6}$", ts_str):
        log(f"Skipping invalid timestamp format: {ts_str}", "warning")
        return None
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d_%H%M%S")
    except Exception as e:
        log(f"Failed to parse timestamp {ts_str}: {e}", "error")
        return None

def get_last_updated_timestamp(parameter_name="LAST_UPDATED_TIMESTAMP"):
    try:
        resp = ssm_client.get_parameter(Name=parameter_name)
        return resp['Parameter']['Value']
    except ssm_client.exceptions.ParameterNotFound:
        return None

def update_last_updated_timestamp(new_timestamp, parameter_name="LAST_UPDATED_TIMESTAMP"):
    ssm_client.put_parameter(
        Name=parameter_name,
        Value=new_timestamp,
        Type='String',
        Overwrite=True
    )

def list_timestamps(bucket_name, prefix=""):
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    timestamps = set()
    for cp in response.get("CommonPrefixes", []):
        folder = cp["Prefix"].strip("/").split("/")[-1]
        timestamps.add(folder)
    return sorted(list(timestamps))

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch

def safe_cast_column(df, col_name, dtype):
    from pyspark.sql.functions import col, lit, regexp_replace
    if col_name not in df.columns:
        return df.withColumn(col_name, lit(None).cast(dtype))
    try:
        if dtype == "int":
            return df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9]", "").cast("int"))
        elif dtype == "float":
            return df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9.]", "").cast("float"))
        elif dtype in ["date", "timestamp"]:
            return df.withColumn(col_name, col(col_name).cast(dtype))
        else:
            return df.withColumn(col_name, col(col_name).cast(dtype))
    except Exception as e:
        log(f"Failed to cast column {col_name} to {dtype}: {e}", "error")
        return df.withColumn(col_name, lit(None).cast(dtype))

def validate_dataframe(df, schema, table):
    print(f"Schema for table: {schema}")
    print(f"DataFrame columns: {df.columns}")
    missing_cols = [col for col in schema if col not in df.columns]
    extra_cols = [col for col in df.columns if col not in schema]
    log(f"Validating {table} table:")
    if missing_cols:
        log(f"  ❗ Missing Columns: {missing_cols}", "warning")
    if extra_cols:
        log(f"  ⚠️  Extra Columns (not defined in schema): {extra_cols}", "warning")
    return not missing_cols

def move_single_parquet_file(bucket_name, temp_prefix, final_path):
    bucket = s3.Bucket(bucket_name)
    parquet_obj = None
    for obj in bucket.objects.filter(Prefix=temp_prefix):
        if re.match(r".*part-.*\.parquet$", obj.key):
            parquet_obj = obj.key
            break
    if not parquet_obj:
        log(f"WARNING: No parquet part file found in temp folder {temp_prefix}. Skipping move.", "warning")
        return
    copy_source = {'Bucket': bucket_name, 'Key': parquet_obj}
    s3.Object(bucket_name, final_path).copy(copy_source)
    bucket.objects.filter(Prefix=temp_prefix).delete()

# ---- Core processing function ----

def process_table(spark, table, fields, timestamp_folder, bucket_name, raw_data_bucket, out_path):
    try:
        raw_data_path = f"s3://{raw_data_bucket}/{timestamp_folder}/{table}.parquet"
        log(f"Reading table: {table} from {raw_data_path}")
        df = spark.read.parquet(raw_data_path)

        # Filter out invalid timestamps if any
        timestamp_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, TimestampType)]
        for ts_col in timestamp_cols:
            df = df.filter(col(ts_col).cast("long") > 0)

        # Validate & clean
        validate_dataframe(df, fields, table)

        if df.rdd.isEmpty():
            log(f"DataFrame for table {table} at {timestamp_folder} is empty. Skipping write.")
            return

        df_single = df.coalesce(1)
        temp_output_prefix = f"{timestamp_folder}/tmp_{table}"
        temp_output_path = f"s3://{bucket_name}/{temp_output_prefix}"
        df_single.write.mode("overwrite").parquet(temp_output_path)

        final_s3_key = f"{timestamp_folder}/{table}.parquet"
        move_single_parquet_file(bucket_name, temp_output_prefix, final_s3_key)

        log(f"Successfully cleaned and saved {table} to {out_path}/{timestamp_folder}/{table}.parquet")
    except Exception as e:
        log(f"Failed to process {table} at timestamp {timestamp_folder}: {str(e)}", "error")

# ---- Main function for Glue job or local runs ----

def main(
    spark=None,
    job_name="ParquetCleanerJob",
    output_path="s3://salesforce-marketing-cleaned-data/",
    raw_data_bucket="salesforce-marketing-raw-data",
    max_workers=10,
    run_job_commit=True
):
    if spark is None:
        spark = SparkSession.builder.appName("ParquetCleaner").getOrCreate()

    out_path = output_path.rstrip("/")
    all_timestamps = list_timestamps(raw_data_bucket)

    last_updated_ts = get_last_updated_timestamp()
    # valid_timestamps = [ts for ts in all_timestamps if parse_timestamp(ts) is not None]
    valid_timestamps = [ts for ts in all_timestamps if isinstance(ts, str) and parse_timestamp(ts) is not None]


    if last_updated_ts:
        log(f"Found LAST_UPDATED_TIMESTAMP: {last_updated_ts}")
        # filtered_timestamps = [ts for ts in valid_timestamps if parse_timestamp(ts) > parse_timestamp(last_updated_ts)]
        filtered_timestamps = [
            ts for ts in valid_timestamps
            if parse_timestamp(ts) > last_updated_ts
        ]

    else:
        log("LAST_UPDATED_TIMESTAMP not found or empty, processing all valid timestamps")
        filtered_timestamps = valid_timestamps

    if not filtered_timestamps:
        log("No new timestamps found to process. Exiting.")
        if run_job_commit:
            # Glue job commit placeholder
            pass
        return

    latest_processed_ts = None
    for timestamp_folder in filtered_timestamps:
        log(f"Processing timestamp folder: {timestamp_folder}")

        # Process tables in parallel batches
        for table_batch in chunked(schemas.items(), max_workers):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        process_table,
                        spark, 
                        table, 
                        fields, 
                        timestamp_folder, 
                        out_path.split("//")[-1],  # bucket name for move_single_parquet_file
                        raw_data_bucket,
                        out_path
                    ): table for table, fields in table_batch
                }
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error processing table {futures[future]}: {e}", "error")

        latest_processed_ts = timestamp_folder

    if run_job_commit:
        if latest_processed_ts:
            update_last_updated_timestamp(latest_processed_ts)
        # Glue job commit placeholder
        log("Completed all timestamp folders processing.")

if __name__ == "__main__":
    main()