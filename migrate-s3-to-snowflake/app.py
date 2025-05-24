import os
import boto3
import datetime
import time
from pyiceberg.catalog import load_catalog
from botocore.exceptions import ClientError
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Snowflake import with error handling
try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("Warning: snowflake-connector-python not installed. Snowflake loading disabled.")

# Configuration
CONFIG = {
    "CATALOG_NAME": os.environ['CATALOG_NAME'],
    "WAREHOUSE_PATH": os.environ['WAREHOUSE_PATH'],
    "GLUE_ENDPOINT": os.environ['GLUE_ENDPOINT'],
    "DYNAMODB_TABLE": os.environ['DYNAMODB_TABLE'],
    "DATABASE_NAME": os.environ['DATABASE_NAME'],
    "TABLES": ['email_template','campaign','contact','account','campaign_member',
               'email_send','lead','email_engagement','event','opportunity'],
    "LOAD_TO_SNOWFLAKE": SNOWFLAKE_AVAILABLE
}

dynamo_values_to_update = {}
dynamodb = boto3.resource('dynamodb')
snapshot_table = dynamodb.Table(CONFIG["DYNAMODB_TABLE"])
snowflake_conn = None
SNOWFLAKE_PASSWORD = ''


def lambda_handler(event,context):

    global SNOWFLAKE_PASSWORD
    global snowflake_conn
    SNOWFLAKE_PASSWORD = get_ssm_parameter('SNOWFLAKE_PASSWORD')

    if CONFIG.get('LOAD_TO_SNOWFLAKE', False):
        snowflake_conn = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=SNOWFLAKE_PASSWORD,
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
            database=os.environ['SNOWFLAKE_DATABASE'],
            schema=os.environ['SNOWFLAKE_SCHEMA'],
            role=os.environ['SNOWFLAKE_ROLE']
        )
    
    print("🚀 Starting Iceberg to Snowflake pipeline...")
    start_time = datetime.datetime.utcnow()
    
    result = process_all_tables()
    
    # Process and format the result for Lambda output
    duration = (datetime.datetime.utcnow() - start_time).total_seconds()
    print(f"Pipeline completed in {duration:.2f} seconds")
    print(f"Overall status: {result['status'].upper()}")
    if result['status'] == 'failed':
        print("\n❌ Pipeline failed completely:")
        print(f"Error: {result['error']}")
        return
        
    # Only show summary if we have one
    if 'summary' in result:
        print("\n📊 Summary:")
        print(f"• Tables processed: {result['summary']['tables_processed']}")
        print(f"• Successful tables: {result['summary']['tables_successful']}")
        print(f"• Tables with new files: {result['summary']['tables_with_new_files']}")
        if 'snowflake_load_success' in result['summary']:
            print(f"• Snowflake loads succeeded: {result['summary']['snowflake_load_success']}")
            print(f"• Total files loaded: {result['summary']['total_files_loaded']}")
    else:
        print("\n⚠️ No summary available (partial failure)")

    # 5. Detailed table results (if available)
    if 'results' in result:
        print("\n🔍 Detailed Results:")
        for table_name, table_result in result['results'].items():
            status_icon = "✅" if table_result.get('status') == 'success' else "❌"
            print(f"\n{status_icon} {table_name}:")
            
            if table_result.get('status') == 'success':
                print(f"   • New files detected: {table_result.get('count', 0)}")
                if 'snowflake_status' in table_result:
                    sf_status = table_result['snowflake_status']
                    print(f"   • Snowflake status: {sf_status.upper()}")
                    if sf_status == 'success':
                        print(f"   • Files loaded: {table_result.get('snowflake_loaded', 0)}")
            if 'snowflake_errors' in table_result and table_result['snowflake_errors']:
                print(f"   ⚠️  Load Errors ({len(table_result['snowflake_errors'])}):")
                for error in table_result['snowflake_errors'][:3]:  # Show first 3 errors
                    print(f"      - File: {error.get('file_path', 'unknown')}")
                    print(f"        Error: {error.get('error', 'Unknown error')}")
                    print(f"        Count: {error.get('count', 1)}")
                
                # Show S3 error log location if available
                if any('s3_key' in e for e in table_result['snowflake_errors']):
                    print("      Full error log available in S3")
    
    # Return the result, which could be used by downstream services if needed
    return {
        'statusCode': 200 if result['status'] == 'completed' else 500,
        'body': json.dumps(result)
    }


def get_ssm_parameter(name, region='ap-south-1'):
    ssm = boto3.client('ssm', region_name=region)
    try:
        response = ssm.get_parameter(
            Name=name,
            WithDecryption=True  # Crucial for SecureString
        )
        return response['Parameter']['Value']
    except Exception as e:
        print(f"Error retrieving parameter {name}: {str(e)}")
        raise
    

def initialize_catalog():
    """Initialize Iceberg catalog using Lambda's IAM role"""
    return load_catalog(
        CONFIG["CATALOG_NAME"],
        **{
            "type": "glue",
            "warehouse": CONFIG["WAREHOUSE_PATH"],
            "uri": CONFIG["GLUE_ENDPOINT"],
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
        }
    )

def get_last_processed_timestamp(table_name):
    """Get last processed timestamp from DynamoDB with proper error handling"""
    try:
        response = snapshot_table.get_item(Key={'table': table_name})
        if 'Item' in response and 'last_timestamp_ms' in response['Item']:
            timestamp_str = response['Item']['last_timestamp_ms']
            return int(timestamp_str) if timestamp_str else None
        return None
    except Exception as e:
        print(f"Error getting timestamp for {table_name}: {e}")
        return None

def update_last_processed_timestamp(table_name, timestamp_ms):
    """Update last processed timestamp in DynamoDB with validation"""
    try:
        if timestamp_ms is None:
            print(f"Warning: No timestamp provided for {table_name}")
            return
            
        snapshot_table.put_item(
            Item={
                'table': table_name,
                'last_timestamp_ms': str(timestamp_ms),  # Ensure string storage
                'last_updated': datetime.datetime.utcnow().isoformat()
            }
        )
    except Exception as e:
        print(f"Error updating timestamp for {table_name}: {e}")
        raise

def process_table_threaded(catalog, table_name, snowflake_conn):
    """Process a single table with error handling for threading"""
    print(f"Thread started for table {table_name}")
    try:
        file_metadata = process_table(catalog, table_name)
        result = {
            'status': 'success',
            'new_files': file_metadata,
            'count': len(file_metadata),
            'snowflake_status': 'skipped',
            'snowflake_loaded': 0,
            'snowflake_errors': []
        }
        
        if snowflake_conn and file_metadata:
            try:
                loaded_count, load_errors = load_to_snowflake(
                    connection=snowflake_conn,
                    table_name=table_name,
                    file_metadata=file_metadata
                )
                result.update({
                    'snowflake_status': 'success',
                    'snowflake_loaded': loaded_count,
                    'snowflake_errors': load_errors
                })
            except Exception as sf_error:
                result.update({
                    'snowflake_status': 'error',
                    'snowflake_error': str(sf_error),
                    'snowflake_errors': load_errors if 'load_errors' in locals() else []
                })
        print(f"Thread complete for table {table_name}")
        return table_name, result
    except Exception as e:
        print(f"Thread complete for table {table_name}")
        return table_name, {
            'status': 'error',
            'error': str(e),
            'snowflake_status': 'skipped'
        }

def process_table(catalog, table_name):
    """Process table with robust timestamp handling"""
    from pyiceberg.io.pyarrow import PyArrowFileIO
    
    try:
        table = catalog.load_table((CONFIG["DATABASE_NAME"], table_name))
        io = PyArrowFileIO()
        
        last_timestamp_ms = get_last_processed_timestamp(table_name)
        print(f"Processing {table_name}. Last timestamp: {last_timestamp_ms or 'Not available'}")
        
        # Get all snapshots and filter new ones
        snapshots = list(table.snapshots())
        new_snapshots = []
        latest_timestamp = last_timestamp_ms or 0
        
        for snapshot in snapshots:
            if last_timestamp_ms is None or snapshot.timestamp_ms > last_timestamp_ms:
                new_snapshots.append(snapshot)
                if snapshot.timestamp_ms > latest_timestamp:
                    latest_timestamp = snapshot.timestamp_ms
        
        if not new_snapshots:
            print(f"No new snapshots for {table_name}")
            return []
        
        file_metadata = []
        for snapshot in new_snapshots:
            for manifest in snapshot.manifests(io):
                for entry in manifest.fetch_manifest_entry(io):
                    if entry.status == 1:  # 1 = ADDED
                        file_metadata.append({
                            'file_path': entry.data_file.file_path,
                            'file_format': 'PARQUET',  # Assuming Iceberg uses Parquet
                            'file_size': entry.data_file.file_size_in_bytes,
                            'snapshot_id': snapshot.snapshot_id,
                            'timestamp_ms': snapshot.timestamp_ms
                        })
        
        # Update last timestamp if we found newer data
        if latest_timestamp > (last_timestamp_ms or 0):
            dynamo_values_to_update[table_name] = latest_timestamp
        
        return file_metadata
        
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
        raise

def process_all_tables():
    """Process all configured tables and load new files to Snowflake"""
    try:
        catalog = initialize_catalog()

        results = {}
        max_workers = min(10, len(CONFIG["TABLES"]))  # Don't exceed 10 threads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all table processing tasks
            future_to_table = {
                executor.submit(
                    process_table_threaded,
                    catalog,
                    table_name,
                    snowflake_conn if CONFIG.get('LOAD_TO_SNOWFLAKE', False) else None
                ): table_name 
                for table_name in CONFIG["TABLES"]
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name, table_result = future.result()
                results[table_name] = table_result

        # Update DynamoDB after all threads complete
        for table_name, timestamp_ms in dynamo_values_to_update.items():
            update_last_processed_timestamp(table_name, timestamp_ms)
        print("Latest timestamps updated in DynamoDB")
        
        return {
            'status': 'completed',
            'summary': {
                'tables_processed': len(results),
                'tables_successful': sum(1 for r in results.values() if r['status'] == 'success'),
                'tables_with_new_files': sum(1 for r in results.values() if r.get('count', 0) > 0),
                'snowflake_load_success': sum(1 for r in results.values() if r.get('snowflake_status') == 'success'),
                'total_files_loaded': sum(r.get('snowflake_loaded', 0) for r in results.values())
            },
            'results': results
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }
    finally:
        if CONFIG.get('LOAD_TO_SNOWFLAKE', False) and snowflake_conn:
            snowflake_conn.close()

def load_to_snowflake(connection, table_name, file_metadata):
    """Load batch of files to Snowflake with retry logic and error capture"""
    max_retries = 3
    batch_size = 100
    error_log = []
    total_loaded = 0
    
    try:
        cursor = connection.cursor()
        
        # Create temp stage
        cursor.execute(f"""
            CREATE STAGE IF NOT EXISTS {table_name}_stage
            URL = 's3://salesforce-marketing-processed/iceberg/salesforce.db/'
            STORAGE_INTEGRATION = SALESFORCE_S3_INTEGRATION_MARKETING
            FILE_FORMAT = (TYPE = 'PARQUET')
        """)
        # Create error logging table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS RAW.{table_name}_load_errors (
                file_path STRING,
                message STRING,
                count INTEGER,
                load_timestamp TIMESTAMP_NTZ,
                batch_id STRING
            )
        """)
        
        # Process in batches
        for i in range(0, len(file_metadata), batch_size):
            batch = file_metadata[i:i + batch_size]
            batch_id = f"{table_name}_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            paths = ",".join(f"'{f['file_path'].replace('s3://salesforce-marketing-processed/iceberg/salesforce.db/','')}'" 
                          for f in batch)
            
            for attempt in range(max_retries):
                try:
                    # Execute COPY with error capture
                    cursor.execute(f"""
                        COPY INTO RAW.{table_name}_temp
                        FROM @{table_name}_stage/
                        FILES = ({paths})
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE 
                        ON_ERROR = CONTINUE
                    """)
                    print(f"Copy command ran for {table_name}")
                    # Capture and log errors
                    errors = cursor.fetchall()
                    if errors:
                        for error in errors:
                            error_log.append({
                                'table': table_name,
                                'file_path': error[0],
                                'error': error[1],
                                'count': error[2],
                                'timestamp': datetime.datetime.utcnow().isoformat(),
                                'batch_id': batch_id
                            })
                            # Insert into error table
                            cursor.execute(
                                f"""
                                INSERT INTO RAW.{table_name}_load_errors 
                                (file_path, message, count, load_timestamp, batch_id)
                                VALUES (%s, %s, %s, CURRENT_TIMESTAMP(), %s)
                                """,
                                (error[0], error[1], error[2], batch_id)
                            )
                    
                    total_loaded += (len(batch) - len(errors))
                    break
                    
                except Exception as e:
                    if attempt == max_retries - 1:
                        # Log entire batch failure
                        for file in batch:
                            error_log.append({
                                'table': table_name,
                                'file_path': file['file_path'],
                                'error': str(e),
                                'count': 1,
                                'timestamp': datetime.datetime.utcnow().isoformat(),
                                'batch_id': batch_id
                            })
                        raise
                    time.sleep(2 ** attempt)

        # Primary key detection and MERGE operation
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM SALESFORCE_MARKETING.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'RAW'
              AND TABLE_NAME = '{table_name.upper()}'
              AND COLUMN_DEFAULT IS NULL
              AND IS_NULLABLE = 'NO'
            ORDER BY ORDINAL_POSITION
            LIMIT 1
        """)
        
        primary_key = cursor.fetchone()
        if not primary_key:
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM SALESFORCE_MARKETING.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'RAW'
                  AND TABLE_NAME = '{table_name.upper()}'
                ORDER BY ORDINAL_POSITION
                LIMIT 1
            """)
            primary_key = cursor.fetchone()
            if not primary_key:
                raise ValueError(f"Cannot determine primary key for RAW.{table_name}")

        pk_column = primary_key[0]
        
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM SALESFORCE_MARKETING.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'RAW'
              AND TABLE_NAME = '{table_name.upper()}'
            ORDER BY ORDINAL_POSITION
        """)
        columns = [row[0] for row in cursor.fetchall()]

        # Exclude both primary key and LAST_MODIFIED_TIMESTAMP from SET clause
        set_columns = [col for col in columns if col != pk_column and col != 'LAST_MODIFIED_TIMESTAMP']
        set_clause = ",\n    ".join([f"target.{col} = source.{col}" for col in set_columns])
        
        # Include all columns (including LAST_MODIFIED_TIMESTAMP) in INSERT
        insert_columns = ", ".join(set_columns)
        insert_values = ", ".join([f"source.{col}" for col in set_columns])
        
        merge_sql = f"""MERGE INTO RAW.{table_name} target USING RAW.{table_name}_temp source ON target.{pk_column} = source.{pk_column} WHEN MATCHED THEN UPDATE SET {set_clause} WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values});"""
        print(f"Merge SQL: {merge_sql}")
        cursor.execute(merge_sql)
        
        cursor.execute(f"TRUNCATE TABLE RAW.{table_name}_temp")
        return total_loaded, error_log

    except Exception as e:
        print(f"Process failed: {e}")

    finally:
        # Write errors to S3 for backup
        if error_log:
            s3 = boto3.client('s3')
            try:
                s3.put_object(
                    Bucket='salesforce-marketing-error-logs',
                    Key=f"load-errors/{table_name}/{datetime.datetime.utcnow().isoformat()}.json",
                    Body=json.dumps(error_log)
                )
            except Exception as e:
                print(f"Failed to write error log to S3: {e}")
        cursor.close()
