import os
import boto3
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

# Initialize boto3 clients
ssm = boto3.client('ssm')
print("Test")

# Constants (can also be env variables)
PARAM_NAME = "ICEBERG_LAST_UPDATED_SNAPSHOT_ID"
S3_BASE_PATH = "s3://salesforce-marketing-processed/iceberg/salesforce.db/"
CATALOG_NAME = "my_catalog"  # arbitrary
CATALOG_TYPE = "hadoop"  # for S3, PyIceberg uses HadoopCatalog
AWS_REGION = "ap-south-1"


def get_last_snapshot_id():
    try:
        response = ssm.get_parameter(Name=PARAM_NAME)
        return int(response['Parameter']['Value'])
    except ssm.exceptions.ParameterNotFound:
        return None


def set_last_snapshot_id(snapshot_id):
    ssm.put_parameter(
        Name=PARAM_NAME,
        Value=str(snapshot_id),
        Type='String',
        Overwrite=True
    )


def lambda_handler(event, context):
    # Load Iceberg catalog (HadoopCatalog points to S3 base path)
    catalog: Catalog = load_catalog(
        name=CATALOG_NAME,
        config={
            "type": CATALOG_TYPE,
            "warehouse": S3_BASE_PATH,
        }
    )

    # Example: specify table name here
    table_name = "salesforce.db.campaign"
    table: Table = catalog.load_table(table_name)

    last_snapshot_id = get_last_snapshot_id()

    if last_snapshot_id:
        print(f"Last loaded snapshot ID: {last_snapshot_id}")
    else:
        print("No last snapshot found, loading all data.")

    # Get all snapshots
    snapshots = list(table.snapshots())
    print(f"Total snapshots found: {len(snapshots)}")

    # Find new snapshots (greater than last loaded)
    new_snapshots = [s for s in snapshots if (not last_snapshot_id) or s.snapshot_id > last_snapshot_id]
    if not new_snapshots:
        print("No new snapshots found.")
        return {
            "new_files": [],
            "message": "No new snapshots to process."
        }

    # Collect new data files from all new snapshots
    new_files = []
    for snap in new_snapshots:
        manifests = snap.manifests
        for manifest in manifests:
            manifest_entries = manifest.fetch_entries()
            for entry in manifest_entries:
                # Only added files, ignore deletes
                if entry.status == "ADDED":
                    new_files.append(entry.data_file.file_path)

    # Update last snapshot ID with the max of new snapshots
    max_snapshot_id = max(s.snapshot_id for s in new_snapshots)
    set_last_snapshot_id(max_snapshot_id)
    print(f"Updated last snapshot ID to: {max_snapshot_id}")

    return {
        "new_files": new_files,
        "last_snapshot_id": max_snapshot_id,
        "num_files": len(new_files)
    }