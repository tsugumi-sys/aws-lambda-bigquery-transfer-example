import logging
import os

import boto3

from bigquery_transferer import BiqQueryTransferer
from aws_lambda_utils import download_export_tables_info, get_env

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Test Cases:
        1. Check if you can get exported tables info.
        2. Check if you can download secrets.
        3. Check if you can authenticate and access BigQuery.
        4. Check if you can create table and send it of a single table.
        5. Check if you can create and send all tables to BQ.
    """
    gc_project_id = get_env("GC_PROJECT_ID")
    bigquery_dataset_id = get_env("BIGQUERY_DATASET_ID")

    # Credential JSON file for authentication of google-cloud client
    # are saved in AWS Secret Manager.
    aws_secret_name_for_gc_service_account = get_env(
        "AWS_SECRET_NAME_FOR_GC_SERVICE_ACCOUNT"
    )
    aws_secret_name_for_iam_user = get_env("AWS_SECRET_NAME_FOR_IAM_USER")
    aws_secret_region = get_env("AWS_SECRET_REGION")

    # Get S3 bucket for storeing snapshots.
    source_s3_bucket_name = get_env("SOUCE_S3_BUCKET_NAME")
    export_task_name = get_env("EXPORT_TASK_NAME")
    s3_client = boto3.client("s3")
    export_tables_info = download_export_tables_info(
        s3_client, source_s3_bucket_name, export_task_name
    )

    bq_transferer = BiqQueryTransferer(
        gc_project_id,
        bigquery_dataset_id,
        aws_secret_name_for_gc_service_account,
        aws_secret_name_for_iam_user,
        aws_secret_region,
    )
    bq_transferer.transfer_rds_snapshot(
        os.path.join(f"s3://{source_s3_bucket_name}", export_task_name),
        export_tables_info,
    )


if __name__ == "__main__":
    bq_transferer = BiqQueryTransferer(
        "ocp-stg",
        "ocp-stg.rds_snapshot_export_test",
        "dum",
        "dum",
        "dum",
        local_cred_file="./credentials.json",
    )

    bq_transferer.transfer_rds_snapshot(
        "s3://casdca",
        {
            "perTableStatus": [
                {
                    "target": "ocp-stg.public.test_table",  # this is table name.
                }
            ],
        },
    )
