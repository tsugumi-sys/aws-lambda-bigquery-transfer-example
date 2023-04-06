import datetime
import json
import logging
import os

import boto3
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer as bq_transfer
from google.cloud.exceptions import NotFound
from google.oauth2.service_account import Credentials

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_gc_credentials(asm_secrets: dict) -> Credentials:
    """

    Args:
        asm_secrets (dict): Response of `boto3.SecretsManager.Client.get_secret_value()`.
            The contents are as follows.
            {
                'ARN': 'string',
                'Name': 'string',
                'VersionId': 'string',
                'SecretBinary': b'bytes',
                'SecretString': 'string',
                'VersionStages': [
                    'string',
                ],
                'CreatedDate': datetime(2015, 1, 1)
            }
    """
    credential_dic = json.loads(asm_secrets["SecretString"])
    return Credentials.from_service_account_info(credential_dic)


def download_secrets_from_ASM(secret_name: str, region: str) -> dict:
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region,
    )

    return client.get_secret_value(SecretId=secret_name)


class BiqQueryTransferer:
    def __init__(
        self,
        gc_project_name: str,
        bigquery_dataset_id: str,
        aws_secret_name: str,
        aws_secret_region: str,
    ):
        """
        Args:
            export_tables_info (dict): The meta info of exported DB tables, which is
                generated in the top directory of S3 after completing snapshot export.
                The information about all exported tables are stored as follows.
                {
                    "perTableStatus": [
                        {
                            "TableStatistics": {...},
                            "schemaMetadata": {...},
                            "status": ...,
                            "sizeGB": ...,
                            "target": xxx, # this is table name.
                        }
                    ],
                    ...
                }
        """
        asm_secrets = download_secrets_from_ASM(aws_secret_name, aws_secret_region)
        if asm_secrets is None:
            raise ValueError(
                f"Download AMS secrets failed (`secret_name`={aws_secret_name}, "
                f"`secret_region`={aws_secret_region})."
            )

        gc_credentials = build_gc_credentials(asm_secrets)
        self.bigquery_dataset_id = bigquery_dataset_id
        self.bigquery_transfer_client = bq_transfer.DataTransferServiceClient(
            credentials=gc_credentials
        )
        self.bigquery_client = bigquery.Client(
            project=gc_project_name, credentials=gc_credentials
        )
        self.bigquery_dataset = self.bigquery_client.dataset(bigquery_dataset_id)

    def transfer_rds_snapshot(self, data_source_s3_path: str, export_tables_info: dict):
        """
        Args:
            export_tables_info (dict): The meta info of exported DB tables, which is
                generated in the top directory of S3 after completing snapshot export.
                The information about all exported tables are stored as follows.
                {
                    "perTableStatus": [
                        {
                            "TableStatistics": {...},
                            "schemaMetadata": {...},
                            "status": ...,
                            "sizeGB": ...,
                            "target": xxx, # this is table name.
                        }
                    ],
                    ...
                }
        """

        def _create_transfer_config(table_names: str) -> list:
            """Create BigQuery Transfer Configuration for all given tables in RDS
            Snapshot. This function only used in `transfer_rds_snapshot`.
            """
            configs = []
            parent = self.bigquery_client.common_project_path(self.bigquery_dataset_id)
            for table_name in table_names:
                transfer_config = bq_transfer.TransferConfig(
                    destination_dataset_id=self.bigquery_dataset_id,
                    display_name=f"test-{table_name}-transfer-to-{self.bigquery_dataset_id}",
                    data_source_id="amazon_s3",
                    schedule_options={"disable_auto_scheduling": True},
                    params={
                        "destination_table_name_template": table_name,
                        "data_path": os.path.join(
                            data_source_s3_path, f"${table_name}/*/*.parquet"
                        ),
                        "file_format": "parquet",
                    },
                )
                transfer_config = self.bigquery_transfer_client.create_transfer_config(
                    parent=parent,
                    transfer_config=transfer_config,
                )
                return configs

        table_names = self._exported_table_names(export_tables_info)
        self._create_bigquery_tables(table_names[:1])

        for config in self._create_transfer_config(table_names):
            transfer_req = bq_transfer.StartManualTransferRunsRequest(
                parent=config.name,
                requested_run_time=datetime.datetime.now(),
            )
            res = self.bigquery_transfer_client.start_manual_transfer_runs(
                request=transfer_req
            )
            logger.info(res)

    def _create_bigquery_tables(self, table_names: str):
        """
        Create BigQuery table wituout schema, because BigQuery read schema infomation
        from parquet file automatically), if the table does not exist.
        If the table exists, do nothing.
        """
        for table_name in table_names:
            try:
                self.bigquery_client.get_table(table_name)
                print('table exists.')
            except NotFound:
                table_ref = self.bigquery_dataset.table(table_name)
                self.bigquery_client.create_table(bigquery.Table(table_ref))
                print('table', table_name, 'created')

    def _exported_table_names(self, export_tables_info: dict):
        return [item["target"] for item in export_tables_info["perTableStatus"]]

    def get_tables(self):
        tables = self.bigquery_client.list_tables(self.bigquery_dataset_id)
        print(f"Table contained in {self.bigquery_dataset_id}")
        for t in tables:
            print("{}.{}.{}".format(t.project, t.dataset_id, t.table_id))
        


def get_env(env_key: str, default_val=None, raise_err: bool = True):
    value = os.environ.get(env_key, default_val)
    if value is None and raise_err:
        raise ValueError(f"Environment Value [{env_key}] not found in {os.environ}")
    return value


def find_export_tables_info_files(
    client, source_s3_bucket_name: str, export_task_name: str
) -> list:
    res = client.list_objects(
        Bucket=source_s3_bucket_name, Prefix=f"{export_task_name}/export_tables_info_"
    )
    logger.info(res)

    files = []
    for object in res["Contents"]:
        filename = object["Key"]
        if filename.endswith("json"):
            files.append(
                {
                    "Bucket": source_s3_bucket_name,
                    "Key": filename,
                }
            )
    return files


def download_export_tables_info(
    client, source_s3_bucket_name: str, export_task_name: str
) -> dict:
    export_tables_info_files = find_export_tables_info_files(
        client, source_s3_bucket_name, export_task_name
    )
    PER_TABLES_STATUS_KEY = "perTableStatus"
    export_tables_info = None
    for f in export_tables_info_files:
        res = client.get_object(Bucket=f["Bucket"], Key=f["Key"])
        body = json.loads(res["Body"].read().decode("utf-8"))
        if export_tables_info is None:
            export_tables_info = body
            continue
        export_tables_info[PER_TABLES_STATUS_KEY] += body[PER_TABLES_STATUS_KEY]
    return export_tables_info


def lambda_handler(event, context):
    """
    Test Cases:
        1. Check if you can get exported tables info.
        2. Check if you can download secrets.
        3. Check if you can authenticate and access BigQuery.
        4. Check if you can create table and send it of a single table.
        5. Check if you can create and send all tables to BQ.
    """
    gc_project_name = get_env("GC_PROJECT_NAME")
    bigquery_dataset_id = get_env("BIGQUERY_DATASET_ID")

    # Credential JSON file for authentication of google-cloud client
    # are saved in AWS Secret Manager.
    aws_secret_name = get_env("AWS_SECRET_NAME")
    aws_secret_region = get_env("AWS_SECRET_REGION")

    # Get S3 bucket for storeing snapshots.
    source_s3_bucket_name = get_env("SOUCE_S3_BUCKET_NAME")
    export_task_name = get_env("EXPORT_TASK_NAME")
    s3_client = boto3.client("s3")
    export_tables_info = download_export_tables_info(
        s3_client, source_s3_bucket_name, export_task_name
    )
    
    bq_transferer = BiqQueryTransferer(
        gc_project_name,
        bigquery_dataset_id,
        aws_secret_name,
        aws_secret_region,
    )
    bq_transferer.transfer_rds_snapshot(source_s3_bucket_name, export_tables_info)
