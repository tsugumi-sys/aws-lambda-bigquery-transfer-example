import datetime
import json
import logging
import os

import boto3
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1 as bq_transfer
from google.cloud.exceptions import NotFound
from google.oauth2.service_account import Credentials
from google.protobuf import json_format as google_json_format

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_gc_credentials(asm_secrets: dict) -> Credentials:
    """

    Args:
        asm_secrets (dict): Response of `boto3.SecretsManager.Client.get_secret_value()`
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
        gc_project_id: str,
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
        # with open("./credentials.json") as f:
        #     gc_credentials = json.load(f)
        # gc_credentials = Credentials.from_service_account_info(gc_credentials)
        self.gc_project_id = gc_project_id
        self.bigquery_dataset_id = bigquery_dataset_id
        self.bigquery_transfer_client = bq_transfer.DataTransferServiceClient(
            credentials=gc_credentials
        )
        self.bigquery_client = bigquery.Client(
            project=gc_project_id, credentials=gc_credentials
        )
        self.bigquery_dataset = self.bigquery_client.dataset(bigquery_dataset_id)

    def _remove_remaining_transfer_configs(self, transfer_config_names: list):
        parent = self.bigquery_transfer_client.common_project_path(self.gc_project_id)
        request = bq_transfer.ListTransferConfigsRequest(parent=parent)
        res = self.bigquery_transfer_client.list_transfer_configs(request=request)
        for name in transfer_config_names:
            remaining_configs = [cfg for cfg in res if res.name == name]
            for cfg in remaining_configs:
                req = bq_transfer.DeleteTransferConfigRequest(name=name)
                self.bigquery_transfer_client.delete_transfer_config(request=req)

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

        def _create_transfer_config(
            bq_table_names: list, snapshot_table_names: list
        ) -> list:
            """Create BigQuery Transfer Configuration for all given tables in RDS
            Snapshot. This function only used in `transfer_rds_snapshot`.
            """
            configs = []
            parent = self.bigquery_transfer_client.common_project_path(
                self.gc_project_id
            )
            for bq_tb_name, snapshot_tb_name in zip(
                bq_table_names, snapshot_table_names
            ):
                transfer_config = {
                    "name": bq_tb_name,
                    # NOTE: self..bigquery_dataset_id is formatted like as follows:
                    # {project_id}.{dataset_id}
                    "destination_dataset_id": self.bigquery_dataset_id.split(".")[-1],
                    "display_name": (
                        f"test-{snapshot_tb_name}-transfer-to-{self.bigquery_dataset_id}"
                    ),
                    "data_source_id": "amazon_s3",
                    "schedule_options": {"disable_auto_scheduling": True},
                    "params": {
                        "destination_table_name_template": bq_tb_name,
                        "data_path": os.path.join(
                            data_source_s3_path,
                            f"{snapshot_tb_name}/*/*.parquet",
                        ),
                        "file_format": "PARQUET",
                    },
                }
                # See: https://shorturl.at/qxzCD
                # This is shortened url of google api docs of python bigquery transfer
                # api. This document is hard to find so comment here so that you can
                # save your time :)
                transfer_config = google_json_format.ParseDict(
                    transfer_config, bq_transfer.types.TransferConfig()._pb
                )
                request = bq_transfer.CreateTransferConfigRequest(
                    parent=parent,
                    transfer_config=transfer_config,
                )
                transfer_config = self.bigquery_transfer_client.create_transfer_config(
                    request
                )
                configs.append(transfer_config)
            return configs

        snapshot_table_names = self._snapshot_table_names(export_tables_info)
        bq_table_names = self._create_bigquery_tables(snapshot_table_names[:1])

        self._remove_remaining_transfer_configs(bq_table_names)
        for config in _create_transfer_config(bq_table_names, snapshot_table_names[:1]):
            transfer_req = bq_transfer.StartManualTransferRunsRequest(
                parent=config.name,
                requested_run_time=datetime.datetime.now(),
            )
            res = self.bigquery_transfer_client.start_manual_transfer_runs(
                request=transfer_req
            )
            logger.info(res)

    def _create_bigquery_tables(self, table_names: str) -> list:
        """
        Create BigQuery table wituout schema, because BigQuery read schema infomation
        from parquet file automatically), if the table does not exist.
        If the table exists, do nothing.

        Returns:
            list: The name of bigquery tables.
        """
        bq_table_names = []
        for table_name in table_names:
            table_id = self._table_id(table_name)
            try:
                self.bigquery_client.get_table(table_id)
            except NotFound:
                table = bigquery.Table(table_id, {})
                self.bigquery_client.create_table(table)
            # table_id is formatted like "project_id.dataset_id.table_name".
            bq_table_names.append(table_id.split(".")[-1])
        return bq_table_names

    def _table_id(self, table_name: str):
        """
        Convert snapshot table name to bigquery table id.
        e.g. {rds-db-name}.public.{table_name}
        -> {GC-project-id}.{bigquery-dataset-id}.{table_name}
        """
        table_name = table_name.split(".")[-1]
        return f"{self.bigquery_dataset_id}.{table_name}"

    def _snapshot_table_names(self, export_tables_info: dict):
        """
        Return table names based on RDS snapshot naming conventions
        like `{rds-db-name}.public.{table_name}`.

        Args:
            export_tables_info (dict): The meta info of exported DB tables, which is
            generated in the top directory of S3 after completing snapshot export.

        Returns:
            list: The exported table names in s3, which are formatted like
            `{rds-db-name}/public.{table_name}`.
        """
        target_tables = [
            item["target"] for item in export_tables_info["perTableStatus"]
        ]
        table_names = []
        for target in target_tables:
            target_name_splits = target.split(".")
            table_names.append(
                target_name_splits[0] + "/" + ".".join(target_name_splits[1:])
            )
        return table_names


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
    gc_project_id = get_env("GC_PROJECT_ID")
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
        gc_project_id,
        bigquery_dataset_id,
        aws_secret_name,
        aws_secret_region,
    )
    bq_transferer.transfer_rds_snapshot(
        os.path.join(source_s3_bucket_name, export_task_name), export_tables_info
    )


# if __name__ == "__main__":
#     bq_transferer = BiqQueryTransferer(
#         "ocp-stg",
#         "ocp-stg.rds_snapshot_export_test",
#         "dum",
#         "dum",
#     )

#     bq_transferer.transfer_rds_snapshot(
#         "s3://casdca",
#         {
#             "perTableStatus": [
#                 {
#                     "target": "ocp-stg.public.test_table",  # this is table name.
#                 }
#             ],
#         },
#     )
