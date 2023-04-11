import datetime
import json
import os

from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1 as bq_transfer
from google.oauth2.service_account import Credentials
from google.protobuf import json_format as google_json_format

from bigquery_utils import create_bigquery_tables
from google_auth_utils import build_gc_credentials
from aws_lambda_utils import retrieve_snapshot_target_names
from aws_secret_manager_utils import download_secrets_from_ASM


class BiqQueryTransferer:
    def __init__(
        self,
        gc_project_id: str,
        bigquery_dataset_id: str,
        aws_secret_name_for_gc_service_account: str,
        aws_secret_name_for_iam_user: str,
        aws_secret_region: str,
        local_cred_file=None,
    ):
        """
        Args:
            gc_project_id (str): Google Cloud project id.
            bigquery_dataset_id (str): Google Cloud BigQuery dataset id to transfer.
            aws_secret_name_for_gc_service_account (str): Name of the secret of the
                credentials of the Google Cloud service account in AMS Secret
                Manager.
            aws_secret_name_for_iam_user (str): Name of the secret (access_key_id &
                secret_access_key) of the IAM user to enable BigQuery Transfer API
                client accessing S3.
            aws_secret_region (str): Region name of AWS Secret Manager.
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
            cred_file_path (str, optional): File path of GCP credentials of a service
                account (only used for debug).
        """
        if local_cred_file is None:
            # Download secrets of IAM user.
            asm_secrets = download_secrets_from_ASM(
                aws_secret_name_for_iam_user, aws_secret_region
            )
            user_credentials = json.loads(asm_secrets["SecretString"])
            self.ACCESS_KEY_ID = user_credentials["access_key_id"]
            self.SECRET_ACCESS_KEY = user_credentials["secret_access_key"]
            # Download secrets of GC service account.
            asm_secrets = download_secrets_from_ASM(
                aws_secret_name_for_gc_service_account, aws_secret_region
            )
            gc_credentials = build_gc_credentials(asm_secrets)
        else:
            with open(local_cred_file) as f:
                gc_credentials = json.load(f)
            gc_credentials = Credentials.from_service_account_info(gc_credentials)
        self.gc_project_id = gc_project_id
        self.bigquery_dataset_id = bigquery_dataset_id
        self.bigquery_transfer_client = bq_transfer.DataTransferServiceClient(
            credentials=gc_credentials
        )
        self.bigquery_client = bigquery.Client(
            project=gc_project_id, credentials=gc_credentials
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
        snapshot_target_names = retrieve_snapshot_target_names(export_tables_info)
        bq_table_names = create_bigquery_tables(
            self.bigquery_client, self.bigquery_dataset_id, snapshot_target_names[:1]
        )

        self._remove_remaining_transfer_configs(bq_table_names)
        for config in self._create_transfer_config_for_s3(
            bq_table_names, snapshot_target_names[:1], data_source_s3_path
        ):
            transfer_req = bq_transfer.StartManualTransferRunsRequest(
                parent=config.name,
                requested_run_time=datetime.datetime.now(),
            )
            self.bigquery_transfer_client.start_manual_transfer_runs(
                request=transfer_req
            )

    def _remove_remaining_transfer_configs(self, transfer_config_names: list):
        parent = self.bigquery_transfer_client.common_project_path(self.gc_project_id)
        request = bq_transfer.ListTransferConfigsRequest(parent=parent)
        res = self.bigquery_transfer_client.list_transfer_configs(request=request)
        for name in transfer_config_names:
            remaining_configs = [cfg for cfg in res if res.name == name]
            for cfg in remaining_configs:
                req = bq_transfer.DeleteTransferConfigRequest(name=name)
                self.bigquery_transfer_client.delete_transfer_config(request=req)

    def _create_transfer_config_for_s3(
        self,
        bq_table_names: list,
        snapshot_target_names: list,
        data_source_s3_path: str,
    ) -> list:
        """
        Create BigQuery Transfer configuration to transfer PARQUET files in S3 (RDS
        snapshot, etc.).
        The configuration name is bigquery table name (item in `bq_table_names`).

        NOTE: You need to "manually" start the configured transfer run in this function.

        Args:
            bq_table_names: List of BigQuery table names formatted like as follows:
                {GC-project-id}.{bigquery-dataset-id}.{table_name}
            snapshot_target_names: List of RDS snapshot target name like as follows:
                {rds-db-name}.public.{table_name}
        Returns:
            list: List of BigQuery Transfer configurations.
        """
        configs = []
        parent = self.bigquery_transfer_client.common_project_path(self.gc_project_id)
        for bq_tb_name, snapshot_tb_name in zip(bq_table_names, snapshot_target_names):
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
                    "access_key_id": self.ACCESS_KEY_ID,
                    "secret_access_key": self.SECRET_ACCESS_KEY,
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
