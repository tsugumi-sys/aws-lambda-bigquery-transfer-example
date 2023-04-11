import json
import os
import re


def retrieve_snapshot_target_names(export_tables_info: dict):
    """
    Return table names based on RDS snapshot naming conventions
    like `{rds-db-name}.public.{table_name}`.

    Args:
        export_tables_info (dict): The meta info of exported DB tables, which is
        generated in the top directory of S3 after completing snapshot export.
        Fortmatted as follows:

            The information about all exported tables are stored as follows.
            {
                "perTableStatus": [
                    {
                        "TableStatistics": {...},
                        "schemaMetadata": {...},
                        "status": ...,
                        "sizeGB": ...,
                        "target": {rds-db-name}/public.{table_name},
                    }
                ],
                ...
            }

    Returns:
        list: The exported target names in s3, which are formatted like
        `{rds-db-name}/public.{table_name}`.
    """
    target_tables = [item["target"] for item in export_tables_info["perTableStatus"]]
    target_tables = [t for t in target_tables if re.match(r".+\.public\..+", t)]
    target_names = []
    for target in target_tables:
        target_name_splits = target.split(".")
        target_names.append(
            target_name_splits[0] + "/" + ".".join(target_name_splits[1:])
        )
    return target_names


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


def find_export_tables_info_files(
    client, source_s3_bucket_name: str, export_task_name: str
) -> list:
    res = client.list_objects(
        Bucket=source_s3_bucket_name, Prefix=f"{export_task_name}/export_tables_info_"
    )

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


def get_env(env_key: str, default_val=None, raise_err: bool = True):
    value = os.environ.get(env_key, default_val)
    if value is None and raise_err:
        raise ValueError(f"Environment Value [{env_key}] not found in {os.environ}")
    return value
