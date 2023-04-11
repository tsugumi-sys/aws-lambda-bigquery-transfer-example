from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def generate_bigquery_table_id(bigquery_dataset_id: str, snapshot_target_name: str):
    """
    Convert snapshot target name to bigquery table id.
        e.g. {rds-db-name}.public.{table_name}
        -> {GC-project-id}.{bigquery-dataset-id}.{table_name}
    """
    table_name = snapshot_target_name.split(".")[-1]
    return f"{bigquery_dataset_id}.{table_name}"


def create_bigquery_tables(
    bigquery_client, bigquery_dataset_id: str, snapshot_target_names: list
):
    """
    Create BigQuery table wituout schema, because BigQuery read schema infomation
    from parquet file automatically), if the table does not exist.
    If the table exists, do nothing.

    Returns:
        list: The name of bigquery tables.
    """
    bq_table_names = []
    for t_name in snapshot_target_names:
        table_id = generate_bigquery_table_id(bigquery_dataset_id, t_name)
        try:
            bigquery_client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, {})
            bigquery_client.create_table(table)
        # table_id is formatted like "project_id.dataset_id.table_name".
        bq_table_names.append(table_id.split(".")[-1])
    return bq_table_names
