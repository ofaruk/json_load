from google.cloud import bigquery
from google.api_core.exceptions import Conflict


class DbStarter:

    def __init__(self, project_id, dataset_name, tables, location, bq_client):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.tables = tables
        self.location = location
        self.client = bq_client

    def create_bq_dataset(self):
        dataset_id = f"{self.project_id}.{self.dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.location
        try:
            dataset = self.client.create_dataset(dataset, timeout=30)
            print("Created dataset {}.{}".format(self.client.project, self.dataset_name))
        except Conflict:
            print(f"Dataset '{self.dataset_name}' already exist in '{self.client.project}' project")
        finally:
            return dataset

    def create_bq_day_partitioned_table(self, bq_dataset, table_name, schema, partition_field):
        table_ref = bq_dataset.table(table_name)
        schema = self.client.schema_from_json(schema)
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
        try:
            table = self.client.create_table(table)
            print(
                "Created table {}, partitioned on column {}".format(
                    table.table_id, table.time_partitioning.field
                )
            )
        except Conflict:
            print(f"Table '{table_name}' already exist in '{bq_dataset.dataset_id}' dataset")
