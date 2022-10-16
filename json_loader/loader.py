from google.cloud import bigquery


class Loader:
    project_id = ""
    dataset_name = ""
    tables = []

    def __init__(self, project_id, dataset_name, tables):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.tables = tables
        self.client = bigquery.Client(project=self.project_id)

    def write_to_bq(self, schema, json_path, table_name, location):
        schema = self.client.schema_from_json(schema)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        table_id = ".".join([self.project_id, self.dataset_name, table_name])
        with open(json_path, "rb") as binary_data:
            load_job = self.client.load_table_from_file(
                binary_data,
                table_id,
                location=location,
                job_config=job_config,
            )
        try:
            result = load_job.result()
        except Exception as exc:
            print(exc)
        else:
            print(result)

