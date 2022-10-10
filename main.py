import os
from google.cloud import bigquery

# Load custom built modules
from json_loader.loader import Loader
from tools.YamlConfigReader import return_content
from tools.paths import Folders
from db_setup.db_creator import DbStarter

path = Folders.config_folder / "config.yaml"
config = return_content(path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['credential_path']


if __name__ == '__main__':
    # Extract config
    project_id = config['bq_project_id']
    client = bigquery.Client(project=project_id)
    loc = config['dataset_location']
    dataset_name = config['dataset_name']
    tables = config['tables']

    # Initialise database setup object
    db_setup = DbStarter(
        project_id=project_id,
        dataset_name=dataset_name,
        tables=tables,
        location=loc,
        bq_client=client
    )

    # Create big query dataset and return bq dataset
    bq_dataset = db_setup.create_bq_dataset()

    # Create big query tables
    [
        db_setup.create_bq_day_partitioned_table(
            bq_dataset=bq_dataset,
            table_name=table['name'],
            schema=Folders.resource_folder / table['schema'],
            partition_field=table['partition_field']
        )
        for table in tables
    ]

    # initialise json loader object
    loader = Loader(
        project_id=config['bq_project_id'],
        dataset_name=config['dataset_name'],
        tables=config['tables']
    )

    # execute loader
    [
        loader.write_to_bq(
            schema=Folders.resource_folder / table['schema'],
            json_path=Folders.data_folder / table['data_path'],
            table_name=table['name'],
            location=config['dataset_location']
        )
        for table in loader.tables
    ]
