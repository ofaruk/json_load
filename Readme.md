## About

This solution uses BigQuery to create tables and insert given json data in the specified schema.  
  

## Local Setup

#### Pre-requisites:
- gcloud sdk  
- gcp project  
- permission to create table in big query  
- python >= v3.11 

Before running the code you will need to populate the `config/config.yaml` file based on your environment.

#### Dowload the source code
Clone the repository to your local and create a virtual environment.
- `git clone`
- `python3 -m venv /path/to/new/virtual/environment` or use your IDE to create one by selecting Python v3.11

#### Python Libraries
This solution was built using Python 3.11. Execute below to install dependencies within your virtual environment.
 - `pip install --upgrade pip`  
 - `pip install -r requirements.txt`  

#### GCP Setup

Run below from your local terminal for gcloud login and ADC  
- `gcloud auth login`  
- `gcloud config set project {your-project-id}`  
- `gcloud auth application-default login` -> follow the instructions and update the config file with the credentials json path prompted in terminal. Ref: https://cloud.google.com/docs/authentication/provide-credentials-adc#local-dev

#### Configure the app
Update below config parameters in `config/config.yaml`:

    credential_path: <path to json file>/application_default_credentials.json  
    bq_project_id: <project-id> 
    dataset_location: EU  
    dataset_name: <dataset-name>
leave the rest as is if you want to use default settings.

## Run

`python main.py`

## SQL Query
Please find the query under `resources/sql/Query.sql`

## Other Considerations
if desired, json files can be uploaded to a gcs bucket and loaded into bq table using `load_table_from_uri` method of big query client. This will lead to a faster load time compared to loading from a remote file system.


