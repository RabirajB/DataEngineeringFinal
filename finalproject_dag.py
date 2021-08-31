import pandas as pd
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from google.cloud import storage
from google.cloud import bigquery

#Now I shall define the functions for the DAG
DAG_NAME = 'capstone_dag'

default_args = {
    "depends_on_past": False,
    "email":[],
    "email_on_failure": False,
    "email_on_retry" : False,
    "owner": "airflow",
    "retries" : 3,
    "retry_delay": timedelta(minutes = 2),
    "start_date" : datetime(2021, 8, 31)
}

dag = DAG(
    dag_id = "finalproject_dag",
    default_args = default_args,
    schedule_interval = '*/5 * * * *',
    max_active_runs = 1

)


def combine_dataframes():
    storage_client = storage.Client()
    bucket_name = "final-csv-bucket"
    blobs = storage_client.list_blobs(bucket_name)
    combined_df = pd.DataFrame()
    for blob in blobs:
        file_name = blob.name
        print(file_name)
        blob.download_to_filename(f'/home/airflow/gcs/data/{file_name}')
        print('Reading Dataframes and combining them')
        df = pd.read_csv(f'/home/airflow/gcs/data/{file_name}')
        combined_df = combined_df.append(df)
        #blob.delete()
    combined_df.to_csv('/home/airflow/gcs/data/final_df.csv', index = False)
'''
def clean_dataframe():
    df = pd.read_csv('/home/airflow/gcs/data/final_df.csv')
    for idx, row in df.iterrows():
        row['text'] = str(row['text'])
    df.to_csv('/home/airflow/gcs/data/final_df.csv', index = False)
'''

def upload_to_bigquery():
    client = bigquery.Client()
    table_id = "finalproject-egen.final_dataset.table1"
    job_config = bigquery.LoadJobConfig(
        schema = [
                bigquery.SchemaField("State_FIPS_Code", "NUMERIC"),
                bigquery.SchemaField("Sampling_Stratum", "NUMERIC"),
                bigquery.SchemaField("Unique_Household_ID", "NUMERIC"),
                bigquery.SchemaField("Form_Type", "NUMERIC"),
                bigquery.SchemaField("Number_of_Children_in_Household", "NUMERIC"),
                bigquery.SchemaField("The_Conditions_under_Which_Land_or_Buildings_Are_Held_or_Occupied", "NUMERIC"),
                bigquery.SchemaField("Primary_Household_Language", "NUMERIC"),
                bigquery.SchemaField("Age_of_Selected_Child_In_Years", "NUMERIC"),
                bigquery.SchemaField("Sex_of_Selected_Child", "NUMERIC"),
                bigquery.SchemaField("Autism_ASD", "NUMERIC"),
                bigquery.SchemaField("Birth_Month", "NUMERIC"),
                bigquery.SchemaField("Birth_Year", "NUMERIC")
        ],
        skip_leading_rows = 1,
        source_format = bigquery.SourceFormat.CSV,
        allow_quoted_newlines = True
    )
    uri = "gs://us-central1-finalprojectcom-8287972a-bucket/data/final_df.csv"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config = job_config
    )
    load_job.result()
    destination_table = client.get_table(table_id)
    print('Loaded {} rows'.format(destination_table.num_rows))


start = DummyOperator(task_id = "start", dag = dag)
end = DummyOperator(task_id = "end", dag = dag)
combine_dataframes_task = PythonOperator(
    task_id = "combine_dataframes_task",
    python_callable = combine_dataframes,
    dag = dag
)
upload_to_bigquery_task = PythonOperator(
    task_id = "upload_to_bigquery_task",
    python_callable = upload_to_bigquery,
    dag = dag
)
'''
clean_dataframe_task = PythonOperator(
    task_id = "clean_dataframe_task",
    python_callable = clean_dataframe,
    dag = dag
)
'''
start >> combine_dataframes_task >> upload_to_bigquery_task >> end




