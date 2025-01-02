from google.cloud import bigquery
import pandas as pd

class BigQueryHandler:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def get_bigquery_data(self, query: str) -> pd.DataFrame:
        try:
            query_job = self.client.query(query)
            result = query_job.result()
            return result.to_dataframe()
        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()

    def insert_data(self, dataset_id: str, table_id: str, dataframe: pd.DataFrame):
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            job = self.client.load_table_from_dataframe(dataframe, table_ref)
            job.result()
            print(f"Inserted {job.output_rows} rows into {dataset_id}:{table_id}.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def delete_table(self, dataset_id: str, table_id: str):
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            self.client.delete_table(table_ref)
            print(f"Deleted table {dataset_id}.{table_id}.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def create_table(self, dataset_id: str, table_id: str, schema: list):
        try:
            dataset_ref = self.client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            print(f"Created table {dataset_id}.{table_id}.")
        except Exception as e:
            print(f"An error occurred: {e}")
            
    def create_or_replace_table(self, dataset_id: str, table_id: str, dataframe: pd.DataFrame):
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            job = self.client.load_table_from_dataframe(dataframe, table_ref, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
            job.result()
            print(f"Table {dataset_id}.{table_id} created or replaced with {job.output_rows} rows.")
        except Exception as e:
            print(f"An error occurred: {e}")
            
            
        

