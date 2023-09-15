from google.cloud import bigquery
from google.oauth2 import service_account
from loguru import logger
from app.common import env


class BigqueryDac:
    def __init__(self) -> None:
        self._credentials = service_account.Credentials.from_service_account_file(env.GCS_KEY_FILE)
        self.bigquery_client = bigquery.Client(credentials=self._credentials, project=self._credentials.project_id)


    def select_query(self, query):
        try:
            results = self.bigquery_client.query(query, location="asia-northeast3")
            result_data = results.to_dataframe()
            return result_data

        except Exception as e:
            logger.error(e)
            return None


    def select_query_only(self, query):
        try:
            results = self.bigquery_client.query(query, location="asia-northeast3")

            return results

        except Exception as e:
            logger.error(e)
            return None


    async def async_select_query(self, query):
        try:
            results = self.bigquery_client.query(query, location="asia-northeast3")
            return results.to_dataframe()

        except Exception as e:
            logger.error(e)
            return None

    
    def select_query_param(self, query, query_parameters):
        try:
            job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
            results = self.bigquery_client.query(query,  job_config=job_config)

            return results.to_dataframe()

        except Exception as e:
            logger.error(e)

            return None


    def execute_query(self, query):
        try:
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            
            return results.to_dataframe()

        except Exception as e:
            logger.error(e)

            return None


    def execute_query_v2(self, origin_table, table_id, query):
        try:
            if self.check_table_exists(table_id) is False:
                job = self.bigquery_client.copy_table(origin_table, table_id)  
                job.result()

            query_job = self.bigquery_client.query(query)
            results = query_job.result()

            return results

        except Exception as e:
            logger.error(e)

            return None


    def check_table_exists(self, table_id):
        try:
            self.bigquery_client.get_table(table_id)
            return True

        except Exception as e:
            logger.error(e)

            return False


    def select_query_only(self, query):
        try:
            results = self.bigquery_client.query(query, location="asia-northeast3")

            return results

        except Exception as e:
            logger.error(e)
            return None


    def insert_rows_json(self, table_id, rows_to_insert):
        try:
            results = self.bigquery_client.insert_rows_json(table_id, rows_to_insert)

            return results

        except Exception as e:
            logger.error(e)
            return None
        
    def create_dataset(self,dataset_id):
        try:
            results = self.bigquery_client.create_dataset(dataset_id,timeout=30)
            print("Created dataset {}.{}".format(self.bigquery_client, self.bigquery_client.dataset))
            
            return results
        
        except Exception as e:
            logger.error(e)
            return None

    def delete_dataset(self, table_id):
        try:
            results = self.bigquery_client.delete_dataset(table_id, delete_contents=True, not_found_ok=True)

            return results

        except Exception as e:
            logger.error(e)
            return None
