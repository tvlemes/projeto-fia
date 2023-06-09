'''
## Importing Libraries ##
'''
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure.storage.blob import BlobServiceClient
from pyspark.sql.functions import to_date
import logging as log
import os
from pyspark.sql import SparkSession

'''
## Class UploadFileAzureOperator ##
'''
class ProcessingFileAzureOperator(BaseOperator):
       

    @apply_defaults
    def __init__(self, path_azure, file_name, path_local, *args, **kwargs):
        self.path_azure    = path_azure
        self.file_name     = file_name
        self.path_local    = path_local
        super().__init__(*args, **kwargs)

    

    def execute(self, context):

        log.info("### Starting... ###")

        spark = (SparkSession
         .builder
         .getOrCreate())
        

        '''
        # Variables - located in /config/var_dags.env
        '''
        container_name           = os.environ['AIRFLOW__CONTAINER__NAME']
        connection_string        = os.environ['AIRFLOW__CONNECTION__STRING']
        path_azure               = self.path_azure + self.file_name
        path_local               = self.path_local
        

        '''
        Variables Local
        '''
        StorageConnectionString = connection_string
        ContainerName            = container_name
        PathAzure                = path_azure 
        PathLocal                = path_local

    
        log.info("### Starting Connection Azure... ###")

        blob_service_client = BlobServiceClient.from_connection_string(StorageConnectionString)
        container_client = blob_service_client.get_container_client(ContainerName)
        

        log.info("### Processing File... ###")

        ### Insert code processing here! ###
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(container_client.download_blob(PathAzure).content_as_text())
            # .load(PathAzure)
        
        df = df.withColumn("data", to_date(df.date))
        df_rs = df.filter((F.col("place_type") == "city") & (F.col("state") == "RS"))
        df_uf = df.filter(F.col("place_type") == "state")

        df.write.format("csv").mode("overwrite").save(PathLocal + 'dados_covid_processing')
        df_rs.write.format("csv").mode("overwrite").save(PathLocal + 'dados_covid_rs_processing')
        df_uf.write.format("csv").mode("overwrite").save(PathLocal + 'dados_covid_uf_processing')


        log.info("### File processed!!! ###")
           

class ProcessingFileAzure(AirflowPlugin):
    name = "processing_file_azure_operator"
    operators = [ProcessingFileAzureOperator]
    sensors = []