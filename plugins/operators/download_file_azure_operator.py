'''
## Importing Libraries ##
'''
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure.storage.blob import BlobServiceClient
import logging as log
import os

'''
## Class UploadFileAzureOperator ##
'''
class DownloadFileAzureOperator(BaseOperator):
       

    @apply_defaults
    def __init__(self, path_azure, file_name, dir_download, *args, **kwargs):
        self.path_azure    = path_azure
        self.file_name     = file_name
        self.dir_download  = dir_download
        super().__init__(*args, **kwargs)

    

    def execute(self, context):

        log.info("### Starting... ###")


        '''
        # Variables - located in /config/var_dags.env
        '''
        container_name           = os.environ['AIRFLOW__CONTAINER__NAME']
        connection_string        = os.environ['AIRFLOW__CONNECTION__STRING']
        working_dir              = os.environ['AIRFLOW__PATH__LOCAL'] + self.dir_download 

        file_name                = self.file_name
        path_azure               = self.path_azure + file_name
        path_file_download       = os.path.join(working_dir, file_name)


        '''
        Variables Local
        '''
        StorageConnection_string = connection_string
        ContainerName            = container_name
        PathAzure                = path_azure
        PathFileDownload         = path_file_download


        '''
        # Create connection Azure
        '''
        log.info("### Starting Connection Azure... ###")
        storage_connection_string = StorageConnection_string
        blob_service_client       = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client          = blob_service_client.get_container_client(ContainerName)

        log.info("### Downloading File... ###")
        with open(PathFileDownload, mode='wb') as download_file:
                download_file.write(container_client.download_blob(PathAzure).readall())

        log.info("### Download Completed Successfully!!! ###")
           

class DownloadFileAzure(AirflowPlugin):
    name = "download_file_azure_operator"
    operators = [DownloadFileAzureOperator]
    sensors = []