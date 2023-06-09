'''
## Importing Libraries ##
'''
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
import logging as log
import os

'''
## Class UploadFileAzureOperator ##
'''
class UploadFileAzureOperator(BaseOperator):

    @apply_defaults
    def __init__(self, blob_root_directory,  working_dir, path_file, path_azure, *args, **kwargs):
        # Root directory
        self.blob_root_directory = blob_root_directory

        # Blob root directory in airflow
        # working_dir = os.getcwd()
        self.working_dir = working_dir

        # File folder
        self.path_file = path_file

        # Path Azure
        self.path_azure = path_azure
        super().__init__(*args, **kwargs)

    def execute(self, context):
            # Variables - located in /config/var_dags.env
            container_id        = os.environ['AIRFLOW__CONTAINER__NAME']
            connection_string   = os.environ['AIRFLOW__CONNECTION__STRING']

            # Create connection
            storage_connection_string = connection_string
            blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

            Blob_root_directory = self.blob_root_directory
            Working_dir = self.working_dir
            Path_file = self.path_file
            Path_azure=self.path_azure

            log.info("### Start ###")

            file_directory = os.walk(Working_dir + Path_file)

            for folder in file_directory:
                for file in folder[-1]:
                    try:
                        file_path = os.path.join(folder[0], file)      
                        blob_path = '{0}{1}{2}'.format(
                            Blob_root_directory,
                            Path_azure,
                            file_path.replace(Working_dir+Path_file, '')
                        )
                        blob_obj = blob_service_client.get_blob_client(container=container_id, blob=blob_path)
                        with open(file_path, mode='rb') as file_data:
                            blob_obj.upload_blob(file_data)
                        print(f'## Upload with success {file_directory} to {Blob_root_directory} ##')
                    except ResourceExistsError as e:
                        print('Blob (file object) {0} already exists.'.format(file))
                        continue
                    except Exception as e:            
                        print(e)

class UploadFileAzure(AirflowPlugin):
    name = "upload_file_azure_operator"
    operators = [UploadFileAzureOperator]
    sensors = []