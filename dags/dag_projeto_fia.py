'''
## Importing Libraries ##
'''
from airflow import DAG
# from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.python_operator import PythonOperator
from operators.download_file_azure_operator import DownloadFileAzureOperator
from operators.processing_file_azure_operator import ProcessingFileAzureOperator

from datetime import datetime, timedelta
import os
# from processing_file_azure import processing_file_azure


'''
## DAG parameters ##
'''
with DAG(
    dag_id='dag_projeto_fia', 
    tags=['fia', 'blob storage'],
    start_date=datetime(2023, 6, 9), 
    schedule_interval='@daily',
	catchup=False
    ) as dag:

    # Start
    ts = DummyOperator(task_id='Start')

    # 1° task - download file Azure
    # t1 = DownloadFileAzureOperator(
    #     task_id='download_file_azure_base_covid',
    #     path_azure   = 'bronze/base_casos/',
    #     file_name    = 'part-00000-tid-2954364198428598051-04f74bca-c5d2-4137-bd93-dba122696be4-116-1-c000.csv',
    #     dir_download = 'download/raw/'
    # )

    # 2° task - download file Azure
    # t2 = DownloadFileAzureOperator(
    #     task_id='download_file_azure_vacinacao',
    #     path_azure   = 'bronze/vac_2milhoes/',
    #     file_name    = 'part-00000-tid-1106569254780964992-9ba5ea2a-4600-49cb-b1e1-baedc14780ec-127-1-c000.csv',
    #     dir_download = 'download/raw/'
    # )

    # 3° task - download file Azure
    # t3 = DownloadFileAzureOperator(
    #     task_id='download_file_azure_raw',
    #     path_azure   = 'raw/',
    #     file_name    = 'part-00000-tid-1691038912231830220-32479aa9-388e-4064-b5a9-d7d289d34544-45-1-c000.csv',
    #     dir_download = 'download/raw/'
    # )

    # 4° task - processing file Azure
    t4 = ProcessingFileAzureOperator(
        task_id='processing_file_azure', 
        path_azure='bronze/base_casos/', 
        file_name='part-00000-tid-2954364198428598051-04f74bca-c5d2-4137-bd93-dba122696be4-116-1-c000.csv', 
        path_local='/usr/local/airflow/blob/raw/'
    )

    # End
    te = DummyOperator(task_id='End')


    #ts >> t1 >> 
    # t2 
    #>> t3 >> te
    t4 >> te