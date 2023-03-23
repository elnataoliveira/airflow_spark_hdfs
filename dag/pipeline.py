import os
from os import chdir, listdir
from os.path import isfile
from pyunpack import Archive
import wget
import py7zr
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pyspark import SparkContext
from pyspark.sql import SparkSession


input_folder = '/home/airflow/data/input'
output_folder = '/home/airflow/data/output'

#download
def download_files(input_folder: str):

    chdir(input_folder)
    url_list = ['ftp://ftp.mtps.gov.br:21/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_CENTRO_OESTE.7z',
        'ftp://ftp.mtps.gov.br:21/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_MG_ES_RJ.7z',
        'ftp://ftp.mtps.gov.br:21/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_NORDESTE.7z',
        'ftp://ftp.mtps.gov.br:21/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_NORTE.7z',
        'ftp://ftp.mtps.gov.br:21/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_SP.7z',
        'ftp://ftp.mtps.gov.br:21/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_SUL.7z']
    for url in url_list:
        wget.download(url, url[51:])
        print('download - ' + url[51:])

#unpack
def unpack_files(input_folder: str, output_folder: str):

    chdir(input_folder)
    for f in listdir():
        if isfile(f):
            print('file name: ' + f)
            archive = py7zr.SevenZipFile(f, mode='r')
            archive.extractall(path=output_folder)
            #archive.close()

#etl
def etl_to_hdfs(files: str):

    spark = SparkSession.builder.appName('etl rais').getOrCreate()
    rais = spark.read \
            .format('csv').option('header', True) \
            .option('inferSchema', True) \
            .option('encoding', 'iso-8859-1') \
            .option('delimiter',';').load(files)
    rais.write.mode('overwrite').format('parquet').save("hdfs://hdpmaster:9000/user/jovyan/archives/silver/rais")

#delete
def delete_files(input_folder: str, output_folder: str):

    dirs = [input_folder, output_folder]
    for d in dirs:
        chdir(d)
        for file in listdir():
            if isfile(file):
                os.system('rm -f ' + file)
                print('delete file - ' + file)


# DAG etl
with DAG(dag_id = 'pipeline', start_date = pendulum.datetime(2022, 1, 1, tz = "UTC"), schedule = '@Daily', catchup = False) as dag:

    #load files .7z on input
    task1_download = PythonOperator(
        task_id = 'download_files',
        python_callable = download_files,
        op_kwargs = {'input_folder' : input_folder},
        dag = dag
    )

    #extract files to output
    task2_unpack = PythonOperator(
        task_id = 'unpack_files',
        python_callable = unpack_files,
        op_kwargs = {'input_folder': input_folder, 'output_folder': output_folder},
        dag = dag
    )

    #load_on_datalake
    task3_etl = PythonOperator(
        task_id = 'etl_datalake',
        python_callable = etl_to_hdfs,
        op_kwargs = {'files' : output_folder},
        dag = dag
    )

    #delete all files
    task4_delete = PythonOperator(
        task_id = 'delete_files',
        python_callable = delete_files,
        op_kwargs = {'input_folder': output_folder, 'output_folder': input_folder},
        dag = dag
    )

#task queue
task1_download >> task2_unpack >> task3_etl >> task4_delete
