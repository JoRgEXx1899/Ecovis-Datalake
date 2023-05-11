from airflow import DAG
import pandas as pd
import requests
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime, timedelta
from io import StringIO

default_args = {
    'owner': 'Jorge Gomez',
    'start_date': datetime(2023, 5, 9),
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs':1
}

# Configuración de las conexiones
github_connection = 'my_github_connection'
gcs_connection = 'google_cloud_default'
bq_conn_id = 'bigquery_default'

# Configuración del dataset en BigQuery
bIGQUERY_DATASET_NAME = 'covid_usa'
bIGQUERY_TABLE_NAME = 'daily_record'
gCP_PROJECT_ID="esoteric-crow-386105"

# Configuración de los nombres de los buckets y objetos
github_repo_owner = 'nytimes'
github_repo_name = 'covid-19-data'
github_file_path = 'master/us.csv'
gcs_bucket_name = 'bucket_ecovis'
gcs_file_name = 'covid_data.csv'

#Función python para extraer de Github un csv y cargarlo a Google Cloud
def read_csv_from_github_and_write_to_gcs(github_connection, github_repo_owner, github_repo_name, github_file_path, gcs_connection, gcs_bucket_name, gcs_file_name,**context):
    # Leer los datos CSV de Github y convertirlos en un objeto DataFrame de Pandas
    url = f'https://raw.githubusercontent.com/{github_repo_owner}/{github_repo_name}/{github_file_path}'
    print (url)
    response = requests.get(url)
    csv_data = StringIO(response.content.decode('utf-8'))
    df = pd.read_csv(csv_data)

    # Escribir el archivo CSV en Google Cloud Storage
    gcs_client = GCSHook(gcp_conn_id=gcs_connection).get_conn()
    bucket = gcs_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_file_name)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    context['ti'].xcom_push(key='csv_data', value=df.to_dict())

#Función python que se encarga de limpiar los datos mediante pandas y guardarlos en Google Cloud
def limpieza_datos(github_repo_owner, github_repo_name, github_file_path, gcs_connection, gcs_bucket_name, gcs_file_name,**context):
    # Lee los datos desde la variable csv_data
    # Leer los datos CSV de Github y convertirlos en un objeto DataFrame de Pandas
    url = f'https://raw.githubusercontent.com/{github_repo_owner}/{github_repo_name}/{github_file_path}'
    print (url)
    response = requests.get(url)
    csv_data = StringIO(response.content.decode('utf-8'))
    df = pd.read_csv(csv_data)

    # Elimina las filas que tienen valores faltantes o inválidos
    df = df.dropna()
    # Convierte la columna de fecha al formato YYYY-MM-DD
    df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')
    # Calcula la diferencia entre un campo de filas consecutivas
    df['casos nuevos'] = df['cases'].diff()
    # Calcula la diferencia entre un campo de filas consecutivas
    df['muertes nuevas'] = df['deaths'].diff()
    # Elimina las filas que tienen valores faltantes o inválidos
    df = df.dropna()

    # Guarda los datos limpios en un nuevo archivo CSV
    df.to_csv('datos_limpios.csv', index=False)
    context['ti'].xcom_push(key='csv_data', value=df.to_dict())
    # Escribir el archivo CSV en Google Cloud Storage
    gcs_client = GCSHook(gcp_conn_id=gcs_connection).get_conn()
    bucket = gcs_client.bucket(gcs_bucket_name)
    blob = bucket.blob('covid_data_procesado.csv')
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    context['ti'].xcom_push(key='csv_data', value=df.to_csv('datos_limpios.csv', index=False))

dag = DAG(
    'extract_data_to_lake',
    default_args=default_args,
    description='Extraer datos de un repositorio de Github, limpiarlos y cargarlos en el lago de datos de BigQuery',
    schedule_interval=timedelta(days=1),
)

#Inicio de la pipeline
start_pipeline = DummyOperator(
    task_id = 'inicio_de_pipeline',
    dag = dag
)

#Creación de bucket en Google Cloud Services
create_gcs_bucket_task = GCSCreateBucketOperator(
    task_id='create_gcs_bucket_task',
    bucket_name=gcs_bucket_name,
    project_id='esoteric-crow-386105',
    gcp_conn_id=gcs_connection,
    dag=dag
)

# Descarga de los datos de Github y escritura en GCS
github_to_gcs_task = PythonOperator(
    task_id='github_to_gcs_task',
    python_callable=read_csv_from_github_and_write_to_gcs,
    op_kwargs={
        'github_connection': github_connection,
        'github_repo_owner': github_repo_owner,
        'github_repo_name': github_repo_name,
        'github_file_path': github_file_path,
        'gcs_connection': gcs_connection,
        'gcs_bucket_name': gcs_bucket_name,
        'gcs_file_name': gcs_file_name
    },
    dag=dag
)

#Limpieza de datos por medio de función python "limpieza_datos"
clean_data = PythonOperator(
    task_id='clean_data_and_add_daily_cases',
    python_callable=limpieza_datos,
    op_kwargs={
        'github_repo_owner': github_repo_owner,
        'github_repo_name': github_repo_name,
        'github_file_path': github_file_path,
        'gcs_connection': gcs_connection,
        'gcs_bucket_name': gcs_bucket_name,
        'gcs_file_name': gcs_file_name
    },
    dag=dag
)

#Tarea de creación de tabla en BigQuery con estructira definida
create_table_task = BigQueryCreateEmptyTableOperator(
    task_id='create_table',
    project_id=gCP_PROJECT_ID,
    dataset_id=bIGQUERY_DATASET_NAME,
    table_id=bIGQUERY_TABLE_NAME,
    gcp_conn_id=gcs_connection,
    schema_fields=[
        {'name': 'fecha', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'casos', 'type': 'float', 'mode': 'NULLABLE'},
        {'name': 'muertes', 'type': 'float', 'mode': 'NULLABLE'},
        {'name': 'nuevos_casos', 'type': 'float', 'mode': 'NULLABLE'},
        {'name': 'nuevas_muertes', 'type': 'float', 'mode': 'NULLABLE'}
    ],
    dag=dag
)

#Tarea para cargar datos desde csv en GCS a BigQuery
load_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="load-data-to-bigquery",
    bucket="bucket_ecovis",
    source_objects=["covid_data_procesado.csv"],
    destination_project_dataset_table="esoteric-crow-386105.covid_usa.daily_record",
    source_format="CSV",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    allow_quoted_newlines = 'true',
    autodetect=True,
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'fecha', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'casos', 'type': 'float', 'mode': 'NULLABLE'},
        {'name': 'muertes', 'type': 'float', 'mode': 'NULLABLE'},
        {'name': 'nuevos_casos', 'type': 'float', 'mode': 'NULLABLE'},
        {'name': 'nuevas_muertes', 'type': 'float', 'mode': 'NULLABLE'}
    ],
    dag=dag
)

#Consulta de primer renglón de consulta de la prueba
consulta_cym_mes_total = BigQueryOperator(
    task_id = 'consulta_cym_mes_total',
    use_legacy_sql = False,
    sql = './sql/casos_y_muertes_mensuales_total.sql',
    destination_dataset_table="esoteric-crow-386105.covid_usa.cym_mensuales_total",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

#Consulta de segundo renglón de consulta de la prueba
consulta_cym_mes_promedio = BigQueryOperator(
    task_id = 'consulta_cym_mes_promedio',
    use_legacy_sql = False,
    sql = './sql/casos_y_muertes_mensuales_promedio.sql',
    destination_dataset_table="esoteric-crow-386105.covid_usa.cym_mensuales_promedio",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

#Consulta de tercer renglón de consulta de la prueba por casos
top_5_casos_nuevos = BigQueryOperator(
    task_id = 'top_5_casos_nuevos',
    use_legacy_sql = False,
    sql = './sql/top_5_nuevos_casos.sql',
    destination_dataset_table="esoteric-crow-386105.covid_usa.top_5_casos_nuevos",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

#Consulta de tercer renglón de consulta de la prueba por muertes
top_5_muertes_nuevas = BigQueryOperator(
    task_id = 'top_5_muertes_nuevas',
    use_legacy_sql = False,
    sql = './sql/top_5_nuevas_muertes.sql',
    destination_dataset_table="esoteric-crow-386105.covid_usa.top_5_muertes_nuevas",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

#Fin de la pipeline
finish_pipeline = DummyOperator(
    task_id = 'fin_de_pipeline',
    dag = dag
) 

#Definición de el flujo de tareas
start_pipeline>>create_gcs_bucket_task >> [github_to_gcs_task,clean_data]
clean_data >> create_table_task >> load_to_bq >> [consulta_cym_mes_total,consulta_cym_mes_promedio,top_5_casos_nuevos,top_5_muertes_nuevas]>>finish_pipeline