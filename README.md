# Ecovis-Datalake
Proyecto de PipeLine ETL para prueba de ECOVIS.

# ¿Cómo ejecutar la pipeline?
Para ejecutar la pipeline diseñada para esta prueba es necesario
1. Crear un entorno en Google Cloud Composer el cual corra apache airflow configurando la locación en "us-central1" y esperar que termine la creación.
2. Cargar el archivo Temp.py al bucket creado por el entorno
3. Cargar la carpeta "sql" al al bucket creado por el entorno con todo su contenido
4. Dentro del entorno dar clic en la UI de OPEN AIRFLOW en la parte superior ![image](https://github.com/JoRgEXx1899/Ecovis-Datalake/assets/42323429/3d0830b3-4fb0-4253-af02-cd011184af69) el cual abrirá el entorno de airflow creado para ese entorno en Google Cloud
5. Dentro de AirFlow en la pestaña de DAGs dar clic en el dag de "extract_to_data_lake" y activar el dag y el auto-refresh
6. Monitorear que se están ejecutando todas las tareas programads correctamente de la pipeline

# Explicación de la solución
Para realizar esta solución se optó por usar apache Airflow debido a que es OpenSource y es fácil para integrar con otras tecnologías. Como proveedor de almacenamiento Data-Lake se eligió a Google Cloud por tener muchas soluciones integradas dentro de su entorno entre ellas Cloud Storage y BigQuery, además del impacto en la actualidad que está teniendo el cómputo en la nube de Google.

Apache Airflow funciona por medio de los DAG (Directed Acyclic Graph) que establecen un flujo de tareas para la ejecución y contrucción de una pipeLine. En su mayoría el DAG trabaja con Python dependiendo de los operadores que se elijan para hacer ciertas funciones.

## Librerías utilizadas
Las librerías que se importaron para la solución con sus usos son:
* DAG: Para la creación del DAG
* pandas: Para la limpieza de los datos
* requests: para obtener respuestas de solicitudes HTTP
* BigQueryCreateEmptyDatasetOperator: para crear un dataset nuevo y vacío de BigQuery
* BigQueryCreateEmptyTableOperator: para crear tablas en BigQuery
* GCSCreateBucketOperator: para crear buckets dentro de Google Cloud Storage
* DummyOperator: Un operador vacío para demarcar el inicio y fin de la pipeline
* PythonOperator: Un operador el cual permite llamar funciones creadas de python
* GCSHook: Se usa para obtener la conexión de Google Cloud Storage
* GoogleCloudStorageToBigQueryOperator: Para transferir y convertir archivos desde Google Cloud Storage a BigQuery
* BigQueryOperator: Se usa para ejecutar consultas de BigQuery
* datetime, timedelta: se usan para establecer los parámetros iniciales del DAG
* StringIO: Se usa para poder transformar los datos de un HTTP a CSV

## Flujo del pipeline
1. Lo primero que se hizo en el flujo de la pipeline fue dar inicio por medio de un dummyoperator vacío
2. Crear un bucket de Google Cloud Storage
3. Extraer el archivo us.csv de el repositorio de github https://github.com/nytimes/covid-19-data/blob/master/us.csv y almacenarlo en el bucket de Google Cloud previamente creado.
4. Limpiar los datos obtenidos eliminando espacios vacíos y datos inválidos, convirtiendo la fecha al formato "yyyy-mm-dd", calculando los casos y muertes diarios.
5. Cargar los datos procesados y limpios como un .csv a Google Cloud Storage.
6. Crear un dataset en BigQuery llamado "covid_usa".
7. Crear una tabla en BigQuery con la estructura o esquema de sus campos llamada "daily_record".
8. Cargar los datos .csv que se habían limpiado y procesado a la tabla creada en BigQuery.
9. Ejecutar las consultas propuestas y almacenar los resultados dentro de el mismo dataset de BigQuery
10. Finalizar la pipeline

Las consultas para el final de la pipeline se encuentran almacenadas como archivos .sql y están escritas para ser interpretadas por Google BigQuery. Estas consultas se ejecutan en simultáneo antes de dar fin al pipeline.

**Solución Creada por**

***Jorge Daniel Gomez Vanegas - Ingeniero de Sistemas de la Universidad Distrital Francisco José de Caldas***
