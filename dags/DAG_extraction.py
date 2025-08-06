from airflow import DAG
from airflow.decorators import task
from dags.ETL_pipeline import Process_data
from pandas import DataFrame
import pendulum

processor = Process_data()

with DAG(
    dag_id = 'etl_currency_bigquery',
    start_date = pendulum.now(),
    schedule='* * * * *',
    catchup=False
) as dag:

    @task
    def extrair_dados() -> dict:
        return processor.scrape_data()   

    @task
    def preprocessamento(dados) -> DataFrame:
        return processor.preprocessing_data(dados)
    
    @task
    def check_alertas(df):
        processor.verificar_alertas(df)
    
    @task
    def conectar_enviar_dados(df):
        processor.connect_and_load_data(df)

    precos_links = extrair_dados()
    df = preprocessamento(precos_links)
    check_alertas(df)
    conectar_enviar_dados(df)