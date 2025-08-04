from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from google.cloud import bigquery
import os
import pandas as pd
import pendulum
import requests

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv('../.env')
bigquery_json_path = 'dags/elevated-style-426310-s8-efb512dc5cb5.json'
table_id = os.getenv('TABELA_ID')
token = os.getenv('API_KEY')
chat_id = os.getenv('CHAT_ID')

def enviar_alerta_telegram(moeda, preco):

    mensagem = f"Alerta: {moeda} atingiu R${preco:,.2f}"
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": mensagem}
    requests.post(url, data=data)

with DAG(
    dag_id = 'etl_currency_bigquery',
    start_date = pendulum.now(),
    schedule='* * * * *',
    catchup=False
) as dag:

    @task
    def scrapee_data():

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        try:
            driver.get("https://coinmarketcap.com/")

            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "sc-142c02c-0"))
            )
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "cmc-link"))
            )

            elementos_precos = driver.find_elements(By.CLASS_NAME, "sc-142c02c-0")
            precos = [e.text for e in elementos_precos if "$" in e.text][:3]

            elementos_links = driver.find_elements(By.CLASS_NAME, "cmc-link")
            links = []
            seen_cryptos = set()

            for el in elementos_links:
                href = el.get_attribute("href")
                if href and "/currencies/" in href:
                    href_base = href.split('#')[0]
                    
                    slug = href_base.rstrip('/').split('/')[-1]
                    
                    if slug not in seen_cryptos:
                        seen_cryptos.add(slug)
                        links.append(href_base)
                    
                    if len(links) == 3:
                        break
        finally:
            driver.quit()
            return {"precos": precos, "links": links}

    @task
    def preprocessing_data(precos_links):
        precos = precos_links['precos']
        links = precos_links['links']

        precos = [float(p.replace('$', '').replace(',', '').strip()) for p in precos]
        cripto_coins = [link.rstrip('/').split('/')[-1] for link in links]

        data = {cripto: preco for cripto, preco in zip(cripto_coins, precos)}
        data['Data_Coleta'] = pendulum.now()

        df_cripto = pd.DataFrame([data])
        
        return df_cripto
    
    @task
    def verificar_alertas(df):
        btc = df['bitcoin'].iloc[0]
        if btc >= 115290:
            enviar_alerta_telegram("Bitcoin", btc)
    
    @task
    def connect_and_load_data(df):
        CAMINHO_CHAVE = fr"{bigquery_json_path}"
        client = bigquery.Client.from_service_account_json(CAMINHO_CHAVE)

        try:
            table = client.get_table(table_id)
            print("Tabela já existe.")

        except Exception as e:
            print("Tabela ainda não existe")

        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print("Dados enviados com sucesso")

    precos_links = scrapee_data()
    df = preprocessing_data(precos_links)
    verificar_alertas(df)
    connect_and_load_data(df)