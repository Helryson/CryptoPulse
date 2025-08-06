# Importação de bibliotecas necessárias
from dotenv import load_dotenv    # Carrega variáveis de ambiente do arquivo .env
from google.cloud import bigquery  # Cliente do BigQuery
import json
import os
import pandas as pd
import pendulum
import requests

# Selenium para web scraping
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class Process_data:
    """
    Classe principal que realiza scraping de dados de criptomoedas,
    faz pré-processamento, envia alertas por Telegram e envia dados ao BigQuery.
    """

    def __init__(self):
        """
        Inicializa a classe carregando variáveis de ambiente e definindo atributos de acesso a APIs.
        """
        load_dotenv('../.env')
        self.bigquery_json_path = 'dags/elevated-style-426310-s8-efb512dc5cb5.json'
        self.table_id = os.getenv('TABELA_ID')
        self.token = os.getenv('API_KEY')
        self.chat_id = os.getenv('CHAT_ID')

    def enviar_alerta_telegram(self, moeda, preco):
        """
        Envia uma mensagem de alerta para o Telegram com o preço da criptomoeda.

        Args:
            moeda (str): Nome da criptomoeda.
            preco (float): Preço atual da criptomoeda.
        """
        mensagem = f"Alerta: {moeda} atingiu R${preco:,.2f}"
        
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        data = {"chat_id": self.chat_id, "text": mensagem}
        requests.post(url, data=data)

    def scrape_data(self):
        """
        Realiza scraping dos dados da CoinMarketCap:
        - Preços das 3 principais criptomoedas
        - Links das páginas dessas moedas

        Returns:
            dict: {'precos': [lista de preços], 'links': [lista de links]}
        """
        options = Options()
        options.add_argument("--headless")  # Executa o Chrome em modo invisível
        options.add_argument("--disable-gpu")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        try:
            driver.get("https://coinmarketcap.com/")

            # Aguarda 20 segundos para o carregamento dos elementos HTML
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "sc-142c02c-0"))
            )
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "cmc-link"))
            )

            # Extrai preços
            elementos_precos = driver.find_elements(By.CLASS_NAME, "sc-142c02c-0")
            precos = [e.text for e in elementos_precos if "$" in e.text][:3]

            # Extrai links das 3 primeiras criptos
            elementos_links = driver.find_elements(By.CLASS_NAME, "cmc-link")
            links = []
            seen_cryptos = set()

            for el in elementos_links:
                href = el.get_attribute("href")  # Obtém o valor do atributo 'href' do elemento atual

                # Verifica se o link é válido e se é um link relacionado a uma criptomoeda
                if href and "/currencies/" in href:
                    # Remove qualquer âncora no link (parte após '#')
                    href_base = href.split('#')[0]
                    
                    # Extrai o 'slug' da criptomoeda (nome no final da URL)
                    slug = href_base.rstrip('/').split('/')[-1]

                    # Garante que a mesma criptomoeda não seja adicionada mais de uma vez
                    if slug not in seen_cryptos:
                        seen_cryptos.add(slug)        # Adiciona o slug ao conjunto para rastrear duplicatas
                        links.append(href_base)       # Adiciona o link limpo à lista de links

                    # Interrompe o loop quando já foram coletados os links de 3 criptomoedas
                    if len(links) == 3:
                        break

        finally:
            driver.quit()
            return {"precos": precos, "links": links}
        
    def preprocessing_data(self, precos_links):
        """
        Faz pré-processamento dos dados coletados:
        - Converte preços para float
        - Extrai slugs dos links
        - Cria DataFrame com timestamp

        Args:
            precos_links (dict): dicionário com chaves 'precos' e 'links'.

        Returns:
            pd.DataFrame: DataFrame formatado para inserção no BigQuery.
        """
        precos = precos_links['precos']
        links = precos_links['links']

        # Limpeza dos preços
        precos = [float(p.replace('$', '').replace(',', '').strip()) for p in precos]
        # Extração do nome da moeda a partir do link
        cripto_coins = [link.rstrip('/').split('/')[-1] for link in links]

        # Combina as informações num dicionário
        data = {cripto: preco for cripto, preco in zip(cripto_coins, precos)}
        data['Data_Coleta'] = pendulum.now()

        # Converte para DataFrame
        df_cripto = pd.DataFrame([data])
        return df_cripto
    
    def verificar_alertas(self, df):
        """
        Verifica se o preço do Bitcoin ultrapassou um valor fixo.
        Envia alerta via Telegram se isso ocorrer.

        Args:
            df (pd.DataFrame): DataFrame com os dados das criptos.
        """
        btc = df['bitcoin'].iloc[0]
        if btc >= 115290:  # Limite fixo (pode ser parametrizado)
            self.enviar_alerta_telegram("Bitcoin", btc)

    def connect_and_load_data(self, df):
        """
        Conecta ao BigQuery e envia os dados do DataFrame para a tabela configurada.

        Args:
            df (pd.DataFrame): DataFrame com os dados já processados.
        """
        CAMINHO_CHAVE = fr"{self.bigquery_json_path}"
        client = bigquery.Client.from_service_account_json(CAMINHO_CHAVE)

        try:
            table = client.get_table(self.table_id)
            print("Tabela já existe.")
        except Exception:
            print("Tabela ainda não existe")

        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')
        job = client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
        job.result()

        print("Dados enviados com sucesso")
