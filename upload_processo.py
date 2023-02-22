# Importando os pacotes necessários
import pandas as pd
from google.cloud import bigquery
import os

PROJECT_ID = 'alert-streamer-286501'
DATASET_REF = "REF_VENDA_BOTICARIO"
DATASET_DMT = "DMT_VENDA_BOTICARIO"

TABLE_ID_UPLOAD = "{0}.{1}.TB_VENDAS".format(PROJECT_ID,DATASET_REF)

# Transformando os dados de Excel para csv
pd.read_excel("Dados/Base 2017.xlsx").to_csv("Dados/Base 2017.csv", index=False)
pd.read_excel("Dados/Base_2018.xlsx").to_csv("Dados/Base 2018.csv", index=False)
pd.read_excel("Dados/Base_2019.xlsx").to_csv("Dados/Base 2019.csv", index=False)

# Atualizando a variável de ambiente com as credenciais da google
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "config/alert-streamer-286501-b0d71197e4e1.json"

# Criando um cliente do BigQuery
client = bigquery.Client()

# Configuracao do job para subir os arquivos na camada REFINADA de dados
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, 
                                    skip_leading_rows=1,
                                    autodetect=True,
                                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

#  Filtrando somente os arquivos .csv
files = [f for f in os.listdir("Dados") if f.endswith(".csv")]

# Fazendo o upload dos arquivos csv para uma tabela no BigQuery
def p_upload(files):
    for file in files:
        file_path="Dados/{}".format(file)
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, TABLE_ID_UPLOAD, job_config=job_config)
        print("Upload do arquivo {} finalizado.".format(file))
        os.remove(file_path)

# Executando queries e fazendo o upload de tabelas transformadas para o BigQuery
def p_upload_transformacoes_query():
    print('\n')
    print("Executando tabelas transformadas...")
    for file in os.listdir(os.getcwd() + "\queries_transformacoes"):
        file_name = file[:-4]
        sql = open(os.getcwd() + "\queries_transformacoes\{}".format(file), 'r').read()
        query_job = client.query(sql,
                                 job_config=bigquery.QueryJobConfig(destination="{0}.{1}.{2}".format(PROJECT_ID,DATASET_DMT, file_name),
                                                                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE))
        print("Tabela {} atualizada.".format(file_name))

# Execucao do upload dos arquivos da origem e tabelas transformadas
p_upload(files)
p_upload_transformacoes_query()