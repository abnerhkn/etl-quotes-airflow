import pandas as pd
import os 

def limpar_frases():
    caminho_arquivo = "/opt/airflow/dags/quotes.csv"
    caminho_saida = "/opt/airflow/dags/quotes_clean.csv"

    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo {caminho_arquivo} não encontrado.")

    df = pd.read_csv(caminho_arquivo)

    df["frase"] = df["frase"].str.replace('“', '', regex=False).str.replace('”', '', regex=False)

  
    df["autor"] = df["autor"].str.strip().str.title()

    df.drop_duplicates(inplace=True)

    df["data_transformacao"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    df.to_csv(caminho_saida, index=False)

    print(f"{len(df)} registros salvos em {caminho_saida}")