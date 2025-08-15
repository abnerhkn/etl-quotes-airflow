import pandas as pd
from sqlalchemy import create_engine

def carregar_frases():
    
    caminho_arquivo = "/opt/airflow/dags/quotes_clean.csv"

    
    df = pd.read_csv(caminho_arquivo)

    
    usuario = "abner.lima"       
    senha = "dracarys"           
    host = "postgres"            
    porta = "5432"
    banco = "webscraping-quotes" 

    
    engine = create_engine(f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}")

    
    df.to_sql("quotes", engine, if_exists="append", index=False)

    print(f"{len(df)} registros inseridos na tabela 'quotes'")
