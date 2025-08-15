import streamlit as st
import pandas as pd
import psycopg2

DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "webscraping-quotes"
DB_USER = "abner.lima"
DB_PASS = "dracarys"

st.title("Dashboard de Frases Coletadas")

@st.cache_data
def carregar_dados():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    df = pd.read_sql("SELECT * FROM quotes", conn)
    conn.close()
    return df

df = carregar_dados()

st.metric("Total de frases coletadas", len(df))

autores = st.multiselect("Filtrar por autor", df['autor'].unique())
if autores:
    df = df[df['autor'].isin(autores)]

st.dataframe(df)

st.bar_chart(df['autor'].value_counts())
