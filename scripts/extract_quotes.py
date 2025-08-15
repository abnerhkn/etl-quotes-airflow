import requests
from bs4 import BeautifulSoup
import pandas as pd

def coletar_frases():
    base_url = "https://quotes.toscrape.com/page/{}/"
    pagina = 1
    dados = []

    while True:
        url = base_url.format(pagina)
        response = requests.get(url)
        if response.status_code != 200:
            break

        soup = BeautifulSoup(response.text, "html.parser")
        quotes = soup.select(".quote")

        if not quotes:
            break

        for quote in quotes:
            texto = quote.select_one(".text").get_text(strip=True)
            autor = quote.select_one(".author").get_text(strip=True)
            dados.append({"frase": texto, "autor": autor})

        print(f"Página {pagina} → {len(quotes)} frases coletadas")
        pagina += 1

    df = pd.DataFrame(dados)
    caminho_arquivo = "/opt/airflow/dags/quotes.csv"
    df.to_csv(caminho_arquivo, index=False)
    print(f"Total de {len(df)} frases salvas em {caminho_arquivo}")
