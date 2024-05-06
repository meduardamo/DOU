import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import gspread
from google.oauth2.service_account import Credentials

# Função para Raspagem dos Dados
def raspa_dou(data=None):
    if data is None:
        data = datetime.now().strftime('%d-%m-%Y')
    print(f'Raspando as notícias do dia {data}...')
    try:
        url = f'http://www.in.gov.br/leiturajornal?data={data}'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find("script", {"id": "params"})
        if script_tag:
            print('Notícias raspadas')
            return json.loads(script_tag.text)
        else:
            print("Elemento script não encontrado.")
            return None
    except requests.RequestException as e:
        print(f"Erro ao fazer a requisição: {e}")
        return None

# Função para Salvar os Resultados na Base de Dados
def salva_na_base(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para salvar.')
        return

    print('Salvando palavras na base de dados...')
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        credentials = Credentials.from_service_account_file('credentials.json', scopes=scopes)
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(os.getenv('PLANILHA')).worksheet('Página1')
        rows_to_append = [[item['date'], palavra, item['title'], item['href'], item['abstract']]
                          for palavra, lista_resultados in palavras_raspadas.items() for item in lista_resultados]

        if rows_to_append:
            sheet.append_rows(rows_to_append)
            print(f'{len(rows_to_append)} linhas foram adicionadas à planilha.')
        else:
            print('Nenhum dado válido para salvar.')
    except Exception as e:
        print(f'Erro ao salvar dados: {e}')

# Função para Enviar Email com os Resultados
def envia_email(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para enviar.')
        return

    print('Enviando e-mail...')
    smtp_server = "smtp-mail.outlook.com"
    port = 587
    email = os.getenv('EMAIL')
    password = os.getenv('SENHA_EMAIL')
    destinatarios = os.getenv('DESTINATARIOS').split(',')
    data = datetime.now().strftime('%d-%m-%Y')
    titulo = f'Busca DOU do dia {data}'

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Busca DOU</title>
</head>
<body>
    <h1>Consulta ao Diário Oficial da União</h1>
    <p>As matérias encontradas no dia {data} foram:</p>
    {format_html_results(palavras_raspadas)}
</body>
</html>"""

    try:
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls()
            server.login(email, password)
            msg = MIMEMultipart('alternative')
            msg["From"] = email
            msg["To"] = ",".join(destinatarios)
            msg["Subject"] = titulo
            msg.attach(MIMEText(html, "html"))
            server.sendmail(email, destinatarios, msg.as_string())
            print('E-mail enviado com sucesso.')
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")

def format_html_results(palavras_raspadas):
    html_content = ""
    for palavra, lista_resultados in palavras_raspadas.items():
        html_content += f"<h2>{palavra}</h2>\n<ul>\n"
        for resultado in lista_resultados:
            html_content += f"<li><a href='{resultado['href']}'>{resultado['title']}</a></li>\n"
        html_content += "</ul>\n"
    return html_content

# Chamar funções
conteudo_raspado = raspa_dou()
palavras_raspadas = procura_termos(conteudo_raspado)
salva_na_base(palavras_raspadas)
envia_email(palavras_raspadas)
