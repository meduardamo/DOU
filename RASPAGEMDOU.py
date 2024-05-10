import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Função para Raspagem dos Dados
def raspa_dou(data=None):
    if data is None:
        data = datetime.now().strftime('%d-%m-%Y')
    print(f'Raspando as notícias do dia {data}...')
    try:
        url = f'http://www.in.gov.br/leiturajornal?data={data}'
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        if soup.find("script", {"id": "params"}):
            print('Notícias raspadas')
            return json.loads(soup.find("script", {"id": "params"}).text)
        else:
            print("Elemento script não encontrado.")
            return None
    except requests.RequestException as e:
        print(f"Erro ao fazer a requisição: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
        return None

# Função para Formatação da Data
def formata_data():
    print('Encontrando a data...')
    data_atual = date.today()
    data_formatada = data_atual.strftime('%d-%m-%Y')
    print('Data encontrada:', data_formatada)
    return data_formatada

# Função para Procurar Termos Específicos
def procura_termos(conteudo_raspado):
    if conteudo_raspado is None or 'jsonArray' not in conteudo_raspado:
        print('Nenhum conteúdo para analisar ou formato de dados inesperado.')
        return None

    print('Buscando palavras-chave...')
    palavras_chave = [
    'Infância', 
    'Saúde', 
    'Educação', 
    'Alfabetização', 
    'Programa Saúde na Escola', 
    'Atenção Psicosocial', 
    'Primeira Infância', 
    'Saúde Mental',
    'Telessaúde', 
    'Telessaúde Digital', 
    'Prontuário Eletrônico', 
    'Prontuário', 
    'Plano Nacional da Educação'
]
    URL_BASE = 'https://www.in.gov.br/en/web/dou/-/'
    resultados_por_palavra = {palavra: [] for palavra in palavras_chave}
    nenhum_resultado_encontrado = True

    for resultado in conteudo_raspado['jsonArray']:
        item = {
            'section': 'Seção 1',
            'title': resultado.get('title', 'Título não disponível'),
            'href': URL_BASE + resultado.get('urlTitle', ''),
            'abstract': resultado.get('content', ''),
            'date': resultado.get('pubDate', 'Data não disponível')
        }
        for palavra in palavras_chave:
            if palavra.lower() in item['abstract'].lower():
                resultados_por_palavra[palavra].append(item)
                nenhum_resultado_encontrado = False

    if nenhum_resultado_encontrado:
        print('Nenhum resultado encontrado para as palavras-chave especificadas.')
        return None

    print('Palavras-chave encontradas.')
    return resultados_por_palavra

# Função para Salvar os Resultados na Base de Dados
def salva_na_base(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para salvar.')
        return

    print('Salvando palavras na base de dados...')
    try:
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        conta = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scopes)
        api = gspread.authorize(conta)
        planilha = api.open_by_key(os.getenv('PLANILHA'))
        sheet = planilha.worksheet('Página1')
        rows_to_append = []

        for palavra, lista_resultados in palavras_raspadas.items():
            for item in lista_resultados:
                row = [item['date'], palavra, item['title'], item['href'], item['abstract']]
                rows_to_append.append(row)

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
    port = 587  # Porta para TLS
    email = os.getenv('EMAIL')
    password = os.getenv('SENHA_EMAIL')
    remetente = email
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
    """

    for palavra, lista_resultados in palavras_raspadas.items():
        html += f"<h2>{palavra}</h2>\n"
        if lista_resultados:
            html += "<ul>\n"
            for resultado in lista_resultados:
                html += f"<li><a href='{resultado['href']}'>{resultado['title']}</a></li>\n"
            html += "</ul>\n"
        else:
            html += "<p>Nenhum resultado encontrado para esta palavra-chave.</p>\n"

    html += "</body>\n</html>"

    try:
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()  # Iniciar TLS
        server.login(email, password)  # Autenticar usando sua senha de aplicativo

        mensagem = MIMEMultipart('alternative')
        mensagem["From"] = remetente
        mensagem["To"] = ",".join(destinatarios)
        mensagem["Subject"] = titulo
        conteudo_html = MIMEText(html, "html")
        mensagem.attach(conteudo_html)

        server.sendmail(remetente, destinatarios, mensagem.as_string())
        print('E-mail enviado com sucesso.')
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
    finally:
        server.quit()

# Chamar funções
conteudo_raspado = raspa_dou()  # Obter conteúdo raspado para data específica
palavras_raspadas = procura_termos(conteudo_raspado)
salva_na_base(palavras_raspadas) 
envia_email(palavras_raspadas)
