import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import json
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
from brevo_python import ApiClient, Configuration
from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
from brevo_python.model.send_smtp_email import SendSmtpEmail

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
    'Infância', 'Criança', 'Infantil', 'Infâncias', 'Crianças', 
    'Educação', 'Ensino', 'Escolaridade',
    'Plano Nacional da Educação', 'PNE', 'Educacional',
    'Alfabetização', 'Letramento',
    'Saúde', 'Telessaúde', 'Telemedicina',
    'Digital', 'Digitais', 'Prontuário',
    'Programa Saúde na Escola', 'PSE', 
    'Psicosocial', 'Mental',
    'Saúde Mental', 'Dados para a Saúde', 'Morte Evitável', 
    'Doenças Crônicas Não Transmissíveis', 'Rotulagem de Bebidas Alcoólicas', 
    'Educação em Saúde', 'Bebidas Alcoólicas', 'Imposto Seletivo', 
    'Rotulagem de Alimentos', 'Alimentos Ultraprocessados', 
    'Publicidade Infantil', 'Publicidade de Alimentos Ultraprocessados', 
    'Tributação de Bebidas Alcoólicas', 'Alíquota de Bebidas Alcoólicas', 
    'Cigarro Eletrônico', 'Controle de Tabaco', 'Violência Doméstica', 
    'Exposição a Fatores de Risco', 'Departamento de Saúde Mental', 
    'Hipertensão Arterial', 'Alimentação Escolar', 'PNAE', "Agora Tem Especialistas"
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
        texto = item['abstract'].lower()
        for palavra in palavras_chave:
            # Busca pela palavra como uma palavra completa, não como substring
            if re.search(r'\b' + re.escape(palavra.lower()) + r'\b', texto):
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

# Função para Enviar Email
def envia_email_brevo(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para enviar.')
        return

    print('Enviando e-mail via Brevo...')
    email = os.getenv('EMAIL')  # remetente (precisa estar validado no Brevo)
    destinatarios = os.getenv('DESTINATARIOS').split(',')
    data = datetime.now().strftime('%d-%m-%Y')
    titulo = f'Busca DOU do dia {data}'
    planilha_url = f'https://docs.google.com/spreadsheets/d/{os.getenv("PLANILHA")}/edit?gid=0'

    # Montar HTML do e-mail
    html = f"""
    <html><body>
      <h1>Consulta ao Diário Oficial da União</h1>
      <p>As matérias encontradas no dia {data} estão listadas a seguir e já foram armazenadas na
      <a href="{planilha_url}" target="_blank">planilha</a>.</p>
    """
    for palavra, lista_resultados in palavras_raspadas.items():
        if lista_resultados:
            html += f"<h2>{palavra}</h2><ul>"
            for r in lista_resultados:
                html += f"<li><a href='{r['href']}'>{r['title']}</a></li>"
            html += "</ul>"
    html += "</body></html>"

    # Configurar cliente Brevo
    config = Configuration()
    config.api_key['api-key'] = os.getenv('BREVO_API_KEY')

    with ApiClient(config) as api_client:
        api = TransactionalEmailsApi(api_client)
        for dest in destinatarios:
            send_email = SendSmtpEmail(
                to=[{"email": dest}],
                sender={"email": email},
                subject=titulo,
                html_content=html
            )
            api.send_transac_email(send_email)
            print(f"✅ E-mail enviado para {dest}")

# Chamar funções
conteudo_raspado = raspa_dou()  # Obter conteúdo raspado para data específica
palavras_raspadas = procura_termos(conteudo_raspado)
salva_na_base(palavras_raspadas) 
envia_email_brevo(palavras_raspadas)
