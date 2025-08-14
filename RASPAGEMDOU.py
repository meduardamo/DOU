# -*- coding: utf-8 -*-
import os
import re
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from brevo_python import ApiClient, Configuration
from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
from brevo_python.models.send_smtp_email import SendSmtpEmail

# ================================
# Raspagem do DOU
# ================================
def raspa_dou(data_str=None):
    if data_str is None:
        data_str = datetime.now().strftime('%d-%m-%Y')
    print(f'Raspando as notícias do dia {data_str}...')
    try:
        url = f'http://www.in.gov.br/leiturajornal?data={data_str}'
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        params = soup.find("script", {"id": "params"})
        if not params:
            print("Elemento <script id='params'> não encontrado.")
            return None
        print('Notícias raspadas')
        return json.loads(params.text)
    except requests.RequestException as e:
        print(f"Erro de rede ao acessar o DOU: {e}")
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON do DOU: {e}")
    return None

# ================================
# Busca por termos
# ================================
PALAVRAS_CHAVE = [
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
    'Hipertensão Arterial', 'Alimentação Escolar', 'PNAE', 'Agora Tem Especialistas'
]

def procura_termos(conteudo_raspado):
    if not conteudo_raspado or 'jsonArray' not in conteudo_raspado:
        print('Nenhum conteúdo para analisar ou formato de dados inesperado.')
        return None

    print('Buscando palavras-chave...')
    base = 'https://www.in.gov.br/en/web/dou/-/'
    resultados = {p: [] for p in PALAVRAS_CHAVE}
    houve = False

    for it in conteudo_raspado['jsonArray']:
        item = {
            'section': it.get('pubName', 'Seção 1'),
            'title': it.get('title', 'Título não disponível'),
            'href': base + it.get('urlTitle', ''),
            'abstract': it.get('content', '') or '',
            'date': it.get('pubDate', '')
        }
        texto = item['abstract'].lower()
        for palavra in PALAVRAS_CHAVE:
            if re.search(r'\b' + re.escape(palavra.lower()) + r'\b', texto):
                resultados[palavra].append(item)
                houve = True

    if not houve:
        print('Nenhum resultado encontrado para as palavras-chave especificadas.')
        return None
    print('Palavras-chave encontradas.')
    return resultados

# ================================
# Google Sheets
# ================================
def get_gspread_client_from_env():
    """Lê secret do env (uma linha com \\n) e devolve gspread client (google-auth)."""
    raw = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("Secret GCP_SERVICE_ACCOUNT_JSON não encontrado no ambiente.")
    info = json.loads(raw)
    if "private_key" in info and "\\n" in info["private_key"]:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    info["token_uri"] = "https://oauth2.googleapis.com/token"
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds)

def salva_na_base(por_palavra):
    if not por_palavra:
        print('Sem palavras encontradas para salvar.')
        return

    print('Salvando palavras na base de dados...')
    try:
        api = get_gspread_client_from_env()
        spreadsheet_key = os.getenv('PLANILHA')  # só a KEY, não a URL
        if not spreadsheet_key:
            raise RuntimeError("Variável PLANILHA não definida (espera a KEY da planilha).")
        ws_name = os.getenv('ABA', 'Página1')

        sh = api.open_by_key(spreadsheet_key)
        try:
            ws = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=ws_name, rows="100", cols="20")

        rows = []
        for palavra, lista in por_palavra.items():
            for it in lista:
                rows.append([it['date'], palavra, it['title'], it['href'], it['abstract']])

        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            print(f'✅ {len(rows)} linhas adicionadas em "{ws_name}".')
        else:
            print('Nenhum dado válido para salvar.')
    except Exception as e:
        print(f'Erro ao salvar dados: {e}')

# ================================
# Brevo (Sendinblue)
# ================================
def envia_email_brevo(por_palavra):
    if not por_palavra:
        print('Sem palavras encontradas para enviar.')
        return

    print('Enviando e-mail via Brevo...')
    api_key = os.getenv('BREVO_API_KEY')
    remetente = os.getenv('EMAIL')
    dests = os.getenv('DESTINATARIOS', '')
    planilha_key = os.getenv('PLANILHA')

    if not api_key or not remetente or not dests or not planilha_key:
        print("⚠️ Variáveis BREVO_API_KEY, EMAIL, DESTINATARIOS ou PLANILHA ausentes.")
        return

    destinatarios = [d.strip() for d in dests.split(',') if d.strip()]
    data = datetime.now().strftime('%d-%m-%Y')
    titulo = f'Busca DOU do dia {data}'
    planilha_url = f'https://docs.google.com/spreadsheets/d/{planilha_key}/edit?gid=0'

    html = [
        "<html><body>",
        "<h1>Consulta ao Diário Oficial da União</h1>",
        f"<p>As matérias encontradas no dia {data} foram armazenadas na ",
        f"<a href='{planilha_url}' target='_blank'>planilha</a>.</p>"
    ]
    for palavra, lista in por_palavra.items():
        if lista:
            html.append(f"<h2>{palavra}</h2><ul>")
            for r in lista:
                html.append(f"<li><a href='{r['href']}'>{r['title']}</a></li>")
            html.append("</ul>")
    html.append("</body></html>")
    html = "".join(html)

    cfg = Configuration()
    cfg.api_key['api-key'] = api_key
    api_client = ApiClient(configuration=cfg)
    api = TransactionalEmailsApi(api_client)

    for dest in destinatarios:
        email = SendSmtpEmail(
            to=[{"email": dest}],
            sender={"email": remetente},
            subject=titulo,
            html_content=html
        )
        api.send_transac_email(email)
        print(f"✅ E-mail enviado para {dest}")

# ================================
# Execução
# ================================
if __name__ == "__main__":
    conteúdo = raspa_dou()
    por_palavra = procura_termos(conteúdo)
    salva_na_base(por_palavra)
    envia_email_brevo(por_palavra)
