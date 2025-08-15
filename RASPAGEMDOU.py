# RASPAGEMDOU.py
import os
import re
import json
import unicodedata
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from brevo_python import ApiClient, Configuration
from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
from brevo_python.models.send_smtp_email import SendSmtpEmail
from brevo_python.rest import ApiException

# ================================
# Raspagem do DOU
# ================================
def raspa_dou(data=None):
    if data is None:
        data = datetime.now().strftime('%d-%m-%Y')
    print(f'Raspando as notícias do dia {data}...')
    try:
        url = f'http://www.in.gov.br/leiturajornal?data={data}'
        page = requests.get(url, timeout=30)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        params = soup.find("script", {"id": "params"})
        if params:
            print('Notícias raspadas')
            return json.loads(params.text)
        print("Elemento script #params não encontrado.")
        return None
    except requests.RequestException as e:
        print(f"Erro ao fazer a requisição: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
        return None

# ================================
# Procura termos no conteúdo
# ================================
def procura_termos(conteudo_raspado):
    if conteudo_raspado is None or 'jsonArray' not in conteudo_raspado:
        print('Nenhum conteúdo para analisar ou formato de dados inesperado.')
        return None

    print('Buscando palavras-chave...')
    palavras_chave = [
        'Infância','Criança','Infantil','Infâncias','Crianças',
        'Educação','Ensino','Escolaridade',
        'Plano Nacional da Educação','PNE','Educacional',
        'Alfabetização','Letramento',
        'Saúde','Telessaúde','Telemedicina',
        'Digital','Digitais','Prontuário',
        'Programa Saúde na Escola','PSE',
        'Psicosocial','Mental','Saúde Mental','Dados para a Saúde','Morte Evitável',
        'Doenças Crônicas Não Transmissíveis','Rotulagem de Bebidas Alcoólicas',
        'Educação em Saúde','Bebidas Alcoólicas','Imposto Seletivo',
        'Rotulagem de Alimentos','Alimentos Ultraprocessados',
        'Publicidade Infantil','Publicidade de Alimentos Ultraprocessados',
        'Tributação de Bebidas Alcoólicas','Alíquota de Bebidas Alcoólicas',
        'Cigarro Eletrônico','Controle de Tabaco','Violência Doméstica',
        'Exposição a Fatores de Risco','Departamento de Saúde Mental',
        'Hipertensão Arterial','Alimentação Escolar','PNAE',"Agora Tem Especialistas"
    ]

    URL_BASE = 'https://www.in.gov.br/en/web/dou/-/'
    resultados_por_palavra = {palavra: [] for palavra in palavras_chave}
    algum = False

    for resultado in conteudo_raspado['jsonArray']:
        item = {
            'section': 'Seção 1',
            'title': resultado.get('title', 'Título não disponível'),
            'href': URL_BASE + resultado.get('urlTitle', ''),
            'abstract': resultado.get('content', ''),
            'date': resultado.get('pubDate', 'Data não disponível')
        }
        texto = (item['abstract'] or '').lower()
        for palavra in palavras_chave:
            if re.search(r'\b' + re.escape(palavra.lower()) + r'\b', texto):
                resultados_por_palavra[palavra].append(item)
                algum = True

    if not algum:
        print('Nenhum resultado encontrado para as palavras-chave especificadas.')
        return None

    print('Palavras-chave encontradas.')
    return resultados_por_palavra

# ================================
# Google Sheets (via secret JSON)
# ================================
def _gs_client_from_env():
    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not raw:
        raise RuntimeError("Secret GOOGLE_APPLICATION_CREDENTIALS_JSON não encontrado.")
    info = json.loads(raw)
    if "private_key" in info and "\\n" in info["private_key"]:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def salva_na_base(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para salvar.')
        return

    print('Salvando palavras na base de dados...')
    try:
        gc = _gs_client_from_env()

        planilha_id = os.getenv('PLANILHA')
        if not planilha_id:
            raise RuntimeError("Env PLANILHA não definido (use apenas a key entre /d/ e /edit).")

        sh = gc.open_by_key(planilha_id)
        try:
            sheet = sh.worksheet('Página1')
        except gspread.WorksheetNotFound:
            sheet = sh.add_worksheet(title='Página1', rows="2000", cols="10")

        rows_to_append = []
        for palavra, lista in palavras_raspadas.items():
            for item in lista:
                row = [
                    item.get('date',''),
                    palavra,
                    item.get('title',''),
                    item.get('href',''),
                    item.get('abstract','')
                ]
                rows_to_append.append(row)

        if rows_to_append:
            sheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
            print(f"{len(rows_to_append)} linhas foram adicionadas à planilha.")
        else:
            print('Nenhum dado válido para salvar.')

    except Exception as e:
        print(f'Erro ao salvar dados: {e}')

# ================================
# E-mail (Brevo) com sanitização
# ================================
EMAIL_RE = re.compile(r'<?("?)([^"\s<>@]+@[^"\s<>@]+\.[^"\s<>@]+)\1>?$')

def _sanitize_emails(raw_list: str):
    """Aceita vírgula, ; e quebras de linha; remove invisíveis/aspas/<>; valida; dedup."""
    if not raw_list:
        return []
    parts = re.split(r'[,\n;]+', raw_list)
    emails = []
    for it in parts:
        s = unicodedata.normalize("NFKC", it)
        s = re.sub(r'[\u200B-\u200D\uFEFF]', '', s)  # zero‑width
        s = s.strip().strip("'").strip('"')
        if not s:
            continue
        m = EMAIL_RE.match(s)
        candidate = (m.group(2) if m else s).strip()
        print(f"→ candidato de e-mail: {repr(candidate)}")
        if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", candidate):
            emails.append(candidate.lower())
        else:
            print(f"⚠️ Ignorando e-mail inválido após sanitização: {repr(candidate)}")

    # dedup preservando ordem
    seen = set(); dedup = []
    for e in emails:
        if e not in seen:
            seen.add(e); dedup.append(e)
    return dedup

def envia_email_brevo(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para enviar.')
        return

    print('Enviando e-mail via Brevo...')

    api_key = os.getenv('BREVO_API_KEY')
    if not api_key:
        raise ValueError("❌ BREVO_API_KEY não encontrado no ambiente!")

    sender_email = os.getenv('EMAIL')
    if not sender_email:
        raise ValueError("❌ EMAIL (remetente) não encontrado no ambiente!")

    raw_dest = os.getenv('DESTINATARIOS', '')
    print(f"RAW DESTINATARIOS = {repr(raw_dest)}")
    destinatarios = _sanitize_emails(raw_dest)
    print(f"Destinatários finais = {destinatarios}")

    if not destinatarios:
        raise ValueError("❌ Nenhum destinatário válido em DESTINATARIOS.")

    data = datetime.now().strftime('%d-%m-%Y')
    titulo = f'Busca DOU do dia {data}'
    planilha_url = f'https://docs.google.com/spreadsheets/d/{os.getenv("PLANILHA")}/edit?gid=0'

    parts = [
        "<html><body>",
        "<h1>Consulta ao Diário Oficial da União</h1>",
        f"<p>As matérias encontradas no dia {data} estão listadas a seguir e já foram armazenadas na ",
        f'<a href="{planilha_url}" target="_blank">planilha</a>.</p>'
    ]
    for palavra, lista in (palavras_raspadas or {}).items():
        if lista:
            parts.append(f"<h2>{palavra}</h2><ul>")
            for r in lista:
                link = r.get('href', '#')
                title = r.get('title', '(sem título)')
                parts.append(f"<li><a href='{link}'>{title}</a></li>")
            parts.append("</ul>")
    parts.append("</body></html>")
    html = "".join(parts)

    config = Configuration()
    config.api_key['api-key'] = api_key
    api_client = ApiClient(configuration=config)
    api = TransactionalEmailsApi(api_client)

    for dest in destinatarios:
        print(f"Enviando para: {repr(dest)}")
        send_email = SendSmtpEmail(
            to=[{"email": dest}],
            sender={"email": sender_email},
            subject=titulo,
            html_content=html
        )
        try:
            api.send_transac_email(send_email)
            print(f"✅ E-mail enviado para {dest}")
        except ApiException as e:
            # loga e segue para os próximos
            print(f"❌ Falha ao enviar para {repr(dest)}: {e}\nResposta: {getattr(e, 'body', None)}")
            continue
        except Exception as e:
            print(f"❌ Erro inesperado ao enviar para {repr(dest)}: {e}")
            continue

# ================================
# Execução principal
# ================================
if __name__ == "__main__":
    conteudo_raspado = raspa_dou()
    palavras_raspadas = procura_termos(conteudo_raspado)
    salva_na_base(palavras_raspadas)
    envia_email_brevo(palavras_raspadas)

