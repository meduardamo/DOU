# RASPAGEMDOU_SECAO1_PAG.py
# Seção 1 do DOU com fallback de paginação (agrega todas as páginas quando necessário)

import os
import re
import json
import unicodedata
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
import gspread
from google.oauth2.service_account import Credentials
from brevo_python import ApiClient, Configuration
from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
from brevo_python.models.send_smtp_email import SendSmtpEmail
from brevo_python.rest import ApiException

BASE = "https://www.in.gov.br"

# ================================
# Raspagem (Seção 1) + fallback de paginação
# ================================
def _fetch_page(url: str):
    """Baixa a página e retorna (soup, blob_json_do_params_ou_None)."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    params = soup.find("script", {"id": "params"})
    blob = json.loads(params.text) if params else None
    return soup, blob

def _discover_other_pages(soup, data_str: str):
    """
    Vasculha os links de paginação da UI e retorna uma lista de URLs absolutas
    para a MESMA data e MESMA secao=do1.
    """
    urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "leiturajornal" not in href:
            continue
        abs_url = urljoin(BASE, href)
        q = parse_qs(urlparse(abs_url).query)
        if q.get("data", [""])[0] == data_str and q.get("secao", [""])[0] == "do1":
            urls.add(abs_url)
    return sorted(urls)

def raspa_dou_secao1(data=None):
    """
    Retorna {'jsonArray': [...], 'pages_visited': [...]}
    Garante agregar todas as páginas, se necessário.
    """
    if data is None:
        data = datetime.now().strftime('%d-%m-%Y')

    print(f"Raspando Seção 1 ({data})…")
    first_url = f"{BASE}/leiturajornal?data={data}&secao=do1"

    soup, blob = _fetch_page(first_url)
    pages = [first_url]

    # Se o blob vier vazio/curto, tenta descobrir as demais páginas e agregá-las
    # Mesmo que venha “cheio”, agregar não fará mal (faremos dedupe por urlTitle).
    other_pages = _discover_other_pages(soup, data)
    for u in other_pages:
        if u not in pages:
            pages.append(u)

    itens = []
    for u in pages:
        try:
            _, b = _fetch_page(u) if u != first_url else (soup, blob)
            if b and "jsonArray" in b:
                itens.extend(b["jsonArray"])
        except Exception as e:
            print(f"⚠️ Falha ao ler {u}: {e}")

    # Dedupe pelo slug (urlTitle). Mantém a 1ª ocorrência.
    dedup = {}
    for r in itens:
        key = r.get("urlTitle") or r.get("url") or r.get("id")
        if key and key not in dedup:
            dedup[key] = r

    arr = list(dedup.values())
    print(f"Total de itens (Seção 1) agregados: {len(arr)} — páginas visitadas: {len(pages)}")
    return {"jsonArray": arr, "pages_visited": pages}


# ================================
# Procura termos
# ================================
def procura_termos(conteudo_raspado):
    if not conteudo_raspado or "jsonArray" not in conteudo_raspado:
        print("Nenhum conteúdo para analisar.")
        return None

    palavras_chave = [
        'Infância','Criança','Infantil','Infâncias','Crianças',
        'Educação','Ensino','Escolaridade',
        'Plano Nacional da Educação','PNE','Educacional',
        'Alfabetização','Letramento',
        'Saúde','Telessaúde','Telemedicina',
        'Digital','Digitais','Prontuário',
        'Programa Saúde na Escola','PSE',
        'Psicossocial','Mental','Saúde Mental','Dados para a Saúde','Morte Evitável',
        'Doenças Crônicas Não Transmissíveis','Rotulagem de Bebidas Alcoólicas',
        'Educação em Saúde','Bebidas Alcoólicas','Imposto Seletivo',
        'Rotulagem de Alimentos','Alimentos Ultraprocessados',
        'Publicidade Infantil','Publicidade de Alimentos Ultraprocessados',
        'Tributação de Bebidas Alcoólicas','Alíquota de Bebidas Alcoólicas',
        'Cigarro Eletrônico','Controle de Tabaco','Violência Doméstica',
        'Exposição a Fatores de Risco','Departamento de Saúde Mental',
        'Hipertensão Arterial','Alimentação Escolar','PNAE', "Agora Tem Especialistas",
        # Alfabetização
        'Alfabetização na Idade Certa','Criança Alfabetizada','Meta de Alfabetização',
        'Plano Nacional de Alfabetização','Programa Criança Alfabetizada',
        'Idade Certa para Alfabetização','Alfabetização Inicial','Alfabetização Plena',
        'Alfabetização em Língua Portuguesa','Analfabetismo','Erradicação do Analfabetismo',
        'Programa Nacional de Alfabetização na Idade Certa','Pacto pela Alfabetização',
        'Política Nacional de Alfabetização','Recomposição das Aprendizagens em Alfabetização',
        'Competências de Alfabetização','Avaliação da Alfabetização','Saeb Alfabetização',
        # Matemática
        'Alfabetização Matemática','Analfabetismo Matemático','Aprendizagem em Matemática',
        'Recomposição das Aprendizagens em Matemática','Recomposição de Aprendizagem',
        'Competências Matemáticas','Proficiência em Matemática',
        'Avaliação Diagnóstica de Matemática','Avaliação Formativa de Matemática',
        'Política Nacional de Matemática','Saeb Matemática','Ideb Matemática',
        'BNCC Matemática','Matemática no Ensino Fundamental','Matemática no Ensino Médio',
        'Anos Iniciais de Matemática','Anos Finais de Matemática',
        'OBMEP','Olimpíada Brasileira de Matemática das Escolas Públicas',
        'Olimpíada de Matemática','PNLD Matemática'
    ]

    URL_BASE = f"{BASE}/web/dou/-/"
    resultados = {p: [] for p in palavras_chave}
    algum = False

    for r in conteudo_raspado["jsonArray"]:
        item = {
            "section": "Seção 1",
            "title": r.get("title", "Título não disponível"),
            "href": URL_BASE + r.get("urlTitle", ""),
            "abstract": r.get("content", ""),
            "date": r.get("pubDate", "Data não disponível"),
        }
        texto = (item["abstract"] or "").lower()
        for p in palavras_chave:
            if re.search(r"\b" + re.escape(p.lower()) + r"\b", texto):
                resultados[p].append(item)
                algum = True

    if not algum:
        print("Nenhum resultado encontrado para as palavras-chave.")
        return None
    return resultados


# ================================
# Google Sheets
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
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def salva_na_base(palavras_raspadas):
    if not palavras_raspadas:
        print("Sem palavras encontradas para salvar.")
        return
    print("Salvando na planilha…")
    try:
        gc = _gs_client_from_env()
        planilha_id = os.getenv("PLANILHA")
        if not planilha_id:
            raise RuntimeError("Env PLANILHA não definido (apenas a key entre /d/ e /edit).")

        sh = gc.open_by_key(planilha_id)
        try:
            sheet = sh.worksheet("Página1")
        except gspread.WorksheetNotFound:
            sheet = sh.add_worksheet(title="Página1", rows="2000", cols="10")

        rows = []
        for palavra, lista in palavras_raspadas.items():
            for item in lista:
                rows.append([
                    item.get("date",""),
                    palavra,
                    item.get("title",""),
                    item.get("href",""),
                    item.get("abstract",""),
                ])
        if rows:
            sheet.append_rows(rows, value_input_option="USER_ENTERED")
            print(f"{len(rows)} linhas adicionadas.")
    except Exception as e:
        print(f"Erro ao salvar dados: {e}")

# ================================
# E-mail (Brevo)
# ================================
EMAIL_RE = re.compile(r'<?("?)([^"\s<>@]+@[^"\s<>@]+\.[^"\s<>@]+)\1>?$')

def _sanitize_emails(raw_list: str):
    if not raw_list:
        return []
    parts = re.split(r"[,\n;]+", raw_list)
    emails = []
    for it in parts:
        s = unicodedata.normalize("NFKC", it)
        s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s).strip().strip("'").strip('"')
        if not s:
            continue
        m = EMAIL_RE.match(s)
        cand = (m.group(2) if m else s).strip()
        if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", cand):
            emails.append(cand.lower())
        else:
            print(f"⚠️ Ignorando e-mail inválido: {repr(cand)}")
    # dedupe preservando ordem
    seen, dedup = set(), []
    for e in emails:
        if e not in seen:
            seen.add(e); dedup.append(e)
    return dedup

def envia_email_brevo(palavras_raspadas):
    if not palavras_raspadas:
        print("Sem palavras para enviar.")
        return
    print("Enviando e-mail via Brevo…")
    api_key = os.getenv("BREVO_API_KEY")
    sender_email = os.getenv("EMAIL")
    raw_dest = os.getenv("DESTINATARIOS", "")
    if not api_key or not sender_email:
        raise ValueError("Configure BREVO_API_KEY e EMAIL no ambiente.")
    destinatarios = _sanitize_emails(raw_dest)
    if not destinatarios:
        raise ValueError("Nenhum destinatário válido em DESTINATARIOS.")

    data = datetime.now().strftime("%d-%m-%Y")
    titulo = f"Busca DOU (Seção 1) — {data}"
    planilha_url = f'https://docs.google.com/spreadsheets/d/{os.getenv("PLANILHA")}/edit?gid=0'

    parts = [
        "<html><body>",
        "<h1>Consulta ao Diário Oficial da União — Seção 1</h1>",
        f"<p>Resultados do dia {data} (agregando todas as páginas, se houver). ",
        f"Dados salvos na <a href='{planilha_url}' target='_blank'>planilha</a>.</p>"
    ]
    for palavra, lista in palavras_raspadas.items():
        if not lista: 
            continue
        parts.append(f"<h2>{palavra}</h2><ul>")
        for r in lista:
            parts.append(f"<li><a href='{r.get('href','#')}'>{r.get('title','(sem título)')}</a></li>")
        parts.append("</ul>")
    parts.append("</body></html>")
    html = "".join(parts)

    config = Configuration()
    config.api_key["api-key"] = api_key
    api = TransactionalEmailsApi(ApiClient(configuration=config))
    for dest in destinatarios:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}],
                sender={"email": sender_email},
                subject=titulo,
                html_content=html
            ))
            print(f"✅ E-mail enviado para {dest}")
        except ApiException as e:
            print(f"❌ Falha ao enviar para {dest}: {e} — {getattr(e,'body',None)}")

# ================================
# Execução principal
# ================================
if __name__ == "__main__":
    blob = raspa_dou_secao1()                         # agrega todas as páginas da Seção 1
    palavras = procura_termos(blob)
    salva_na_base(palavras)
    envia_email_brevo(palavras)
