"""Peças comuns das raspagens dedicadas do DOU (transição federal, MegaEdu).

Lê a listagem de uma seção do DOU, baixa o texto de um ato, normaliza texto,
abre planilha com a conta de serviço e formata aba no padrão Eixo. O
dou_unificado.py tem as suas próprias versões e não usa este módulo.
"""

import json
import os
import re
import time
import unicodedata
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo

TZ_BR = ZoneInfo("America/Sao_Paulo")
URL_ATO = "https://www.in.gov.br/en/web/dou/-/"

HDR = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126 Safari/537.36"
    ),
}


def norm(s: str) -> str:
    """Minúsculo, sem acento e sem pontuação: "Lei nº 14.180" vira "lei n 14 180"."""
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def _sessao():
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.7,
              status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",),
              raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=r, pool_maxsize=16))
    return s


HTTP = _sessao()


def link(it: dict) -> str:
    return URL_ATO + (it.get("urlTitle") or "")


def edicao(data: str, secao: str) -> list[dict] | None:
    """Itens de uma seção (DO1, DO2E...) num dia dd-mm-aaaa, com o campo "secao".
    None se o site não responder (502 etc.) depois das tentativas."""
    url = f"https://www.in.gov.br/leiturajornal?data={data}&secao={secao}"
    for tentativa in range(3):
        try:
            r = HTTP.get(url, timeout=(10, 75), headers=HDR)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            params = soup.find("script", {"id": "params"})
            raw = (params.get_text() if params else "").strip()
            if not raw:
                m = re.search(r'(\{"jsonArray"\s*:\s*\[.*?\]\s*\})', r.text, flags=re.S)
                raw = m.group(1) if m else ""
            if not raw:
                return []
            raw = raw[raw.find("{"): raw.rfind("}") + 1]
            arr = json.loads(raw).get("jsonArray", []) or []
            for it in arr:
                it["secao"] = secao
            return [it for it in arr if isinstance(it, dict)]
        except Exception as e:
            print(f"[{data} {secao}] tentativa {tentativa + 1}: {e}")
            time.sleep(10 * (tentativa + 1))
    print(f"[{data} {secao}] não respondeu")
    return None


def edicoes(pares: list[tuple[str, str]], espera_min: float | None = None
            ) -> tuple[dict[tuple[str, str], list[dict]], list[tuple[str, str]]]:
    """Lê várias seções (data, secao). Uma seção fora do ar não derruba a rodada:
    as que responderam seguem, e as que não responderam são tentadas de novo a
    cada 10 min até espera_min (env DOU_ESPERA_MIN, padrão 0 = sem espera).
    Em 24/11/2026 o in.gov.br deu 502 ao GitHub das 5h40 às ~8h50.
    Devolve ({(data, secao): itens}, [(data, secao) que ficaram faltando])."""
    if espera_min is None:
        espera_min = float(os.getenv("DOU_ESPERA_MIN") or 0)
    lidas: dict[tuple[str, str], list[dict]] = {}
    faltam = list(pares)
    limite = time.monotonic() + espera_min * 60
    while True:
        for p in list(faltam):
            itens = edicao(*p)
            if itens is not None:
                lidas[p] = itens
                faltam.remove(p)
        resta = limite - time.monotonic()
        if not faltam or resta <= 0:
            break
        pausa = min(600, resta)
        print(f"DOU fora do ar em {', '.join(f'{d} {s}' for d, s in faltam)}; "
              f"nova tentativa em {pausa / 60:.0f} min.")
        time.sleep(pausa)
    for d, s in faltam:
        print(f"::warning::DOU {d} {s} não respondeu; fica para a próxima rodada.")
    return lidas, faltam


def texto_ato(url: str, por_paragrafo: bool = False) -> str:
    """Texto do ato. Vazio se a página não abrir.

    Padrão: quebra de linha em todo elemento, inclusive no verbo em negrito
    ("NOMEAR\nFULANO"), que é o que a transição espera. Com por_paragrafo=True,
    uma linha por <p>, com o negrito junto do resto da frase."""
    html = ""
    for tentativa in range(3):
        try:
            r = HTTP.get(url, timeout=(10, 75), headers=HDR)
            if r.status_code == 403 and tentativa < 2:  # o in.gov.br barra rajada
                time.sleep(8 * (tentativa + 1))
                continue
            r.raise_for_status()
            html = r.text
            break
        except Exception as e:
            print(f"falha ao baixar {url}: {e}")
            return ""
    soup = BeautifulSoup(html, "html.parser")
    bloco = soup.select_one("article#materia div.texto-dou") or soup.select_one("div.texto-dou")
    if not bloco:
        return ""
    if por_paragrafo:
        ps = [p.get_text(" ", strip=True) for p in bloco.find_all(["p", "li"])]
        txt = "\n".join(p for p in ps if p) or bloco.get_text(" ", strip=True)
        return re.sub(r"[ \t]+", " ", txt).strip()
    return re.sub(r"[ \t]+", " ", bloco.get_text("\n", strip=True)).strip()


def datas_padrao() -> list[str]:
    """DOU_DATE (lista dd-mm-aaaa separada por vírgula) ou ontem e hoje.

    Ontem entra porque a edição extra sai à noite, depois da última rodada."""
    fixa = os.getenv("DOU_DATE", "").strip()
    if fixa:
        return [d.strip() for d in fixa.split(",") if d.strip()]
    hoje = datetime.now(TZ_BR).date()
    return [(hoje - timedelta(days=1)).strftime("%d-%m-%Y"), hoje.strftime("%d-%m-%Y")]


def abre_planilha(sheet_id: str, nome_secret: str):
    """Planilha pela conta de serviço do secret GOOGLE_APPLICATION_CREDENTIALS_JSON."""
    import gspread
    from google.oauth2.service_account import Credentials

    if not sheet_id:
        raise RuntimeError(f"Secret {nome_secret} não definido.")
    info = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    if "\\n" in info.get("private_key", ""):
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(sheet_id)


def aba(sh, nome: str, colunas: list[str], larguras: list[int]):
    """Abre a aba ou cria com cabeçalho e formato da Eixo."""
    import gspread
    try:
        return sh.worksheet(nome)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(nome, rows=2, cols=len(colunas))
        ws.update(range_name="A1", values=[colunas])
        formata(sh, ws, larguras)
        return ws


def formata(sh, ws, larguras: list[int]):
    """Montserrat 10, cabeçalho marinho, corpo gelo, alinhamento no meio."""
    marinho = {"red": 0x19 / 255, "green": 0x2D / 255, "blue": 0x4E / 255}
    gelo = {"red": 0xF4 / 255, "green": 0xF3 / 255, "blue": 0xEF / 255}
    reqs = [
        {"repeatCell": {
            "range": {"sheetId": ws.id},
            "cell": {"userEnteredFormat": {
                "textFormat": {"fontFamily": "Montserrat", "fontSize": 10},
                "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP",
                "backgroundColor": gelo}},
            "fields": "userEnteredFormat(textFormat,verticalAlignment,wrapStrategy,backgroundColor)"}},
        {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {
                "backgroundColor": marinho,
                "textFormat": {"fontFamily": "Montserrat", "fontSize": 10, "bold": True,
                               "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)"}},
        {"updateSheetProperties": {
            "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"}},
    ]
    for i, px in enumerate(larguras):
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": px}, "fields": "pixelSize"}})
    sh.batch_update({"requests": reqs})


def grava_novos(ws, colunas: list[str], linhas: list[dict], chave) -> list[dict]:
    """Insere no topo só as linhas cuja chave(linha) ainda não está na aba."""
    vals = ws.get_all_values()
    idx = {c: i for i, c in enumerate(vals[0])} if vals else {}
    existentes = set()
    for r in vals[1:]:
        existentes.add(chave({c: (r[i] if i < len(r) else "") for c, i in idx.items()}))
    novos = []
    for a in linhas:
        k = chave(a)
        if k in existentes:
            continue
        existentes.add(k)
        novos.append(a)
    if novos:
        ws.insert_rows([[a.get(c, "") for c in colunas] for a in novos], row=2,
                       value_input_option="RAW")
        # Grade no tamanho dos dados: a aba nasce com uma linha vazia que desce a cada inserção.
        ws.resize(rows=max(len(vals), 1) + len(novos))
    return novos
