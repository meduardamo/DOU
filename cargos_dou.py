import os
import re
import json
from datetime import datetime

import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo

TZ_BR = ZoneInfo("America/Recife")
now_br = lambda: datetime.now(TZ_BR)
today_dou = lambda: now_br().date().strftime("%d-%m-%Y")

# CONFIG (tudo via env/secret)
SHEET_ID = os.getenv("PLANILHA_CARGOS", "").strip()
SHEET_NAME = os.getenv("SHEET_NAME", "").strip()  # se vazio: usa a primeira aba

if not SHEET_ID:
    raise RuntimeError("Env PLANILHA_CARGOS não definido.")

DOU_URL_BASE = "https://www.in.gov.br/leiturajornal"
DOU_MATERIA_BASE = "https://www.in.gov.br/en/web/dou/-/"

HDR = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126 Safari/537.36"
    ),
}

CONTEUDO_MAX = int(os.getenv("DOU_CONTEUDO_MAX", "45000"))

COLS = [
    "Data",
    "Verbos acionados",
    "Termos de cargo",
    "Link",
    "Título",
    "Trecho",
    "Conteúdo",
]

# cargos-alvo
CARGOS_TERMO = [
    "Secretário-Executivo", "Secretária-Executiva",
    "Secretário Nacional", "Secretária Nacional",
    "Secretário Especial", "Secretária Especial",
    "Chefe de Gabinete",
    "Assessor Especial", "Assessora Especial",
    "Assessoria Especial",
    "Chefe da Assessoria Especial",
    "Chefe de Assessoria Especial",
    "Consultoria Jurídica", "CONJUR",
    "Coordenador-Geral", "Coordenadora-Geral",
    "Coordenador Geral", "Coordenadora Geral",
]

# verbos-alvo
VERBOS = ["EXONERAR", "NOMEAR", "DESIGNAR", "DISPENSAR", "TORNAR SEM EFEITO"]
VERBOS_RX = re.compile(r"\b(" + "|".join(VERBOS) + r")\b", re.I)

ORGAO_SINAL_RX = re.compile(
    r"\b("
    r"minist[eé]rio|presid[eê]ncia\s+da\s+rep[uú]blica|casa\s+civil|"
    r"advocacia-geral\s+da\s+uni[aã]o|agu|controladoria-geral\s+da\s+uni[aã]o|cgu|"
    r"secretaria-?geral\s+da\s+presid[eê]ncia|secretaria\s+de\s+comunica[cç][aã]o\s+social"
    r")\b",
    re.I,
)

# filtro negativo simples
EXCLUI_RUIDO_RX = re.compile(
    r"\b(universidade|instituto\s+federal|cefet|reitor(a)?|pr[oó]-reitor|campus)\b",
    re.I,
)

def _normalize(s: str) -> str:
    if s is None:
        return ""
    return str(s).lower()

def _normalize_ws(s: str) -> str:
    return re.sub(r"[^\w]+", " ", _normalize(s), flags=re.UNICODE).strip()

def _compact_ws(s: str) -> str:
    s = re.sub(r"[ \t\r\f\v]+", " ", s or "")
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        k = _normalize_ws(x)
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out

def _build_term_matchers(items: list[str]):
    matchers = []
    union_pats = []
    for it in items:
        toks = [t for t in _normalize_ws(it).split() if t]
        if not toks:
            continue
        pat = r"\b" + r"\s+".join(map(re.escape, toks)) + r"\b"
        rx = re.compile(pat, re.I | re.UNICODE)
        matchers.append((it, rx))
        union_pats.append(pat)
    union_rx = re.compile("(" + "|".join(union_pats) + ")", re.I | re.UNICODE) if union_pats else re.compile(r"$^")
    return matchers, union_rx

CARGO_MATCHERS, CARGO_RX = _build_term_matchers(CARGOS_TERMO)

def _termos_cargo_acionados(texto: str) -> list[str]:
    base = _normalize_ws(texto)
    if not base:
        return []
    out = []
    for termo, rx in CARGO_MATCHERS:
        if rx.search(base):
            out.append(termo)
    return _dedupe(out)

def _sess():
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s = requests.Session()
    r = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    a = HTTPAdapter(max_retries=r)
    s.mount("https://", a)
    s.mount("http://", a)
    return s

HTTP = _sess()

_CONTENT_CACHE: dict[str, str] = {}

def _baixar_conteudo_pagina(url: str) -> str:
    if not url:
        return ""
    if url in _CONTENT_CACHE:
        return _CONTENT_CACHE[url]
    try:
        r = HTTP.get(url, timeout=(10, 75), headers=HDR, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for t in soup(["script", "style", "noscript"]):
            t.decompose()

        # ALTERAÇÃO: em vez de filtrar por classes específicas, pega o texto inteiro do bloco texto-dou
        bloco = soup.select_one("article#materia div.texto-dou") or soup.select_one("div.texto-dou")
        if bloco:
            txt = bloco.get_text("\n", strip=True)
            txt = re.sub(r"[ \t]+", " ", txt).strip()
            if CONTEUDO_MAX and len(txt) > CONTEUDO_MAX:
                txt = txt[:CONTEUDO_MAX] + "…"
            _CONTENT_CACHE[url] = txt
            return txt

        txt = soup.get_text(" ", strip=True)
        txt = re.sub(r"[ \t]+", " ", txt).strip()
        if CONTEUDO_MAX and len(txt) > CONTEUDO_MAX:
            txt = txt[:CONTEUDO_MAX] + "…"
        _CONTENT_CACHE[url] = txt
        return txt
    except Exception:
        return ""

def _get_jsonarray_from_leitura(data_str: str, secao: str) -> list[dict]:
    url = f"{DOU_URL_BASE}?data={data_str}&secao={secao}"
    print(url)
    r = HTTP.get(url, timeout=(10, 75), headers=HDR, allow_redirects=True)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    params = soup.find("script", {"id": "params"})
    raw_json = (params.get_text("\n", strip=True) if params else "") or ""
    raw_json = raw_json.strip()
    if raw_json:
        i = raw_json.find("{")
        j = raw_json.rfind("}")
        if i >= 0 and j > i:
            raw_json = raw_json[i : j + 1]

    if not raw_json:
        m = re.search(r'(\{"jsonArray"\s*:\s*\[.*?\]\s*\})', r.text, flags=re.S)
        raw_json = (m.group(1).strip() if m else "")

    if not raw_json:
        return []

    payload = json.loads(raw_json)
    arr = payload.get("jsonArray", []) or []
    out = []
    for it in arr:
        if isinstance(it, dict):
            it["secao"] = secao
            out.append(it)
    return out

def raspa_dou2_dia(data_str: str, secoes: list[str]) -> dict:
    combined = {"jsonArray": []}
    for sec in secoes:
        try:
            arr = _get_jsonarray_from_leitura(data_str, sec)
            combined["jsonArray"].extend(arr)
            print(f"[{sec}] itens: {len(arr)}")
        except Exception as e:
            print(f"[{sec}] erro: {e}")
    print(f"Total coletado (DO2/DO2E): {len(combined['jsonArray'])}")
    return combined

RESOLVE_TRECHO_RX = re.compile(
    r"\bRESOLVE\b\s*:?\s*(?P<trecho>.*?)(?=\bRESOLVE\b|\Z)", re.I | re.S
)

def _extrai_clipping(texto: str) -> dict | None:
    if not texto:
        return None

    resolves = [m.group("trecho") for m in RESOLVE_TRECHO_RX.finditer(texto)]
    if not resolves:
        resolves = [texto]

    resolve_ok = []
    for r in resolves:
        if VERBOS_RX.search(r):
            resolve_ok.append(_compact_ws(r))

    if not resolve_ok:
        return None

    joined = "\n\n".join(resolve_ok)
    verbos = _dedupe([v.upper() for v in VERBOS_RX.findall(joined)])
    termos = _termos_cargo_acionados(joined)

    if not verbos or not termos:
        return None

    trecho_final = joined
    if len(trecho_final) > 1800:
        trecho_final = trecho_final[:1800] + "…"

    return {
        "Verbos acionados": "; ".join(verbos),
        "Termos de cargo": "; ".join(termos),
        "Trecho": trecho_final,
    }

def procura_cargos(conteudo_raspado: dict) -> list[dict]:
    achados = []
    seen = set()

    for it in (conteudo_raspado or {}).get("jsonArray", []):
        titulo = it.get("title", "") or ""
        resumo = it.get("content", "") or ""
        url_title = it.get("urlTitle", "") or ""
        link = DOU_MATERIA_BASE + url_title

        data_pub = (it.get("pubDate", "") or "")[:10]
        secao = (it.get("secao") or "").strip()

        pre = f"{titulo}\n{resumo}"
        if EXCLUI_RUIDO_RX.search(pre):
            continue
        if not CARGO_RX.search(_normalize_ws(pre)):
            continue

        conteudo_pagina = _baixar_conteudo_pagina(link)
        if not conteudo_pagina:
            continue
        if EXCLUI_RUIDO_RX.search(conteudo_pagina[:2500]):
            continue
        if not ORGAO_SINAL_RX.search(conteudo_pagina[:2500]):
            continue

        clip = _extrai_clipping(conteudo_pagina)
        if not clip:
            continue

        # TIRA secao da chave de dedupe
        k = (
            data_pub,
            _normalize_ws(clip["Verbos acionados"]),
            _normalize_ws(clip["Termos de cargo"]),
            link,
        )
        if k in seen:
            continue
        seen.add(k)

        achados.append({
            "Data": data_pub,
            "Verbos acionados": clip["Verbos acionados"],
            "Termos de cargo": clip["Termos de cargo"],
            "Link": link,
            "Título": titulo,
            "Trecho": clip["Trecho"],
            "Conteúdo": conteudo_pagina,
        })

    return achados

def _gs_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not raw:
        raise RuntimeError("Secret GOOGLE_APPLICATION_CREDENTIALS_JSON não encontrado.")
    info = json.loads(raw)
    if "private_key" in info and "\\n" in info["private_key"]:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def _ensure_header(ws, header: list[str]):
    current = ws.row_values(1)
    if not current:
        ws.resize(rows=max(2, ws.row_count), cols=len(header))
        ws.update("1:1", [header])
        return
    if current[: len(header)] == header:
        return
    ws.resize(rows=max(2, ws.row_count), cols=max(ws.col_count, len(header)))
    ws.update("1:1", [header])

def _get_first_worksheet(sh):
    ws_list = sh.worksheets()
    if not ws_list:
        raise RuntimeError("Planilha sem abas.")
    return ws_list[0]

def _load_existing_keys(ws):
    vals = ws.get_all_values()
    if len(vals) <= 1:
        return set()

    idx = {c: i for i, c in enumerate(vals[0])}
    need = ["Data", "Verbos acionados", "Termos de cargo", "Link"]
    if not all(c in idx for c in need):
        return set()

    out = set()
    for r in vals[1:]:
        def _g(col):
            i = idx[col]
            return (r[i].strip() if i < len(r) else "")
        k = (
            _g("Data"),
            _normalize_ws(_g("Verbos acionados")),
            _normalize_ws(_g("Termos de cargo")),
            _g("Link"),
        )
        if k[-1]:
            out.add(k)
    return out

def salva_planilha(achados: list[dict]):
    if not achados:
        print("sem itens de clipping.")
        return

    gc = _gs_client()
    sh = gc.open_by_key(SHEET_ID)

    if SHEET_NAME:
        ws = sh.worksheet(SHEET_NAME)
    else:
        ws = _get_first_worksheet(sh)

    _ensure_header(ws, COLS)
    existing = _load_existing_keys(ws)

    rows = []
    add = 0

    for a in achados:
        k = (
            a.get("Data", ""),
            _normalize_ws(a.get("Verbos acionados", "")),
            _normalize_ws(a.get("Termos de cargo", "")),
            a.get("Link", ""),
        )
        if k in existing:
            continue
        existing.add(k)

        rows.append([
            a.get("Data", ""),
            a.get("Verbos acionados", ""),
            a.get("Termos de cargo", ""),
            a.get("Link", ""),
            a.get("Título", ""),
            a.get("Trecho", ""),
            a.get("Conteúdo", ""),
        ])
        add += 1

    if not rows:
        print("nada novo.")
        return

    ws.insert_rows(rows, row=2, value_input_option="USER_ENTERED")
    print(f"+{add} linhas anexadas.")

if __name__ == "__main__":
    data_str = os.getenv("DOU_DATE", "").strip() or today_dou()
    conteudo = raspa_dou2_dia(data_str, secoes=["DO2", "DO2E"])
    achados = procura_cargos(conteudo)
    salva_planilha(achados)
