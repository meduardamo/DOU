"""Transição federal: nomeação, exoneração e designação nos cargos acompanhados.

Lê as Seções 1 e 2 do DOU (regular e extra) de hoje e de ontem, porque a
edição extra sai à noite, depois da última rodada do dia. Grava os achados na
aba "DOU federal" da planilha de transição e manda email só quando entra
linha nova. Dedup por link + cargo + movimento + nome, então rodar várias
vezes no dia não repete nada.

Os cargos vêm da lista pedida em 27/10/2026. O casamento é pelo nome do cargo
escrito no ato ("para exercer o cargo de Secretário de Educação Básica do
Ministério da Educação"), não pelo órgão que publica: quem nomeia secretário e
presidente de autarquia é a Casa Civil.
"""

import os
import re
import sys
import json
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

SHEET_ID = os.getenv("PLANILHA_TRANSICAO", "").strip()
ABA = "DOU federal"
SECOES = ["DO1", "DO2", "DO1E", "DO2E"]

HDR = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126 Safari/537.36"
    ),
}

COLS = ["Data", "Seção", "Movimento", "Órgão", "Cargo", "Nome", "Trecho", "Ato", "Link"]

MEC = "Ministério da Educação"
ESP = "Ministério do Esporte"
MDS = "Ministério do Desenvolvimento e Assistência Social"
SAU = "Ministério da Saúde"
MUL = "Ministério das Mulheres"
MTE = "Ministério do Trabalho e Emprego"
CC = "Casa Civil"

# Ministério como aparece no texto do ato (sem acento, minúsculo) -> rótulo.
# "trabalho" sozinho cobre uma eventual volta do Trabalho e Previdência.
MINISTERIOS = [
    (r"\beducacao\b", MEC),
    (r"\besporte\b", ESP),
    (r"\bdesenvolvimento e assistencia social\b", MDS),
    (r"\bsaude\b", SAU),
    (r"\bmulheres\b", MUL),
    (r"\btrabalho\b", MTE),
    (r"\bcasa civil\b", CC),
]

# Cargo de chefia de unidade: (órgão, rótulo, regex no começo do nome do cargo).
UNIDADES = [
    (MEC, "Secretaria de Educação Básica (SEB)", r"secretari[oa] de educacao basica"),
    (MEC, "Secretaria de Educação Profissional e Tecnológica (SETEC)",
     r"secretari[oa] de educacao profissional e tecnologica"),
    (MEC, "Subsecretaria da Política Nacional Integrada da Primeira Infância",
     r"subsecretari[oa] (?:da subsecretaria )?da politica nacional integrada da primeira infancia"),
    (MEC, "FNDE", r"presidente do fundo nacional de desenvolvimento da educacao"),
    (MEC, "INEP", r"presidente do instituto nacional de estudos e pesquisas educacionais"),
    (ESP, "Secretaria Nacional de Esporte Amador, Educação, Lazer e Inclusão Social (SNEAELIS)",
     r"secretari[oa] nacional de esporte amador"),
    (ESP, "Diretoria de Programas e Políticas de Incentivo ao Esporte (DPPIE)",
     r"diretora? (?:da diretoria )?de programas e politicas de incentivo ao esporte"),
    (MDS, "Secretaria Nacional da Política de Cuidados e Família (SNCF)",
     r"secretari[oa] nacional da politica de cuidados e familia"),
    (MDS, "Departamento de Políticas de Cuidados da Primeira Infância e da Pessoa Idosa",
     r"diretora? do departamento de politicas de cuidados da primeira infancia e da pessoa idosa"),
    (MDS, "Secretaria Nacional de Assistência Social (SNAS)",
     r"secretari[oa] nacional de assistencia social"),
    (MDS, "Secretaria de Avaliação, Gestão da Informação e Cadastro Único (SAGICAD)",
     r"secretari[oa] de avaliacao gestao da informacao e cadastro unico"),
    (MDS, "Secretaria Nacional de Renda de Cidadania (SENARC)",
     r"secretari[oa] nacional de renda de cidadania"),
    (SAU, "Secretaria de Atenção Primária à Saúde (SAPS)",
     r"secretari[oa] de atencao primaria a saude"),
    (MUL, "Secretaria Nacional de Autonomia Econômica",
     r"secretari[oa] nacional de autonomia economica"),
]
UNIDADES_RX = [(o, r, re.compile(r"^" + p)) for o, r, p in UNIDADES]

# Ministro e secretário-executivo: só dos órgãos da lista.
MINISTRO_DE = {MEC, ESP, MDS, SAU, MUL, MTE, CC}
SE_DE = {MEC, ESP, MDS, MUL, CC}

MINISTRO_RX = re.compile(r"^ministr[oa] de estado (?:chefe )?(?:d[aoe]s? )?(?P<resto>.*)")
SE_RX = re.compile(r"^secretari[oa] executiv[oa](?! adjunt)(?P<resto>.*)")
# Depois de "Secretário-Executivo" só pode vir o órgão. "Secretária-Executiva do
# Conselho da Federação" ou "da Câmara de Comércio Exterior" é outro cargo.
SE_RESTO_OK = re.compile(r"^\s*(?:da secretaria executiva\s*)?(?:(?:do|deste) ministerio|da casa civil|$)")

VERBOS = ["TORNAR SEM EFEITO", "EXONERAR", "NOMEAR", "DESIGNAR", "DISPENSAR"]
MOVIMENTO = {
    "NOMEAR": "Nomeação", "EXONERAR": "Exoneração", "DESIGNAR": "Designação",
    "DISPENSAR": "Dispensa", "TORNAR SEM EFEITO": "Sem efeito",
}
VERBO_RX = re.compile(r"\b(" + "|".join(v.replace(" ", r"\s+") for v in VERBOS) + r")\b", re.I)

# Nome do cargo dentro do ato: "cargo de X", "função comissionada executiva de X".
# Não corta no código: em "função de Subsecretária, código FCE 1.16, da
# Subsecretaria da Política..." a unidade vem depois dele. O código sai em _sem_codigo.
CARGO_RX = re.compile(
    r"\b(?:cargo|fun[cç][aã]o)\s+(?:em\s+comiss[aã]o\s+)?(?:de\s+natureza\s+especial\s+)?"
    r"(?:comissionad[ao]\s+executiv[ao]\s+)?de\s+(?P<cargo>.+?)"
    r"(?=,\s*(?:a\s+contar|a\s+partir|com\s+efeito|ficando|em\s+substitui|nos\s+afastamentos)"
    r"|;|\.\s|\.$|\n|$)",
    re.I | re.S,
)
CODIGO_RX = re.compile(r",?\s*(?:c[oó]digo\s+)?(?:CCE|FCE|DAS|CDT|NE)\s*[\d\.]*\s*,?", re.I)


def _sem_codigo(cargo: str) -> str:
    return re.sub(r"\s+", " ", CODIGO_RX.sub(" ", cargo)).strip(" ,")

# Substituto eventual e interino por período (férias, viagem) são rotina e
# ficam de fora. "Na vacância da função" é texto padrão do substituto eventual,
# então não serve de exceção; interino sem período ("até a nomeação do
# titular") entra.
TEMPORARIO_RX = re.compile(r"substitut|interin")
PERIODO_RX = re.compile(r"substituto eventual|periodo|nos dias|no dia|impediment|afastament|ferias|viage|ausencia")

# Órgão que publica e pode conter ato desses cargos (decreto e portaria da Casa
# Civil saem sob Presidência da República). Universidade, instituto federal e
# hospital do MEC e da Saúde publicam muito ato de pessoal e nunca destes cargos.
PUBLICA_RX = re.compile(
    r"^(presid[eê]ncia da rep[uú]blica|atos do poder executivo|minist[eé]rio da educa[cç][aã]o|minist[eé]rio do esporte|"
    r"minist[eé]rio do desenvolvimento e assist[eê]ncia social|minist[eé]rio da sa[uú]de|"
    r"minist[eé]rio das mulheres|minist[eé]rio do trabalho)",
    re.I,
)
FORA_RX = re.compile(
    r"universidade|instituto federal|centro federal|col[eé]gio|hospital|"
    r"funda[cç][aã]o universidade|empresa brasileira de servi[cç]os hospitalares|"
    r"funda[cç][aã]o oswaldo cruz|ag[eê]ncia nacional|instituto nacional do c[aâ]ncer",
    re.I,
)


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def _sess():
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.7,
              status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",),
              raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=r))
    return s


HTTP = _sess()


def _edicao(data: str, secao: str) -> list[dict]:
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
    raise RuntimeError(f"DOU {data} {secao} não respondeu")


def _texto_ato(url: str) -> str:
    try:
        r = HTTP.get(url, timeout=(10, 75), headers=HDR)
        r.raise_for_status()
    except Exception as e:
        print(f"falha ao baixar {url}: {e}")
        return ""
    soup = BeautifulSoup(r.text, "html.parser")
    bloco = soup.select_one("article#materia div.texto-dou") or soup.select_one("div.texto-dou")
    if not bloco:
        return ""
    return re.sub(r"[ \t]+", " ", bloco.get_text("\n", strip=True)).strip()


def _candidato(it: dict) -> bool:
    hier = (it.get("hierarchyStr") or "").strip()
    if not PUBLICA_RX.search(hier) or FORA_RX.search(hier):
        return False
    tipo = _norm(it.get("artType"))
    if tipo and not re.search(r"decreto|portaria|ato|despacho|retifica", tipo):
        return False
    resumo = _norm(it.get("content"))
    # Resumo inteiro (sem reticências) e sem verbo de pessoal: não é ato de cargo.
    cortado = (it.get("content") or "").rstrip().endswith("...")
    if not cortado and not re.search(r"\b(nomear|exonerar|designar|dispensar|tornar sem efeito)\b", resumo):
        return False
    return True


def _orgao_do_cargo(resto_norm: str, hier: str) -> str | None:
    for rx, rotulo in MINISTERIOS:
        if re.search(rx, resto_norm):
            return rotulo
    h = _norm(hier.split("/")[0] + " " + (hier.split("/")[1] if "/" in hier else ""))
    if "presidencia da republica" in h and "casa civil" not in h:
        return None
    for rx, rotulo in MINISTERIOS:
        if re.search(rx, h):
            return rotulo
    return None


def _casa_cargo(cargo: str, hier: str) -> tuple[str, str] | None:
    c = _norm(_sem_codigo(cargo))
    if re.search(r"\badjunt", c[:90]):  # Secretário-Executivo Adjunto, Secretário de APS Adjunto
        return None
    m = MINISTRO_RX.match(c)
    if m:
        orgao = _orgao_do_cargo(m.group("resto"), "")
        if orgao in MINISTRO_DE:
            return orgao, ("Ministro-chefe da Casa Civil" if orgao == CC else "Ministro(a)")
        return None
    m = SE_RX.match(c)
    if m:
        if not SE_RESTO_OK.match(m.group("resto")):
            return None
        orgao = _orgao_do_cargo(m.group("resto"), hier)
        if orgao in SE_DE:
            return orgao, "Secretaria-Executiva"
        return None
    for orgao, rotulo, rx in UNIDADES_RX:
        if rx.match(c):
            return orgao, rotulo
    return None


def _nome(segmento: str, verbo: str) -> str:
    s = segmento[len(verbo):] if segmento.upper().startswith(verbo) else segmento
    s = re.sub(r"^\s*,?\s*(a\s+pedido|ex\s+officio|a\s+partir\s+de[^,]*|por\s+necessidade[^,]*)\s*,?",
               "", s, flags=re.I)
    s = re.sub(r"^\s*,?\s*(a\s+pedido)\s*,?", "", s, flags=re.I)
    s = re.sub(r"^\s*,?\s*(?:o|a)\s+(?:servidora?|senhora?|empregad[oa]|militar)\s+", " ", s, flags=re.I)
    m = re.match(r"\s*(?:o\s+ato\s+[^,]*?\s+que\s+(?:nomeou|exonerou|designou|dispensou)\s+)?"
                 r"(?P<n>[A-ZÀ-Ý][A-ZÀ-Ýa-zà-ÿ'\.\s-]{4,}?)"
                 r"(?=\s*,|\s+para\b|\s+do\s+cargo|\s+da\s+fun|\s+de\s+exercer|\s+do\s+exerc|\s+como\b)",
                 s)
    return re.sub(r"\s+", " ", m.group("n")).strip() if m else ""


def extrai(it: dict, texto: str) -> list[dict]:
    hier = it.get("hierarchyStr") or ""
    m = re.search(r"\bresolve\b|\bdecreta\b|\bart\.\s*1", texto, re.I)
    corpo = texto[m.start():] if m else texto
    verbos = list(VERBO_RX.finditer(corpo))
    achados = []
    for i, v in enumerate(verbos):
        fim = verbos[i + 1].start() if i + 1 < len(verbos) else len(corpo)
        seg = corpo[v.start():fim].strip()
        verbo = re.sub(r"\s+", " ", v.group(1).upper())
        for cm in CARGO_RX.finditer(seg):
            cargo = re.sub(r"\s+", " ", cm.group("cargo")).strip()
            casado = _casa_cargo(cargo, hier)
            if not casado:
                continue
            seg_n = _norm(seg)
            if TEMPORARIO_RX.search(seg_n) and PERIODO_RX.search(seg_n):
                continue
            orgao, rotulo = casado
            # Até o fim da frase do cargo; depois vem assinatura e o ato seguinte.
            fim_frase = re.compile(r"\.(?=\s|$)").search(seg, cm.end())
            trecho = re.sub(r"\s+", " ", seg[: fim_frase.end()] if fim_frase else seg)
            achados.append({
                "Data": it.get("pubDate", ""),
                "Seção": it.get("secao", ""),
                "Movimento": MOVIMENTO.get(verbo, verbo.title()),
                "Órgão": orgao,
                "Cargo": rotulo,
                "Nome": _nome(seg, verbo),
                "Trecho": trecho[:1500] + ("…" if len(trecho) > 1500 else ""),
                "Ato": it.get("title", ""),
                "Link": "https://www.in.gov.br/en/web/dou/-/" + (it.get("urlTitle") or ""),
            })
            break  # um achado por segmento
    return achados


def varre(datas: list[str]) -> list[dict]:
    achados, vistos = [], set()
    for data in datas:
        for sec in SECOES:
            itens = _edicao(data, sec)
            cand = [it for it in itens if _candidato(it)]
            print(f"[{data} {sec}] {len(itens)} itens, {len(cand)} baixados")
            for it in cand:
                link = "https://www.in.gov.br/en/web/dou/-/" + (it.get("urlTitle") or "")
                if link in vistos:
                    continue
                vistos.add(link)
                achados.extend(extrai(it, _texto_ato(link)))
    return achados


# ---------------------------------------------------------------------------
# Planilha
# ---------------------------------------------------------------------------

def _chave(a: dict) -> tuple:
    return (a["Link"], _norm(a["Cargo"]), _norm(a["Movimento"]), _norm(a["Nome"]))


def _aba(sh):
    import gspread
    try:
        return sh.worksheet(ABA)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(ABA, rows=2, cols=len(COLS))
        ws.update(range_name="A1", values=[COLS])
        _formata(sh, ws)
        return ws


def _formata(sh, ws):
    marinho = {"red": 0x19 / 255, "green": 0x2D / 255, "blue": 0x4E / 255}
    gelo = {"red": 0xF4 / 255, "green": 0xF3 / 255, "blue": 0xEF / 255}
    larguras = [90, 60, 100, 220, 300, 230, 520, 260, 120]
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


def grava(achados: list[dict]) -> tuple[list[dict], str]:
    import gspread
    from google.oauth2.service_account import Credentials

    if not SHEET_ID:
        raise RuntimeError("Secret PLANILHA_TRANSICAO não definido.")
    info = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    if "\\n" in info.get("private_key", ""):
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    sh = gspread.authorize(creds).open_by_key(SHEET_ID)
    ws = _aba(sh)
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid={ws.id}"

    vals = ws.get_all_values()
    idx = {c: i for i, c in enumerate(vals[0])} if vals else {}
    existentes = set()
    for r in vals[1:]:
        g = lambda c: r[idx[c]] if c in idx and idx[c] < len(r) else ""
        existentes.add((g("Link"), _norm(g("Cargo")), _norm(g("Movimento")), _norm(g("Nome"))))

    novos = []
    for a in achados:
        k = _chave(a)
        if k in existentes:
            continue
        existentes.add(k)
        novos.append(a)
    if not novos:
        print("nada novo na planilha.")
        return [], url

    ws.insert_rows([[a[c] for c in COLS] for a in novos], row=2, value_input_option="RAW")
    print(f"+{len(novos)} linhas em {ABA}.")
    return novos, url


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def _html(novos: list[dict], url: str) -> str:
    import email_layout as L

    ordem = [MEC, ESP, MDS, SAU, MUL, MTE, CC]
    corpo = ""
    for orgao in ordem:
        its = [a for a in novos if a["Órgão"] == orgao]
        if not its:
            continue
        corpo += L.secao(orgao, len(its))
        for a in its:
            titulo = f'{a["Movimento"]}: {a["Nome"]}' if a["Nome"] else a["Movimento"]
            corpo += L.item(titulo, href=a["Link"], rotulo=a["Cargo"],
                            meta=f'{a["Data"]} · {a["Seção"]} · {a["Ato"]}',
                            texto=a["Trecho"][:400] + ("…" if len(a["Trecho"]) > 400 else ""))
    orgaos = len({a["Órgão"] for a in novos})
    return L.pagina(
        "DOU · Transição", "Cargos acompanhados no DOU",
        datetime.now(TZ_BR).strftime("%d/%m/%Y"), corpo,
        nums=[(len(novos), "atos novos" if len(novos) > 1 else "ato novo"),
              (orgaos, "órgãos" if orgaos > 1 else "órgão")],
        previa=" · ".join(f'{a["Movimento"]} {a["Cargo"]}' for a in novos[:3]),
        botoes=[("Abrir a planilha", url)],
    )


def envia(novos: list[dict], url: str) -> None:
    from dou_unificado import _pick_recipients, _send_email

    dest = _pick_recipients("DESTINATARIOS_TRANSICAO", env_fallback="DESTINATARIOS_TRANSICAO")
    if not dest:
        print("Secret DESTINATARIOS_TRANSICAO vazio; sem email.")
        return
    n = len(novos)
    _send_email(f"DOU | Transição: {n} {'atos' if n > 1 else 'ato'}", _html(novos, url), dest)


def _datas() -> list[str]:
    fixa = os.getenv("DOU_DATE", "").strip()
    if fixa:
        return [d.strip() for d in fixa.split(",") if d.strip()]
    hoje = datetime.now(TZ_BR).date()
    return [(hoje - timedelta(days=1)).strftime("%d-%m-%Y"), hoje.strftime("%d-%m-%Y")]


if __name__ == "__main__":
    datas = _datas()
    achados = varre(datas)
    print(f"{len(achados)} achados em {', '.join(datas)}")
    if "--seco" in sys.argv:
        for a in achados:
            print(json.dumps({k: v for k, v in a.items() if k != "Trecho"}, ensure_ascii=False))
            print("   ", a["Trecho"][:300])
        sys.exit(0)
    novos, url = grava(achados)
    if novos:
        envia(novos, url)
