import os, re, json, unicodedata, requests, gspread
from bs4 import BeautifulSoup
from datetime import datetime
from google.oauth2.service_account import Credentials

# E-mail (Brevo) – mesmo formato do diário, com assunto [DOU EXTRA]
from brevo_python import ApiClient, Configuration
from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
from brevo_python.models.send_smtp_email import SendSmtpEmail
from brevo_python.rest import ApiException

# Normalização + busca whole-word
def _normalize(s: str) -> str:
    if s is None:
        return ""
    t = unicodedata.normalize("NFD", str(s))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return t.lower()

def _normalize_ws(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', ' ', _normalize(s)).strip()

def _wholeword_pattern(phrase: str):
    toks = [t for t in _normalize_ws(phrase).split() if t]
    if not toks:
        return None
    return re.compile(r'\b' + r'\s+'.join(map(re.escape, toks)) + r'\b')

# BLOQUEIO DE MENÇÕES (CREF/CONFEF) + CNE/CES
EXCLUDE_PATTERNS = [
    _wholeword_pattern("Conselho Regional de Educação Física"),
    _wholeword_pattern("Conselho Federal de Educação Física"),
    _wholeword_pattern("Conselho Regional de Educacao Fisica"),
    _wholeword_pattern("Conselho Federal de Educacao Fisica"),
    re.compile(r"\bCREF\b", re.I),
    re.compile(r"\bCONFEF\b", re.I),
]

_CNE_PATTERNS = [
    _wholeword_pattern("Conselho Nacional de Educação"),
    _wholeword_pattern("Conselho Nacional de Educacao"),
    re.compile(r"\bCNE\b", re.I),
]

_CES_PATTERNS = [
    _wholeword_pattern("Câmara de Educação Superior"),
    _wholeword_pattern("Camara de Educacao Superior"),
    re.compile(r"\bCES\b", re.I),
]

def _has_any(text_norm: str, patterns: list[re.Pattern]) -> bool:
    return any(p and p.search(text_norm) for p in patterns)

def _is_blocked(text: str) -> bool:
    """True se o texto contiver menções bloqueadas."""
    if not text:
        return False
    nt = _normalize_ws(text)

    for pat in EXCLUDE_PATTERNS:
        if pat and pat.search(nt):
            return True

    if _has_any(nt, _CNE_PATTERNS) and _has_any(nt, _CES_PATTERNS):
        return True

    return False

# Coleta do conteúdo completo da página do DOU (sempre ligada)
CONTEUDO_MAX = int(os.getenv("DOU_CONTEUDO_MAX", "49500"))  # limite de caracteres
_CONTENT_CACHE: dict[str, str] = {}

_HDR = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126 Safari/537.36"
}

def _baixar_conteudo_pagina(url: str) -> str:
    if not url:
        return ""
    if url in _CONTENT_CACHE:
        return _CONTENT_CACHE[url]
    try:
        r = requests.get(url, timeout=40, headers=_HDR)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        for t in soup(["script", "style", "noscript"]):
            t.decompose()

        bloco = soup.select_one("article#materia div.texto-dou") or soup.select_one("div.texto-dou")
        if bloco:
            ps = []
            for p in bloco.find_all(["p", "li"]):
                cls = set(p.get("class") or [])
                if {"dou-paragraph", "identifica", "ementa"} & cls or p.name == "li":
                    txt = p.get_text(" ", strip=True)
                    if txt:
                        ps.append(txt)
            if ps:
                txt = "\n\n".join(ps)
                txt = re.sub(r"[ \t]+", " ", txt).strip()
                if CONTEUDO_MAX and len(txt) > CONTEUDO_MAX:
                    txt = txt[:CONTEUDO_MAX] + "…"
                _CONTENT_CACHE[url] = txt
                return txt

        sels = ["div.single-content", "div.article-content", "article",
                "div#content-core", "div#content", "section#content"]
        textos = []
        for sel in sels:
            el = soup.select_one(sel)
            if not el:
                continue
            ps = [p.get_text(" ", strip=True) for p in el.find_all(["p", "li"]) if p.get_text(strip=True)]
            textos.append("\n\n".join(ps) if len(ps) >= 2 else el.get_text(" ", strip=True))

        txt = max(textos, key=len) if textos else soup.get_text(" ", strip=True)
        txt = re.sub(r"[ \t]+", " ", txt).strip()
        if CONTEUDO_MAX and len(txt) > CONTEUDO_MAX:
            txt = txt[:CONTEUDO_MAX] + "…"
        _CONTENT_CACHE[url] = txt
        return txt
    except Exception:
        return ""

# Raspagem da(s) Edição(ões) Extra (DO1E/DO2E/DO3E…)
def raspa_dou_extra(data=None, secoes=None):
    """
    Retorna a mesma estrutura (dict com jsonArray) combinando as seções extras pedidas.
    Por padrão tenta DO1E, DO2E, DO3E.
    """
    if data is None:
        data = datetime.now().strftime('%d-%m-%Y')
    if secoes is None:
        secoes = [s.strip() for s in (os.getenv("DOU_EXTRA_SECOES") or "DO1E,DO2E,DO3E").split(",") if s.strip()]
    print(f'Raspando edição EXTRA do dia {data} nas seções: {", ".join(secoes)}…')

    combined = {"jsonArray": []}
    for sec in secoes:
        try:
            url = f'http://www.in.gov.br/leiturajornal?data={data}&secao={sec}'
            page = requests.get(url, timeout=40, headers=_HDR)
            if page.status_code != 200:
                continue
            soup = BeautifulSoup(page.text, 'html.parser')
            params = soup.find("script", {"id": "params"})
            if not params:
                continue
            j = json.loads(params.text)
            arr = j.get("jsonArray", [])
            if arr:
                combined["jsonArray"].extend(arr)
        except Exception:
            continue

    if combined["jsonArray"]:
        print(f'OK, coletadas {len(combined["jsonArray"])} entradas (extra).')
        return combined
    print("Nenhum item encontrado na(s) seção(ões) extra.")
    return None

# Palavras gerais (planilha geral)
PALAVRAS_GERAIS = [
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
    'Hipertensão Arterial','Alimentação Escolar','PNAE','Agora Tem Especialistas',
    # Alfabetização e matemática
    'Alfabetização','Alfabetização na Idade Certa','Criança Alfabetizada','Meta de Alfabetização',
    'Plano Nacional de Alfabetização','Programa Criança Alfabetizada','Idade Certa para Alfabetização',
    'Alfabetização de Crianças','Alfabetização Inicial','Alfabetização Plena',
    'Alfabetização em Língua Portuguesa','Analfabetismo','Erradicação do Analfabetismo',
    'Programa Nacional de Alfabetização na Idade Certa','Pacto pela Alfabetização',
    'Política Nacional de Alfabetização','Recomposição das Aprendizagens em Alfabetização',
    'Competências de Alfabetização','Avaliação da Alfabetização','Saeb Alfabetização',
    'Alfabetização Matemática','Analfabetismo Matemático','Aprendizagem em Matemática',
    'Recomposição das Aprendizagens em Matemática','Recomposição de Aprendizagem',
    'Competências Matemáticas','Proficiência em Matemática',
    'Avaliação Diagnóstica de Matemática','Avaliação Formativa de Matemática',
    'Política Nacional de Matemática','Saeb Matemática','Ideb Matemática','BNCC Matemática',
    'Matemática no Ensino Fundamental','Matemática no Ensino Médio',
    'Anos Iniciais de Matemática','Anos Finais de Matemática','OBMEP',
    'Olimpíada Brasileira de Matemática das Escolas Públicas','Olimpíada de Matemática','PNLD Matemática'
]
_PATTERNS_GERAL = [(kw, _wholeword_pattern(kw)) for kw in PALAVRAS_GERAIS]

def procura_termos(conteudo_raspado):
    """
    Planilha geral — Data | Palavra-chave | Portaria | Link | Resumo | Conteúdo
    (com bloqueio CREF/CONFEF e CNE+CES)
    """
    if conteudo_raspado is None or 'jsonArray' not in conteudo_raspado:
        print('Nenhum conteúdo para analisar (geral extra).')
        return None

    print('Buscando palavras-chave (geral, whole-word, título+resumo)…')
    URL_BASE = 'https://www.in.gov.br/en/web/dou/-/'
    resultados_por_palavra = {palavra: [] for palavra in PALAVRAS_GERAIS}
    algum = False

    for resultado in conteudo_raspado['jsonArray']:
        titulo   = resultado.get('title', 'Título não disponível')
        resumo   = resultado.get('content', '')
        link     = URL_BASE + resultado.get('urlTitle', '')
        data_pub = (resultado.get('pubDate', '') or '')[:10]

        if _is_blocked(titulo + " " + resumo):
            continue

        texto_norm = _normalize_ws(titulo + " " + resumo)
        conteudo_pagina = None  # baixa apenas se houver match

        for palavra, patt in _PATTERNS_GERAL:
            if patt and patt.search(texto_norm):
                if conteudo_pagina is None:
                    conteudo_pagina = _baixar_conteudo_pagina(link)
                if _is_blocked(conteudo_pagina):
                    continue
                resultados_por_palavra[palavra].append({
                    'date': data_pub,
                    'title': titulo,
                    'href': link,
                    'abstract': resumo,
                    'content_page': conteudo_pagina or ""
                })
                algum = True

    if not algum:
        print('Nenhum resultado encontrado (geral extra).')
        return None
    print('Palavras-chave (geral extra) encontradas.')
    return resultados_por_palavra

# Mapa: Cliente → Tema → Keywords (whole-word)
CLIENT_THEME_DATA = """
IAS|Educação|Matemática; Alfabetização; Alfabetização Matemática; Recomposição de aprendizagem; Plano Nacional de Educação
ISG|Educação|Tempo Integral; Ensino em tempo integral; Ensino Profissional e Tecnológico; Fundeb; PROPAG; Educação em tempo integral; Escola em tempo integral; Plano Nacional de Educação; Programa escola em tempo integral; Programa Pé-de-meia; PNEERQ; INEP; FNDE; Conselho Nacional de Educação; PDDE; Programa de Fomento às Escolas de Ensino Médio em Tempo Integral; Celular nas escolas; Juros da Educação
IU|Educação|Gestão Educacional; Diretores escolares; Magistério; Professores ensino médio; Sindicatos de professores; Ensino Médio; Fundeb; Adaptações de Escolas; Educação Ambiental; Plano Nacional de Educação; PDDE; Programa Pé de Meia; INEP; FNDE; Conselho Nacional de Educação; VAAT; VAAR; Secretaria Estadual de Educação; Celular nas escolas; EAD; Juro da educação; Recomposição de Aprendizagem
Reúna|Educação|Matemática; Alfabetização; Alfabetização Matemática; Recomposição de aprendizagem; Plano Nacional de Educação; Emendas parlamentares educação
REMS|Esportes|Esporte amador; Esporte para toda a vida; Esporte e desenvolvimento social; Financiamento do esporte; Lei de Incentivo ao Esporte; Plano Nacional de Esporte; Conselho Nacional de Esporte; Emendas parlamentares esporte
FMCSV|Primeira infância|Criança; Infância; infanto-juvenil; educação básica; PNE; FNDE; Fundeb; VAAR; VAAT; educação infantil; maternidade; paternidade; alfabetização; creche; pré-escola; parentalidade; materno-infantil; infraestrutura escolar; política nacional de cuidados; Plano Nacional de Educação; Bolsa Família; Conanda; visitação domiciliar; Homeschooling; Política Nacional Integrada da Primeira Infância
IEPS|Saúde|SUS; Sistema Único de Saúde; fortalecimento; Universalidade; Equidade em saúde; populações vulneráveis; desigualdades sociais; Organização do SUS; gestão pública; políticas públicas em saúde; Governança do SUS; regionalização; descentralização; Regionalização em saúde; Políticas públicas em saúde; População negra em saúde; Saúde indígena; Povos originários; Saúde da pessoa idosa; envelhecimento ativo; Atenção Primária; Saúde da criança; Saúde do adolescente; Saúde da mulher; Saúde do homem; Saúde da pessoa com deficiência; Saúde da população LGBTQIA+; Financiamento da saúde; atenção primária; tripartite; orçamento; Emendas e orçamento da saúde; Ministério da Saúde; Trabalhadores de saúde; Força de trabalho em saúde; Recursos humanos em saúde; Formação profissional de saúde; Cuidados primários em saúde; Emergências climáticas e ambientais em saúde; mudanças climáticas; adaptação climática; saúde ambiental; políticas climáticas; Vigilância em saúde; epidemiológica; Emergência em saúde; estado de emergência; Saúde suplementar; complementar; privada; planos de saúde; seguros; seguradoras; planos populares; Anvisa; gestão; governança; ANS; Sandbox regulatório; Cartões e administradoras de benefícios em saúde; Economia solidária em saúde mental; Pessoa em situação de rua; saúde mental; Fiscalização de comunidades terapêuticas; Rede de atenção psicossocial; RAPS; unidades de acolhimento; assistência multiprofissional; centros de convivência; Cannabis; canabidiol; tratamento terapêutico; Desinstitucionalização; manicômios; hospitais de custódia; Saúde mental na infância; adolescência; escolas; comunidades escolares; protagonismo juvenil; Dependência química; vícios; ludopatia; Treinamento em saúde mental; capacitação em saúde mental; Intervenções terapêuticas em saúde mental; Internet e redes sociais na saúde mental; Violência psicológica; Surto psicótico
Manual|Saúde|Ozempic; Wegovy; Mounjaro; Telemedicina; Telessaúde; CBD; Cannabis Medicinal; CFM; Conselho Federal de Medicina; Farmácia Magistral; Medicamentos Manipulados; Minoxidil; Emagrecedores; Retenção de receita de medicamentos
Mevo|Saúde|Prontuário eletrônico; dispensação eletrônica; telessaúde; assinatura digital; certificado digital; controle sanitário; prescrição por enfermeiros; doenças crônicas; autonomia da ANPD; Acesso e uso de dados; responsabilização de plataformas digitais; regulamentação de marketplaces; segurança cibernética; inteligência artificial; digitalização do SUS; venda de medicamentos; distribuição de medicamentos; Bula digital; Atesta CFM; SNGPC; Farmacêutico Remoto; Medicamentos Isentos de Prescrição; MIPs; RNDS; Rede Nacional de Dados em Saúde
Cactus|Saúde|Saúde mental; saúde mental para meninas; saúde mental para juventude; saúde mental para mulheres; Rede de atenção psicossocial; RAPS; CAPS; Centro de Apoio Psicossocial
Vital Strategies|Saúde|Saúde mental; Dados para a saúde; Morte evitável; Doenças crônicas não transmissíveis; Rotulagem de bebidas alcoólicas; Educação em saúde; Bebidas alcoólicas; Imposto seletivo; Rotulagem de alimentos; Alimentos ultraprocessados; Publicidade infantil; Publicidade de alimentos ultraprocessados; Tributação de bebidas alcoólicas; Alíquota de bebidas alcoólicas; Cigarro eletrônico; Controle de tabaco; Violência doméstica; Exposição a fatores de risco; Departamento de Saúde Mental; Hipertensão arterial; Saúde digital; Violência contra crianças; Violência contra mulheres; Feminicídio; COP 30
NPNM|Direitos reprodutivos|aborto; nascituro; gestação acima de 22 semanas; interrupção legal da gestação; interrupção da gestação; Resolução 258 Conanda; vida por nascer; vida desde a concepção; criança por nascer; infanticídio; feticídio; assistolia fetal; medicamento abortivo; misoprostol; citotec; cytotec; mifepristona; ventre; assassinato de bebês; luto parental; síndrome pós aborto
""".strip()

def _parse_client_keywords(text: str):
    out = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        cliente, tema, kws = [x.strip() for x in line.split("|", 2)]
        out.setdefault(cliente, [])
        for kw in [k.strip() for k in kws.split(";") if k.strip()]:
            if kw not in out[cliente]:
                out[cliente].append(kw)
    return out

CLIENT_KEYWORDS = _parse_client_keywords(CLIENT_THEME_DATA)

# pré-compila padrões por cliente
CLIENT_PATTERNS = []  # (regex, cliente, kw_original)
for cli, kws in CLIENT_KEYWORDS.items():
    for kw in kws:
        pat = _wholeword_pattern(kw)
        if pat:
            CLIENT_PATTERNS.append((pat, cli, kw))

def procura_termos_clientes(conteudo_raspado):
    """
    Retorna dict cliente -> [rows] onde cada row é:
    [Data, Cliente, Palavra-chave, Portaria, Link, Resumo, Conteúdo]
    (com bloqueio CREF/CONFEF e CNE+CES)
    """
    if conteudo_raspado is None or 'jsonArray' not in conteudo_raspado:
        print('Nenhum conteúdo para analisar (clientes extra).')
        return {}

    print('Buscando palavras-chave por cliente (whole-word, título+resumo)…')
    URL_BASE = 'https://www.in.gov.br/en/web/dou/-/'
    por_cliente = {c: [] for c in CLIENT_KEYWORDS.keys()}

    for r in conteudo_raspado['jsonArray']:
        titulo   = r.get('title', 'Título não disponível')
        resumo   = r.get('content', '')
        link     = URL_BASE + r.get('urlTitle', '')
        data_pub = (r.get('pubDate', '') or '')[:10]

        if _is_blocked(titulo + " " + resumo):
            continue

        texto_norm = _normalize_ws(titulo + " " + resumo)
        conteudo_pagina = None
        for pat, cliente, kw in CLIENT_PATTERNS:
            if pat.search(texto_norm):
                if conteudo_pagina is None:
                    conteudo_pagina = _baixar_conteudo_pagina(link)
                if _is_blocked(conteudo_pagina):
                    continue
                por_cliente[cliente].append([data_pub, cliente, kw, titulo, link, resumo, conteudo_pagina or ""])
    return por_cliente

# ============================================================
# Google Sheets helpers
# ============================================================
def _gs_client_from_env():
    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not raw:
        jf = "credentials.json"
        if os.path.exists(jf):
            scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_file(jf, scopes=scopes)
            return gspread.authorize(creds)
        raise RuntimeError("Secret GOOGLE_APPLICATION_CREDENTIALS_JSON não encontrado.")
    info = json.loads(raw)
    if "private_key" in info and "\\n" in info["private_key"]:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

# Colunas FIXAS (Conteúdo é sempre a última)
COLS_GERAL   = ["Data","Palavra-chave","Portaria","Link","Resumo","Conteúdo"]
COLS_CLIENTE = ["Data","Cliente","Palavra-chave","Portaria","Link","Resumo","Conteúdo","Alinhamento","Justificativa"]

def _ensure_header(ws, header):
    first = ws.row_values(1)
    if first != header:
        ws.resize(rows=max(2, ws.row_count), cols=len(header))
        ws.update('1:1', [header])

# Geral
def salva_na_base(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para salvar (geral extra).')
        return 0
    print('Salvando (geral extra) na planilha…')

    gc = _gs_client_from_env()
    planilha_id = os.getenv('PLANILHA')
    if not planilha_id:
        raise RuntimeError("Env PLANILHA não definido (use apenas a key entre /d/ e /edit).")

    sh = gc.open_by_key(planilha_id)
    try:
        ws = sh.worksheet('Página1')
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title='Página1', rows="2000", cols=str(len(COLS_GERAL)))
    _ensure_header(ws, COLS_GERAL)

    rows_to_append = []
    for palavra, lista in (palavras_raspadas or {}).items():
        for item in lista:
            row = [
                item.get('date',''),
                palavra,
                item.get('title',''),
                item.get('href',''),
                item.get('abstract',''),
                item.get('content_page','')
            ]
            rows_to_append.append(row)

    if rows_to_append:
        ws.insert_rows(rows_to_append, row=2, value_input_option='USER_ENTERED')
        print(f"{len(rows_to_append)} linhas adicionadas (geral extra).")
    else:
        print('Nenhum dado válido para salvar (geral extra).')
    return len(rows_to_append)

# Por cliente (dedupe por Link+Palavra-chave+Cliente)
def _append_dedupe_por_cliente(sh, sheet_name: str, rows: list[list[str]]):
    if not rows:
        print(f"[{sheet_name}] sem linhas para anexar.")
        return 0
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=str(max(100, len(rows)+10)), cols=len(COLS_CLIENTE))
    _ensure_header(ws, COLS_CLIENTE)

    link_idx    = COLS_CLIENTE.index("Link")
    palavra_idx = COLS_CLIENTE.index("Palavra-chave")
    cliente_idx = COLS_CLIENTE.index("Cliente")

    all_vals = ws.get_all_values()
    existing = set()
    if len(all_vals) > 1:
        for r in all_vals[1:]:
            if len(r) > link_idx:
                existing.add((r[link_idx].strip(),
                              r[palavra_idx].strip() if len(r) > palavra_idx else "",
                              r[cliente_idx].strip() if len(r) > cliente_idx else ""))
    new = [r for r in rows if (r[link_idx].strip(), r[palavra_idx].strip(), r[cliente_idx].strip()) not in existing]
    if not new:
        print(f"[{sheet_name}] nada novo.")
        return 0

    ws.insert_rows(new, row=2, value_input_option="USER_ENTERED")
    print(f"[{sheet_name}] +{len(new)} linhas (extra).")
    return len(new)

def salva_por_cliente(por_cliente: dict):
    plan_id = os.getenv("PLANILHA_CLIENTES")
    if not plan_id:
        print("PLANILHA_CLIENTES não definido; pulando saída por cliente (extra).")
        return 0
    gc = _gs_client_from_env()
    sh = gc.open_by_key(plan_id)

    # Garante existência das abas
    for cli in CLIENT_KEYWORDS.keys():
        try:
            ws = sh.worksheet(cli); _ensure_header(ws, COLS_CLIENTE)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=cli, rows="2", cols=len(COLS_CLIENTE))
            _ensure_header(ws, COLS_CLIENTE)

    total_new = 0
    for cli, rows in (por_cliente or {}).items():
        total_new += _append_dedupe_por_cliente(sh, cli, rows)
    return total_new

# E-mail (Brevo) — só envia se houve ao menos 1 linha nova
EMAIL_RE = re.compile(r'<?("?)([^"\s<>@]+@[^"\s<>@]+\.[^"\s<>@]+)\1>?$')

def _sanitize_emails(raw_list: str):
    if not raw_list:
        return []
    parts = re.split(r'[,\n;]+', raw_list)
    emails, seen = [], set()
    for it in parts:
        s = unicodedata.normalize("NFKC", it)
        s = re.sub(r'[\u200B-\u200D\uFEFF]', '', s).strip().strip("'").strip('"')
        if not s:
            continue
        m = EMAIL_RE.match(s)
        candidate = (m.group(2) if m else s).strip()
        if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", candidate) and candidate.lower() not in seen:
            seen.add(candidate.lower()); emails.append(candidate.lower())
    return emails

def envia_email_brevo_extra(palavras_raspadas, total_clientes, total_geral):
    total = int(total_clientes) + int(total_geral)
    if total <= 0:
        print('Nada novo — e-mail (extra) não será enviado.')
        return

    api_key = os.getenv('BREVO_API_KEY')
    sender_email = os.getenv('EMAIL')
    raw_dest = os.getenv('DESTINATARIOS', '')
    if not (api_key and sender_email and raw_dest):
        print("Dados de e-mail incompletos; pulando envio (extra).")
        return

    destinatarios = _sanitize_emails(raw_dest)
    data = datetime.now().strftime('%d-%m-%Y')
    titulo = f'Resultados do Diário Oficial (EXTRA) — {data}'
    planilha_url = f'https://docs.google.com/spreadsheets/d/{os.getenv("PLANILHA")}/edit?gid=0'

    parts = [
        "<html><body>",
        "<h1>Edição Extra do DOU</h1>",
        f"<p>Foram adicionados <b>{total}</b> itens. Veja a ",
        f'<a href="{planilha_url}" target="_blank">planilha geral</a> e as abas por cliente.</p>'
    ]
    shown = 0
    for palavra, lista in (palavras_raspadas or {}).items():
        if not lista:
            continue
        parts.append(f"<h3>{palavra}</h3><ul>")
        for r in lista[:5]:
            link = r.get('href', '#')
            title = r.get('title', '(sem título)')
            parts.append(f"<li><a href='{link}'>{title}</a></li>")
            shown += 1
        parts.append("</ul>")
        if shown >= 15:
            break
    parts.append("</body></html>")
    html = "".join(parts)

    cfg = Configuration(); cfg.api_key['api-key'] = api_key
    api = TransactionalEmailsApi(ApiClient(configuration=cfg))
    for dest in destinatarios:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}],
                sender={"email": sender_email},
                subject=titulo,
                html_content=html
            ))
            print(f"✅ [DOU EXTRA] E-mail enviado para {dest}")
        except (ApiException, Exception) as e:
            print(f"❌ Falha ao enviar (extra) para {dest}: {e}")

# Execução principal
if __name__ == "__main__":
    conteudo = raspa_dou_extra()

    # 1) Planilha geral
    geral = procura_termos(conteudo)
    qtd_geral = salva_na_base(geral)

    # 2) Planilha por cliente (uma aba por sigla)
    por_cliente = procura_termos_clientes(conteudo)
    qtd_clientes = salva_por_cliente(por_cliente)

    # 3) E-mail só se houve algo novo
    envia_email_brevo_extra(geral, qtd_clientes, qtd_geral)
