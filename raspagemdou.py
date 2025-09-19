# -*- coding: utf-8 -*-
"""
DOU – raspagem diária com duas saídas:
1) Planilha geral (aba "Página1"):
   Data | Palavra-chave | Portaria | Link | Resumo | Conteúdo
2) Planilha por cliente (uma aba por sigla):
   Data | Cliente | Palavra-chave | Portaria | Link | Resumo | Conteúdo

Observação:
- A coluna "Conteúdo" SEMPRE é preenchida automaticamente com o texto da página do DOU.
- Limite padrão de caracteres = 3000 (mude com env DOU_CONTEUDO_MAX).
- Inclui bloqueio de menções ao Conselho Regional/Federal de Educação Física (CREF/CONFEF).
"""

import os, re, json, unicodedata, requests, gspread
from bs4 import BeautifulSoup
from datetime import datetime
from google.oauth2.service_account import Credentials

# ======= E-mail (Brevo) =======
from brevo_python import ApiClient, Configuration
from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
from brevo_python.models.send_smtp_email import SendSmtpEmail
from brevo_python.rest import ApiException

# ============================================================
# Normalização + busca whole-word
# ============================================================
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

# ============================================================
# BLOQUEIO DE MENÇÕES (CREF/CONFEF)
# ============================================================
EXCLUDE_PATTERNS = [
    _wholeword_pattern("Conselho Regional de Educação Física"),
    _wholeword_pattern("Conselho Federal de Educação Física"),
    _wholeword_pattern("Conselho Regional de Educacao Fisica"),
    _wholeword_pattern("Conselho Federal de Educacao Fisica"),
    re.compile(r"\bCREF\b", re.I),
    re.compile(r"\bCONFEF\b", re.I),
]

def _is_blocked(text: str) -> bool:
    if not text:
        return False
    nt = _normalize_ws(text)
    for pat in EXCLUDE_PATTERNS:
        if pat and pat.search(nt):
            return True
    return False

# ============================================================
# Coleta do conteúdo completo da página do DOU
# ============================================================
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

        sels = [
            "div.single-content", "div.article-content", "article",
            "div#content-core", "div#content", "section#content"
        ]
        textos = []
        for sel in sels:
            el = soup.select_one(sel)
            if not el:
                continue
            ps = [p.get_text(" ", strip=True) for p in el.find_all(["p", "li"]) if p.get_text(strip=True)]
            if len(ps) >= 2:
                textos.append("\n\n".join(ps))
            else:
                textos.append(el.get_text(" ", strip=True))

        txt = max(textos, key=len) if textos else soup.get_text(" ", strip=True)
        txt = re.sub(r"[ \t]+", " ", txt).strip()
        if CONTEUDO_MAX and len(txt) > CONTEUDO_MAX:
            txt = txt[:CONTEUDO_MAX] + "…"
        _CONTENT_CACHE[url] = txt
        return txt
    except Exception:
        return ""

# ============================================================
# Raspagem do DOU (capa do dia)
# ============================================================
def raspa_dou(data=None):
    if data is None:
        data = datetime.now().strftime('%d-%m-%Y')
    print(f'Raspando as notícias do dia {data}...')
    try:
        url = f'http://www.in.gov.br/leiturajornal?data={data}'
        page = requests.get(url, timeout=40, headers=_HDR)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        params = soup.find("script", {"id": "params"})
        if params:
            print('Notícias raspadas.')
            return json.loads(params.text)
        print("Elemento <script id='params'> não encontrado.")
        return None
    except requests.RequestException as e:
        print(f"Erro ao fazer a requisição: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
        return None

# ============================================================
# Palavras gerais (planilha geral)
# ============================================================
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
    # Alfabetização
    'Alfabetização','Alfabetização na Idade Certa','Criança Alfabetizada','Meta de Alfabetização',
    'Plano Nacional de Alfabetização','Programa Criança Alfabetizada','Idade Certa para Alfabetização',
    'Alfabetização de Crianças','Alfabetização Inicial','Alfabetização Plena',
    'Alfabetização em Língua Portuguesa','Analfabetismo','Erradicação do Analfabetismo',
    'Programa Nacional de Alfabetização na Idade Certa','Pacto pela Alfabetização',
    'Política Nacional de Alfabetização','Recomposição das Aprendizagens em Alfabetização',
    'Competências de Alfabetização','Avaliação da Alfabetização','Saeb Alfabetização',
    # Matemática
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
    if conteudo_raspado is None or 'jsonArray' not in conteudo_raspado:
        print('Nenhum conteúdo para analisar (geral).')
        return None

    print('Buscando palavras-chave (geral, whole-word, título+resumo)...')
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
        conteudo_pagina = None

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
                    'content_page': conteudo_pagina
                })
                algum = True

    if not algum:
        print('Nenhum resultado encontrado (geral).')
        return None
    print('Palavras-chave (geral) encontradas.')
    return resultados_por_palavra

# ============================================================
# Cliente → Palavras (whole-word)
# ============================================================
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
Giro de notícias|Temas gerais para o Giro de Notícias e clipping cactus|Governo Lula; Presidente Lula; Governo; Governo Federal; Governo economia; Economia; Governo internacional; Saúde; Medicamento; Vacina; Câncer; Oncologia; Gripe; Diabetes; Obesidade; Alzheimer; Saúde mental; Síndrome respiratória; SUS; Sistema Único de Saúde; Ministério da Saúde; Alexandre Padilha; ANVISA; Primeira Infância; Infância; Criança; Saúde criança; Saúde infantil; cuidado criança; legislação criança; direitos da criança; criança câmara; criança senado; alfabetização; creche; ministério da educação; educação; educação Brasil; escolas; aprendizado; ensino integral; ensino médio; Camilo Santana
Cactus|Saúde|Saúde mental; saúde mental para meninas; saúde mental para juventude; saúde mental para mulheres; Rede de atenção psicossocial; RAPS; CAPS; Centro de Apoio Psicossocial
Vital Strategies|Saúde|Saúde mental; Dados para a saúde; Morte evitável; Doenças crônicas não transmissíveis; Rotulagem de bebidas alcoólicas; Educação em saúde; Bebidas alcoólicas; Imposto seletivo; Rotulagem de alimentos; Alimentos ultraprocessados; Publicidade infantil; Publicidade de alimentos ultraprocessados; Tributação de bebidas alcoólicas; Alíquota de bebidas alcoólicas; Cigarro eletrônico; Controle de tabaco; Violência doméstica; Exposição a fatores de risco; Departamento de Saúde Mental; Hipertensão arterial; Saúde digital; Violência contra crianças; Violência contra mulheres; Feminicídio; COP 30
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

CLIENT_PATTERNS = []
for cli, kws in CLIENT_KEYWORDS.items():
    for kw in kws:
        pat = _wholeword_pattern(kw)
        if pat:
            CLIENT_PATTERNS.append((pat, cli, kw))

def procura_termos_clientes(conteudo_raspado):
    if conteudo_raspado is None or 'jsonArray' not in conteudo_raspado:
        print('Nenhum conteúdo para analisar (clientes).')
        return {}

    print('Buscando palavras-chave por cliente (whole-word, título+resumo)...')
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
                por_cliente[cliente].append([data_pub, cliente, kw, titulo, link, resumo, conteudo_pagina])
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

COLS_GERAL   = ["Data","Palavra-chave","Portaria","Link","Resumo","Conteúdo"]
COLS_CLIENTE = ["Data","Cliente","Palavra-chave","Portaria","Link","Resumo","Conteúdo"]

def _ensure_header(ws, header):
    first = ws.row_values(1)
    if first != header:
        ws.resize(rows=max(2, ws.row_count), cols=len(header))
        ws.update('1:1', [header])

def salva_na_base(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para salvar (geral).')
        return
    print('Salvando (geral) na planilha...')

    gc = _gs_client_from_env()
    planilha_id = os.getenv('PLANILHA')
    if not planilha_id:
        raise RuntimeError("Env PLANILHA não definido.")

    sh = gc.open_by_key(planilha_id)
    try:
        ws = sh.worksheet('Página1')
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title='Página1', rows="2000", cols=str(len(COLS_GERAL)))
    _ensure_header(ws, COLS_GERAL)

    rows_to_append = []
    for palavra, lista in (palavras_raspadas or {}).items():
        for item in lista:
            rows_to_append.append([
                item.get('date',''),
                palavra,
                item.get('title',''),
                item.get('href',''),
                item.get('abstract',''),
                item.get('content_page','')
            ])

    if rows_to_append:
        ws.insert_rows(rows_to_append, row=2, value_input_option='USER_ENTERED')
        print(f"{len(rows_to_append)} linhas adicionadas (geral).")
    else:
        print('Nenhum dado válido para salvar (geral).')

def _append_dedupe_por_cliente(sh, sheet_name: str, rows: list[list[str]]):
    if not rows:
        print(f"[{sheet_name}] sem linhas para anexar.")
        return
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
        return

    ws.insert_rows(new, row=2, value_input_option="USER_ENTERED")
    print(f"[{sheet_name}] +{len(new)} linhas.")

def salva_por_cliente(por_cliente: dict):
    plan_id = os.getenv("PLANILHA_CLIENTES")
    if not plan_id:
        print("PLANILHA_CLIENTES não definido; pulando saída por cliente.")
        return
    gc = _gs_client_from_env()
    sh = gc.open_by_key(plan_id)

    for cli in CLIENT_KEYWORDS.keys():
        try:
            ws = sh.worksheet(cli)
            _ensure_header(ws, COLS_CLIENTE)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=cli, rows="2", cols=len(COLS_CLIENTE))
            _ensure_header(ws, COLS_CLIENTE)

    for cli, rows in (por_cliente or {}).items():
        _append_dedupe_por_cliente(sh, cli, rows)

# ============================================================
# E-mails
# ============================================================
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

def _brevo_client():
    api_key = os.getenv('BREVO_API_KEY')
    if not api_key:
        return None
    cfg = Configuration()
    cfg.api_key['api-key'] = api_key
    return TransactionalEmailsApi(ApiClient(configuration=cfg))

def envia_email_geral(palavras_raspadas):
    if not palavras_raspadas:
        print('Sem palavras encontradas para enviar (geral).')
        return
    sender_email = os.getenv('EMAIL'); raw_dest = os.getenv('DESTINATARIOS', '')
    planilha_id  = os.getenv("PLANILHA")
    api = _brevo_client()
    if not (api and sender_email and raw_dest and planilha_id):
        print("Dados incompletos; pulando envio (geral).")
        return
    destinatarios = _sanitize_emails(raw_dest)
    data = datetime.now().strftime('%d-%m-%Y')
    titulo = f'Resultados do Diário Oficial — {data}'
    planilha_url = f'https://docs.google.com/spreadsheets/d/{planilha_id}/edit?gid=0'
    parts = [
        "<html><body>",
        "<h1>Consulta ao Diário Oficial da União</h1>",
        f"<p>As matérias já estão na <a href='{planilha_url}' target='_blank'>planilha</a>.</p>"
    ]
    for palavra, lista in (palavras_raspadas or {}).items():
        if lista:
            parts.append(f"<h2>{palavra}</h2><ul>")
            for r in lista:
                link = r.get('href', '#'); title = r.get('title', '(sem título)')
                parts.append(f"<li><a href='{link}'>{title}</a></li>")
            parts.append("</ul>")
    parts.append("</body></html>")
    html = "".join(parts)
    for dest in destinatarios:
        try:
            api.send_transac_email(SendSmtpEmail(to=[{"email": dest}],
                                                sender={"email": sender_email},
                                                subject=titulo, html_content=html))
            print(f"✅ E-mail (geral) enviado para {dest}")
        except (ApiException, Exception) as e:
            print(f"❌ Falha ao enviar (geral) para {dest}: {e}")

def envia_email_clientes(por_cliente: dict):
    """E-mail com resultados organizados por cliente + SUMÁRIO por cliente."""
    if not por_cliente:
        print("Sem resultados para enviar (clientes).")
        return

    from collections import Counter

    sender_email = os.getenv("EMAIL")
    raw_dest = os.getenv("DESTINATARIOS", "")
    planilha_id_clientes = os.getenv("PLANILHA_CLIENTES")
    api = _brevo_client()
    if not (api and sender_email and raw_dest and planilha_id_clientes):
        print("Dados de e-mail incompletos; pulando envio (clientes).")
        return

    def _slug(s: str) -> str:
        return re.sub(r'[^a-z0-9]+', '-', _normalize_ws(s)).strip('-') or "secao"

    destinatarios = _sanitize_emails(raw_dest)
    data = datetime.now().strftime("%d-%m-%Y")
    titulo = f"Resultados do Diário Oficial (Clientes) – {data}"
    planilha_url = f"https://docs.google.com/spreadsheets/d/{planilha_id_clientes}/edit?gid=0"

    # ---------- SUMÁRIO por cliente ----------
    # Monta linhas com: Cliente | Total | Top KWs
    sum_rows = []
    for cliente, rows in (por_cliente or {}).items():
        if not rows:
            continue
        kw_counts = Counter(r[2] for r in rows)
        top_kw = ", ".join(f"{k} ({n})" for k, n in kw_counts.most_common(3))
        sum_rows.append((cliente, len(rows), top_kw))

    # Ordena por maior número de ocorrências
    sum_rows.sort(key=lambda t: t[1], reverse=True)

    parts = [
        "<html><body>",
        "<h1>Consulta ao Diário Oficial da União (Clientes)</h1>",
        f"<p>Os resultados já estão na <a href='{planilha_url}' target='_blank'>planilha de clientes</a>.</p>",
    ]

    if sum_rows:
        parts.append("<h2>Sumário por cliente</h2>")
        parts.append("<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse'>")
        parts.append("<tr><th>Cliente</th><th>Total</th><th>Top palavras-chave</th></tr>")
        for cliente, total, top_kw in sum_rows:
            anchor = _slug(cliente)
            parts.append(
                f"<tr>"
                f"<td><a href='#{anchor}'>{cliente}</a></td>"
                f"<td>{total}</td>"
                f"<td>{top_kw or '-'}</td>"
                f"</tr>"
            )
        parts.append("</table>")

    # ---------- Detalhes por cliente ----------
    for cliente, rows in (por_cliente or {}).items():
        if not rows:
            continue
        anchor = _slug(cliente)
        parts.append(f"<h2 id='{anchor}'>{cliente}</h2>")

        # agrupa por palavra-chave
        agrupados = {}
        for r in rows:
            kw = r[2]
            agrupados.setdefault(kw, []).append(r)

        # ordena grupos por quantidade desc
        for kw, lista in sorted(agrupados.items(), key=lambda kv: len(kv[1]), reverse=True):
            parts.append(f"<h3>Palavra-chave: {kw} — {len(lista)} ocorrência(s)</h3><ul>")
            for item in lista:
                # item = [data_pub, cliente, kw, titulo, link, resumo, conteudo]
                link = item[4]
                titulo_item = item[3] or "(sem título)"
                resumo = (item[5] or "").strip()
                parts.append(
                    f"<li><a href='{link}' target='_blank'>{titulo_item}</a>"
                    + (f"<br><em>Resumo:</em> {resumo}" if resumo else "")
                    + "</li>"
                )
            parts.append("</ul>")

    parts.append("</body></html>")
    html = "".join(parts)

    for dest in destinatarios:
        try:
            api.send_transac_email(
                SendSmtpEmail(
                    to=[{"email": dest}],
                    sender={"email": sender_email},
                    subject=titulo,
                    html_content=html,
                )
            )
            print(f"✅ E-mail (clientes) enviado para {dest}")
        except (ApiException, Exception) as e:
            print(f"❌ Falha ao enviar (clientes) para {dest}: {e}")

# ============================================================
# Execução principal
# ============================================================
if __name__ == "__main__":
    conteudo = raspa_dou()

    # 1) Planilha geral
    geral = procura_termos(conteudo)
    salva_na_base(geral)
    envia_email_geral(geral)

    # 2) Planilha por cliente (uma aba por sigla)
    por_cliente = procura_termos_clientes(conteudo)
    salva_por_cliente(por_cliente)
    envia_email_clientes(por_cliente)

