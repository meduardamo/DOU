import os, re, json, unicodedata, requests, gspread
from bs4 import BeautifulSoup
from datetime import datetime
from google.oauth2.service_account import Credentials

# E-mail (Brevo)
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


# BLOQUEIO DE MENÇÕES (CREF/CONFEF) + CNE/CES + Educação Superior + Acórdão + Novo PAC + decisões de casos particulares + Registro Especial
EXCLUDE_PATTERNS = [
    _wholeword_pattern("Conselho Regional de Educação Física"),
    _wholeword_pattern("Conselho Federal de Educação Física"),
    _wholeword_pattern("Conselho Regional de Educacao Fisica"),
    _wholeword_pattern("Conselho Federal de Educacao Fisica"),
    re.compile(r"\bCREF\b", re.I),
    re.compile(r"\bCONFEF\b", re.I),

    # Educação superior
    _wholeword_pattern("Educação Superior"),
    _wholeword_pattern("Educacao Superior"),
    _wholeword_pattern("Ensino Superior"),
    _wholeword_pattern("Instituição de Ensino Superior"),
    _wholeword_pattern("Instituicao de Ensino Superior"),
    re.compile(r"\bIES\b", re.I),
    _wholeword_pattern("Credenciamento"),
    _wholeword_pattern("Recredenciamento"),
    _wholeword_pattern("Autorização de curso"),
    _wholeword_pattern("Autorizacao de curso"),
    _wholeword_pattern("Reconhecimento de curso"),
    _wholeword_pattern("Renovação de reconhecimento"),
    _wholeword_pattern("Renovacao de reconhecimento"),

    # Acórdão
    _wholeword_pattern("Acórdão"),
    _wholeword_pattern("Acordao"),
    re.compile(r"\bac[oó]rd[aã]o\b", re.I),

    # Novo PAC
    _wholeword_pattern("Novo PAC"),
    _wholeword_pattern("Novo Programa de Aceleração do Crescimento"),
    _wholeword_pattern("Novo Programa de Aceleracao do Crescimento"),

    # Registro Especial (genérico)
    _wholeword_pattern("Registro Especial"),
    re.compile(r"\bregesp\b", re.I),
]

# CNE
_CNE_PATTERNS = [
    _wholeword_pattern("Conselho Nacional de Educação"),
    _wholeword_pattern("Conselho Nacional de Educacao"),
    re.compile(r"\bCNE\b", re.I),
]

# CES (Câmara de Educação Superior) — só bloqueia se houver coocorrência com CNE
_CES_PATTERNS = [
    _wholeword_pattern("Câmara de Educação Superior"),
    _wholeword_pattern("Camara de Educacao Superior"),
    re.compile(r"\bCES\b", re.I),
]

# Decisões/pedidos de casos particulares
_DECISAO_CASE_REGEX = re.compile(
    r"\b("
    r"defiro|indefiro|deferido|indeferido|homologo|homologar|concedo|conceder|"
    r"autorizo|autorizar|reconheco|reconhecer|recredencio|recredenciar|"
    r"credencio|credenciar|reconhecido|credenciado|recredenciado|"
    r"aprovado|aprovo|aprovar|nego\s+provimento|dou\s+provimento|"
    r"julgo|julgar|decido|decidir"
    r")\b.*\b("
    r"pedido|requerimento|processo|interessado|interessada|"
    r"credenciamento|recredenciamento|autorizacao|reconhecimento"
    r")\b",
    re.I
)

def _has_any(text_norm: str, patterns: list[re.Pattern]) -> bool:
    return any(p and p.search(text_norm) for p in patterns)

def _is_blocked(text: str) -> bool:
    if not text:
        return False
    nt = _normalize_ws(text)

    for pat in EXCLUDE_PATTERNS:
        if pat and pat.search(nt):
            return True

    if _has_any(nt, _CNE_PATTERNS) and _has_any(nt, _CES_PATTERNS):
        return True

    if _DECISAO_CASE_REGEX.search(nt):
        return True

    return False


# Filtro específico para "Bebidas Alcoólicas"
_BEBIDAS_EXCLUDE_TERMS = [
    "ato declaratorio executivo",
    "registro especial",
    "regesp",
    "defis",
    "srrf",
    "drf",
    "efi2vit",
    "delegacia da receita federal",
    "cnpj",
    "ncm",
    "mapa",
    "engarrafador",
    "produtor",
    "importador",
    "marcas comerciais",
    "atualiza as marcas"
]

_BEBIDAS_WHITELIST_TERMS = [
    "lei",
    "decreto",
    "projeto de lei",
    "consulta publica",
    "audiencia publica",
    "campanha",
    "advertencia",
    "rotulagem",
    "publicidade",
    "propaganda",
    "tributacao",
    "aliquota",
    "saude publica",
    "controle de consumo",
    "controle de oferta",
    "pontos de venda",
    "seguranca viaria",
    "alcool e direcao",
    "monitoramento"
]

def _is_bebidas_ato_irrelevante(texto_bruto: str) -> bool:
    nt = _normalize_ws(texto_bruto)
    if any(t in nt for t in _BEBIDAS_WHITELIST_TERMS):
        return False
    if any(t in nt for t in _BEBIDAS_EXCLUDE_TERMS):
        return True
    return False


# Bloqueio genérico: atos/portarias de concessão/decisão individual (empresa/processo)
_ATO_EMPRESA_EXCLUDE_TERMS = [
    "ato declaratorio executivo",
    "registro especial",
    "regesp",
    "defis",
    "srrf",
    "drf",
    "cnpj",
    "ncm",
    "importador",
    "exportador",
    "engarrafador",
    "produtor",
    "estabelecimentos comerciais atacadistas",
    "cooperativas de produtores",
    "delegacia da receita federal",
]

_ATO_EMPRESA_DECISAO_REGEX = re.compile(
    r"\b("
    r"concede|conceder|defiro|indefiro|deferido|indeferido|"
    r"autoriza|autorizar|homologo|homologar|"
    r"credencio|credenciar|recredencio|recredenciar|"
    r"reconheco|reconhecer|aprovo|aprovar|"
    r"torna\s+publico\s+o\s+resultado"
    r")\b.*\b("
    r"registro\s+especial|regesp|"
    r"pedido|requerimento|processo|interessad[oa]|"
    r"credenciamento|recredenciamento|autorizacao|reconhecimento"
    r")\b",
    re.I
)

def _is_ato_decisao_empresa_irrelevante(texto_bruto: str) -> bool:
    nt = _normalize_ws(texto_bruto)
    if _ATO_EMPRESA_DECISAO_REGEX.search(nt):
        return True
    if any(t in nt for t in _ATO_EMPRESA_EXCLUDE_TERMS):
        return True
    return False


# Coleta do conteúdo completo da página do DOU
CONTEUDO_MAX = int(os.getenv("DOU_CONTEUDO_MAX", "49500"))
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


# Raspagem do DOU (DO1 + DO2) e coluna "secao" (DO1/DO2 em maiúsculo)
def raspa_dou(data=None, secoes=("do1", "do2")):
    if data is None:
        data = datetime.now().strftime('%d-%m-%Y')

    print(f'Raspando as notícias do dia {data} nas seções: {", ".join([s.upper() for s in secoes])}...')

    combined = {"jsonArray": []}

    for sec in secoes:
        try:
            url = f'http://www.in.gov.br/leiturajornal?data={data}&secao={sec}'
            page = requests.get(url, timeout=40, headers=_HDR)
            page.raise_for_status()

            soup = BeautifulSoup(page.text, 'html.parser')
            params = soup.find("script", {"id": "params"})
            if not params:
                print(f"[{sec.upper()}] Elemento <script id='params'> não encontrado.")
                continue

            j = json.loads(params.text)
            arr = j.get("jsonArray", []) or []

            for it in arr:
                if isinstance(it, dict):
                    it["secao"] = sec.upper()

            combined["jsonArray"].extend(arr)
            print(f"[{sec.upper()}] itens: {len(arr)}")

        except Exception as e:
            print(f"Erro ao raspar seção {sec.upper()}: {e}")
            continue

    if combined["jsonArray"]:
        print(f"Total de itens coletados: {len(combined['jsonArray'])}")
        return combined

    print("Nenhum item encontrado em DO1/DO2.")
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
        secao    = (resultado.get('secao') or '').strip()

        if _is_blocked(titulo + " " + resumo):
            continue

        texto_norm = _normalize_ws(titulo + " " + resumo)
        conteudo_pagina = None

        for palavra, patt in _PATTERNS_GERAL:
            if patt and patt.search(texto_norm):

                if palavra.strip().lower() == "bebidas alcoólicas":
                    if conteudo_pagina is None:
                        conteudo_pagina = _baixar_conteudo_pagina(link)
                    alltxt = f"{titulo}\n{resumo}\n{conteudo_pagina or ''}"
                    if _is_bebidas_ato_irrelevante(alltxt):
                        continue

                if conteudo_pagina is None:
                    conteudo_pagina = _baixar_conteudo_pagina(link)

                alltxt = f"{titulo}\n{resumo}\n{conteudo_pagina or ''}"
                if _is_ato_decisao_empresa_irrelevante(alltxt):
                    continue

                if _is_blocked(conteudo_pagina):
                    continue

                resultados_por_palavra[palavra].append({
                    'date': data_pub,
                    'title': titulo,
                    'href': link,
                    'abstract': resumo,
                    'content_page': conteudo_pagina,
                    'secao': secao
                })
                algum = True

    if not algum:
        print('Nenhum resultado encontrado (geral).')
        return None
    print('Palavras-chave (geral) encontradas.')
    return resultados_por_palavra


# Mapa: Cliente → Tema → Keywords (whole-word)
CLIENT_THEME_DATA = """
IAS|Educação|Matemática; Alfabetização; Alfabetização Matemática; Recomposição de aprendizagem; Plano Nacional de Educação
ISG|Educação|Tempo Integral; PNE; FUNDEB; Ensino Técnico Profissionalizante (EPT); Ensino Médio; Propag; Infraestrutura Escolar; Ensino Fundamental Integral; Albetização Integral
IU|Educação|Recomposição da aprendizagem; Educação em tempo integral; Fundeb; Educação e equidade; PNE; Educação Profissional e Técnologica (EPT)
Reúna|Educação|Matemática; Alfabetização; Alfabetização Matemática; Recomposição de aprendizagem; Plano Nacional de Educação; Emendas parlamentares educação
REMS|Esportes|Esporte e desenvolvimento social; esporte e educação; esporte e equidade; paradesporto
FMCSV|Primeira infância|Criança; Criança Feliz (sempre); alfabetização; creche; Conanda; maternidade; parentalidade; paternidade; primeira infancia; infantil; infancia; FUNDEB; educação básica; PNE; Homeschooling
IEPS|Saúde|SUS; Sistema Único de Saúde; Equidade em saúde; Seguros de saúde; populações vulneráveis; desigualdades sociais; Organização do SUS; políticas públicas em saúde; Governança do SUS; Regionalização em saúde; População negra em saúde; Saúde indígena; Povos originários; Saúde da pessoa idosa; envelhecimento ativo; Atenção Primária; Saúde da criança; Saúde do adolescente; Saúde da mulher; Saúde do homem; Saúde da pessoa com deficiência; Saúde da população LGBTQIA+; Financiamento da saúde; Emendas e orçamento da saúde; emendas parlamentares; Ministério da Saúde; Trabalhadores e profissionais de saúde; Força de trabalho em saúde; política de recursos humanos em saúde; Formação profissional de saúde; Cuidados primários em saúde; Emergências climáticas e ambientais em saúde; mudanças ambientais; adaptação climática; saúde ambiental; políticas climáticas; Vigilância em saúde; epidemiológica; Emergência em saúde; estado de emergência; Saúde suplementar; complementar; privada; planos; seguros; seguradoras; planos populares; Anvisa; ANS; Sandbox regulatório; Cartões e administradoras de benefícios em saúde; Economia solidária em saúde mental; Pessoa em situação de rua; saúde mental; Fiscalização de comunidades terapêuticas; Rede de atenção psicossocial; RAPS; unidades de acolhimento; assistência multiprofissional; centros de convivência; Cannabis; canabidiol; tratamento terapêutico; Desinstitucionalização; manicômios; hospitais de custódia; Saúde mental na infância; adolescência; escolas; comunidades escolares; protagonismo juvenil; Dependência química; vícios; ludopatia; Treinamento; capacitação em saúde mental; Intervenções terapêuticas em saúde mental; Internet e redes sociais na saúde mental; Violência psicológica; Surto psicótico
Manual|Saúde|Ozempic; Wegovy; Mounjaro; Telemedicina; Telessaúde; CBD; Cannabis Medicinal; CFM; Conselho Federal de Medicina; Farmácia Magistral; Medicamentos Manipulados; Minoxidil; Emagrecedores; Retenção de receita de medicamentos; tirzepatida; liraglutida
Mevo|Saúde|Prontuário eletrônico; dispensação eletrônica; telessaúde; assinatura digital; certificado digital; controle sanitário; prescrição por enfermeiros; doenças crônicas; responsabilização de plataformas digitais; regulamentação de marketplaces; segurança cibernética; inteligência artificial; digitalização do SUS; venda e distribuição de medicamentos; venda de medicamentos; Bula digital; Atesta CFM; Sistemas de Controle de farmácia; SNGPC; Farmacêutico Remoto; Medicamentos Isentos de Prescrição (MIPs); RNDS - Rede Nacional de Dados em Saúde; Interoperabilidade; Listas de Substâncias Entorpecentes, Psicotrópicas, Precursoras e outras; Substâncias sob controle especial; Tabela SUS; Saúde digital; SEIDIGI; ICP - Brasil; Farmácia popular; CMED
Umane|Saúde|SUS; Sistema Único de Saúde; Equidade em saúde; populações vulneráveis; desigualdades sociais; Organização do SUS; políticas públicas em saúde; Governança do SUS; Regionalização em saúde; População negra em saúde; Saúde indígena; Povos originários; Saúde da pessoa idosa; envelhecimento ativo; Atenção Primária; Saúde da criança; Saúde do adolescente; Saúde da mulher; Saúde do homem; Saúde da pessoa com deficiência; Saúde da população LGBTQIA+; Financiamento da saúde; Emendas e orçamento da saúde; emendas parlamentares; Ministério da Saúde; Trabalhadores e profissionais de saúde; Força de trabalho em saúde; política de recursos humanos em saúde; Formação profissional de saúde; Cuidados primários em saúde; Emergências climáticas e ambientais em saúde; mudanças ambientais; adaptação climática; saúde ambiental; políticas climáticas; Vigilância em saúde; epidemiológica; Emergência em saúde; estado de emergência; Saúde suplementar; complementar; privada; planos; seguros; seguradoras; planos populares; Anvisa; ANS; Sandbox regulatório; Cartões e administradoras de benefícios em saúde; Conass; Conasems
Cactus|Saúde|Saúde mental; saúde mental para meninas; saúde mental para juventude; saúde mental para mulheres; Rede de atenção psicossocial; RAPS; CAPS; Centro de Apoio Psicossocial; programa saúde na escola; bullying; cyberbullying; eca digital
Vital Strategies|Saúde|Saúde mental; Dados para a saúde; Morte evitável; Doenças crônicas não transmissíveis; Rotulagem de bebidas alcoólicas; Educação em saúde; Bebidas alcoólicas; Imposto seletivo; Rotulagem de alimentos; Alimentos ultraprocessados; Publicidade infantil; Publicidade de alimentos ultraprocessados; Tributação de bebidas alcoólicas; Alíquota de bebidas alcoólicas; Cigarro eletrônico; Controle de tabaco; Violência doméstica; Exposição a fatores de risco; Departamento de Saúde Mental; Hipertensão arterial; Saúde digital; Violência contra crianças; Violência contra mulheres; Feminicídio; COP 30
Coletivo Feminista|Direitos reprodutivos|aborto; nascituro; gestação acima de 22 semanas; interrupção legal da gestação; interrupção da gestação; Resolução 258 Conanda; vida por nascer; vida desde a concepção; criança por nascer; infanticídio; feticídio; assistolia fetal; medicamento abortivo; misoprostol; citotec; cytotec; mifepristona; ventre; assassinato de bebês; luto parental; síndrome pós aborto
IDEC|Saúde|Defesa do consumidor; Ação Civil Pública; Arbitragem; SAC; Concorrência; Reforma Tributária; Ultraprocessados; Doenças crônicas não transmissíveis (DCNTs); Obesidade; Codex Alimentarius; Gordura trans; Adoçantes; edulcorantes; Rotulagem de alimentos; Transgênicos; Organismos geneticamente modificados (OGMs); Marketing e publicidade de alimentos; Comunicação mercadológica; Escolas e alimentação escolar; Bebidas açucaradas; refrigerante; Programa Nacional de Alimentação Escolar (PNAE); Educação Alimentar e Nutricional (EAN); Agrotóxicos; pesticidas; defensivos fitossanitários; Orgânicos; Tributação de alimentos não saudáveis; Desertos alimentares; Desperdício de alimentos; Segurança Alimentar e Nutricional (SAN); Direito Humano à Alimentação; Fome; Sustentabilidade; Mudança climática; Plástico; Gestão de resíduos; Economia circular; Desmatamento; Greenwashing; Energia elétrica; Encargos tarifários; Subsídios na tarifa de energia; Descontos na tarifa de energia; Energia pré-paga; Abertura do mercado de energia para consumidor cativo; Mercado livre de energia; Qualidade do serviço de energia; Tarifa Social de Energia Elétrica; Geração térmica; Combustíveis fósseis; Transição energética; Descarbonização da matriz elétrica; Gases de efeito estufa; Acordo de Paris; Objetivos do Desenvolvimento Sustentável; Reestruturação do setor de energia; Reforma do setor elétrico; Modernização do setor elétrico; Itens de custo da tarifa de energia elétrica; Universalização do acesso à energia; Eficiência energética; Geração distribuída; Carvão mineral; Painel solar; Crédito imobiliário; Crédito consignado; Publicidade de crédito; Cartão de crédito; pagamento de fatura; parcelamento com e sem juros; Cartões pré-pagos; Programas de fidelidade; Cheque especial; Taxa de juros; Contrato de crédito; Endividamento de jovens; Crédito estudantil; Endividamento de idosos; Crédito por meio de aplicativos; Abertura e movimentação de conta bancária; Cobrança de serviços sem autorização; Cadastro Positivo; Contratação de serviços bancários com imposição de seguros e títulos de capitalização; Acessibilidade aos canais de serviços bancários (caixa eletrônico, agências, internet banking e aplicativos móveis); Contratação de pacotes de contas bancárias; Acesso à informação em caso de negativa de crédito; Plano de saúde; Saúde suplementar; Medicamentos isentos de prescrição (MIP); Medicamentos antibióticos; antimicrobianos; Propriedade intelectual; Patentes; Licença compulsória; Preços de medicamentos; Complexo Econômico-Industrial da Saúde; Saúde digital; Prontuário eletrônico; Rede Nacional de Dados em Saúde (RNDS); DATASUS; Proteção de dados pessoais; Telessaúde; Telecomunicações; Internet; TV por assinatura; Serviço de Acesso Condicionado (SeAC); Telefonia móvel; Telefonia fixa; TV digital; Lei Geral de Proteção de Dados (LGPD); Autoridade Nacional de Proteção de Dados (ANPD); Reconhecimento facial; Lei Geral de Telecomunicações; Bens reversíveis; Fundo de Universalização dos Serviços de Telecomunicações (FUST); Provedores de acesso; Franquia de internet; Marco Civil da Internet; Neutralidade de rede; zero rating; Privacidade; Lei de Acesso à Informação; Regulação de plataformas digitais; Desinformação; Fake news; Dados biométricos; Vazamento de dados; Telemarketing; Serviço de Valor Adicionado (SVA)
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
        secao    = (r.get('secao') or '').strip()

        if _is_blocked(titulo + " " + resumo):
            continue

        texto_norm = _normalize_ws(titulo + " " + resumo)
        conteudo_pagina = None
        for pat, cliente, kw in CLIENT_PATTERNS:
            if pat.search(texto_norm):

                if kw.strip().lower() == "bebidas alcoólicas":
                    if conteudo_pagina is None:
                        conteudo_pagina = _baixar_conteudo_pagina(link)
                    alltxt = f"{titulo}\n{resumo}\n{conteudo_pagina or ''}"
                    if _is_bebidas_ato_irrelevante(alltxt):
                        continue

                if conteudo_pagina is None:
                    conteudo_pagina = _baixar_conteudo_pagina(link)

                alltxt = f"{titulo}\n{resumo}\n{conteudo_pagina or ''}"
                if _is_ato_decisao_empresa_irrelevante(alltxt):
                    continue

                if _is_blocked(conteudo_pagina):
                    continue

                por_cliente[cliente].append([
                    data_pub,
                    cliente,
                    kw,
                    titulo,
                    link,
                    resumo,
                    conteudo_pagina,
                    "",
                    "",
                    secao
                ])

    return por_cliente


# Google Sheets helpers
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

COLS_GERAL = ["Data","Palavra-chave","Portaria","Link","Resumo","Conteúdo","Seção"]

# IMPORTANTE: manter igual sua planilha (Data, Cliente, Palavra-chave, Portaria, Link, ...)
COLS_CLIENTE = ["Data","Cliente","Palavra-chave","Portaria","Link","Resumo","Conteúdo","Alinhamento","Justificativa","Seção"]

def _ensure_header(ws, header):
    current = ws.row_values(1)

    if not current:
        ws.resize(rows=max(2, ws.row_count), cols=len(header))
        ws.update("1:1", [header])
        return

    if current == header:
        return

    if current == header[:-1]:
        ws.resize(rows=max(2, ws.row_count), cols=len(header))
        ws.update_cell(1, len(header), header[-1])
        return

    if "Seção" not in current:
        new_col = len(current) + 1
        ws.resize(rows=max(2, ws.row_count), cols=max(ws.col_count, new_col))
        ws.update_cell(1, new_col, "Seção")


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
                item.get('content_page',''),
                item.get('secao','')
            ])

    if rows_to_append:
        ws.insert_rows(rows_to_append, row=2, value_input_option='USER_ENTERED')
        print(f"{len(rows_to_append)} linhas adicionadas (geral).")
    else:
        print('Nenhum dado válido para salvar (geral).')


# Por cliente (dedupe por Link+Palavra-chave+Cliente)
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
                existing.add((
                    r[link_idx].strip(),
                    r[palavra_idx].strip() if len(r) > palavra_idx else "",
                    r[cliente_idx].strip() if len(r) > cliente_idx else ""
                ))

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


# E-mails
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
                link = r.get('href', '#')
                title = r.get('title', '(sem título)')
                secao = (r.get('secao') or '').strip()
                prefix = f"[{secao}] " if secao else ""
                parts.append(f"<li>{prefix}<a href='{link}'>{title}</a></li>")
            parts.append("</ul>")
    parts.append("</body></html>")
    html = "".join(parts)
    for dest in destinatarios:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}],
                sender={"email": sender_email},
                subject=titulo,
                html_content=html
            ))
            print(f"✅ E-mail (geral) enviado para {dest}")
        except (ApiException, Exception) as e:
            print(f"❌ Falha ao enviar (geral) para {dest}: {e}")

def envia_email_clientes(por_cliente: dict):
    if not por_cliente or all(not (rows) for rows in por_cliente.values()):
        print("Sem resultados para enviar (clientes) — nenhuma ocorrência encontrada.")
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

    sum_rows = []
    for cliente, rows in (por_cliente or {}).items():
        if not rows:
            continue
        kw_counts = Counter(r[2] for r in rows)
        top_kw = ", ".join(f"{k} ({n})" for k, n in kw_counts.most_common(3))
        sum_rows.append((cliente, len(rows), top_kw))
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
        for cliente, total_cli, top_kw in sum_rows:
            anchor = _slug(cliente)
            parts.append(f"<tr><td><a href='#{anchor}'>{cliente}</a></td><td>{total_cli}</td><td>{top_kw or '-'}</td></tr>")
        parts.append("</table>")

    for cliente, rows in (por_cliente or {}).items():
        if not rows:
            continue
        anchor = _slug(cliente)
        parts.append(f"<h2 id='{anchor}'>{cliente}</h2>")
        agrupados = {}
        for r in rows:
            kw = r[2]
            agrupados.setdefault(kw, []).append(r)

        for kw, lista in sorted(agrupados.items(), key=lambda kv: len(kv[1]), reverse=True):
            parts.append(f"<h3>Palavra-chave: {kw} — {len(lista)} ocorrência(s)</h3><ul>")
            for item in lista:
                secao_item = (item[-1] or "").strip()
                titulo_item = item[3] or "(sem título)"
                link = item[4]
                resumo = (item[5] or "").strip()
                prefix = f"[{secao_item}] " if secao_item else ""
                parts.append(
                    f"<li>{prefix}<a href='{link}' target='_blank'>{titulo_item}</a>"
                    + (f"<br><em>Resumo:</em> {resumo}" if resumo else "")
                    + "</li>"
                )
            parts.append("</ul>")

    parts.append("</body></html>")
    html = "".join(parts)

    for dest in destinatarios:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}],
                sender={"email": sender_email},
                subject=titulo,
                html_content=html
            ))
            print(f"✅ E-mail (clientes) enviado para {dest}")
        except (ApiException, Exception) as e:
            print(f"❌ Falha ao enviar (clientes) para {dest}: {e}")


# Execução principal
if __name__ == "__main__":
    conteudo = raspa_dou()

    geral = procura_termos(conteudo)
    salva_na_base(geral)
    envia_email_geral(geral)

    por_cliente = procura_termos_clientes(conteudo)
    salva_por_cliente(por_cliente)
    envia_email_clientes(por_cliente)

