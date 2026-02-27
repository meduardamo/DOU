import os
import re
import json
import html
import unicodedata
import requests
import gspread
from bs4 import BeautifulSoup
from datetime import datetime
from google.oauth2.service_account import Credentials

from brevo_python import ApiClient, Configuration
from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
from brevo_python.models.send_smtp_email import SendSmtpEmail
from brevo_python.rest import ApiException


# ---------------------------------------------------------------------------
# Normalização
# ---------------------------------------------------------------------------

def _normalize(s: str) -> str:
    if s is None:
        return ""
    t = unicodedata.normalize("NFD", str(s))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return t.lower()


def _normalize_ws(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _normalize(s)).strip()


def _wholeword_pattern(phrase: str):
    toks = [t for t in _normalize_ws(phrase).split() if t]
    if not toks:
        return None
    return re.compile(r"\b" + r"\s+".join(map(re.escape, toks)) + r"\b")


# ---------------------------------------------------------------------------
# Padrões de exclusão
# ---------------------------------------------------------------------------

EXCLUDE_PATTERNS = [
    # CREF/CONFEF
    _wholeword_pattern("Conselho Regional de Educação Física"),
    _wholeword_pattern("Conselho Federal de Educação Física"),
    _wholeword_pattern("Conselho Regional de Educacao Fisica"),
    _wholeword_pattern("Conselho Federal de Educacao Fisica"),
    re.compile(r"\bCREF\b", re.I),
    re.compile(r"\bCONFEF\b", re.I),

    # CFMV/CRMV
    _wholeword_pattern("Conselho Federal de Medicina Veterinária"),
    _wholeword_pattern("Conselho Federal de Medicina Veterinaria"),
    _wholeword_pattern("Conselho Regional de Medicina Veterinária"),
    _wholeword_pattern("Conselho Regional de Medicina Veterinaria"),
    re.compile(r"\bCFMV\b", re.I),
    re.compile(r"\bCRMV\b", re.I),

    # Educação superior
    _wholeword_pattern("Educação Superior"),
    _wholeword_pattern("Educacao Superior"),
    _wholeword_pattern("Ensino Superior"),
    _wholeword_pattern("Instituição de Ensino Superior"),
    _wholeword_pattern("Instituicao de Ensino Superior"),
    re.compile(r"\bIES\b", re.I),
    _wholeword_pattern("E-MEC"),
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

    # Retificação
    _wholeword_pattern("Retificação"),
    _wholeword_pattern("Retificacao"),
    re.compile(r"\bretifica[cç][aã]o\b", re.I),

    # Registro Especial
    _wholeword_pattern("Registro Especial"),
    re.compile(r"\bregesp\b", re.I),

    # Licitações
    _wholeword_pattern("Licitação"),
    _wholeword_pattern("Licitacao"),
    _wholeword_pattern("Pregão"),
    _wholeword_pattern("Pregao"),
    _wholeword_pattern("Tomada de Preços"),
    _wholeword_pattern("Tomada de Precos"),
    _wholeword_pattern("Chamamento Público"),
    _wholeword_pattern("Chamamento Publico"),
    _wholeword_pattern("Dispensa de Licitação"),
    _wholeword_pattern("Dispensa de Licitacao"),
    _wholeword_pattern("Inexigibilidade"),
    _wholeword_pattern("Aviso de Licitação"),
    _wholeword_pattern("Aviso de Licitacao"),
    _wholeword_pattern("Cotação eletrônica"),
    _wholeword_pattern("Cotacao eletronica"),
    re.compile(r"\b(preg[aã]o|concorr[eê]ncia|tomada\s+de\s+pre[cç]os|inexigibilidade)\b", re.I),
    re.compile(r"\b(aviso\s+de\s+licita[cç][aã]o|edital\s+(de\s+)?licita[cç][aã]o|chamamento\s+p[uú]blico)\b", re.I),
    re.compile(r"\bdispensa\s+de\s+licita[cç][aã]o\b", re.I),

    # Prorrogações / extratos
    _wholeword_pattern("Extrato de Contrato"),
    _wholeword_pattern("Extrato do Contrato"),
    _wholeword_pattern("Extrato de Termo Aditivo"),
    _wholeword_pattern("Extrato do Termo Aditivo"),
    _wholeword_pattern("Aditamento"),
    _wholeword_pattern("Prorrogação de Prazo"),
    _wholeword_pattern("Prorrogacao de Prazo"),
    _wholeword_pattern("Prorrogação de Vigência"),
    _wholeword_pattern("Prorrogacao de Vigencia"),
    _wholeword_pattern("Termo de Prorrogação"),
    _wholeword_pattern("Termo de Prorrogacao"),
    _wholeword_pattern("Apostilamento"),
    re.compile(r"\b(prorrog(a|ã)o|prorroga-se|aditivo|apostilamento|vig[eê]ncia)\b.*\b(contrato|conv[eê]nio)\b", re.I),
    re.compile(r"\bextrato\b.*\b(contrato|termo\s+aditivo|conv[eê]nio)\b", re.I),

    # Radiodifusão
    _wholeword_pattern("Radiodifusão"),
    _wholeword_pattern("Radiodifusao"),
    _wholeword_pattern("Serviço de Radiodifusão"),
    _wholeword_pattern("Servico de Radiodifusao"),
    _wholeword_pattern("Radiofrequências"),
    _wholeword_pattern("Radiofrequência"),
    _wholeword_pattern("Renovação de Outorga"),
    _wholeword_pattern("Renovacao de Outorga"),
    _wholeword_pattern("Retransmissão de Televisão"),
    _wholeword_pattern("Retransmissao de Televisao"),
    re.compile(r"\b(radiodifus[aã]o|rtv|retransmiss[aã]o|outorga|canal\s+\d+)\b", re.I),
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

_DECISAO_CASE_REGEX = re.compile(
    r"\b("
    r"defiro|indefiro|deferido|indeferido|homologo|homologar|"
    r"recredencio|recredenciar|credencio|credenciar|"
    r"credenciado|recredenciado|"
    r"nego\s+provimento|dou\s+provimento"
    r")\b",
    re.I,
)

_PROF_RH_PATTERNS = [
    re.compile(
        r"\b(contratac(?:a|ã)o|admiss(?:a|ã)o|nomeac(?:a|ã)o|designac(?:a|ã)o|convocac(?:a|ã)o|posse|exonerac(?:a|ã)o|dispensa)\b.*\bprofessor(?:a)?\b",
        re.I,
    ),
    re.compile(
        r"\bprofessor(?:a)?\b.*\b(contratac(?:a|ã)o|admiss(?:a|ã)o|nomeac(?:a|ã)o|designac(?:a|ã)o|convocac(?:a|ã)o|posse|exonerac(?:a|ã)o|dispensa)\b",
        re.I,
    ),
    re.compile(r"\b(processo\s+seletivo|selec(?:a|ã)o\s+simplificada|concurso\s+p[uú]blico)\b.*\bprofessor(?:a)?\b", re.I),
    re.compile(r"\bprofessor(?:a)?\b.*\b(processo\s+seletivo|selec(?:a|ã)o\s+simplificada|concurso\s+p[uú]blico)\b", re.I),
    re.compile(r"\bprofessor\s+(substituto|tempor[aá]rio|visitante)\b", re.I),
]

_ATO_EMPRESA_EXCLUDE_TERMS = [
    "ato declaratorio executivo",
    "registro especial",
    "regesp",
    "defis",
    "srrf",
    "drf",
    "cnpj",
    "ncm",
    "engarrafador",
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
    r"registro\s+especial|regesp"
    r")\b",
    re.I,
)

_BEBIDAS_EXCLUDE_TERMS = [
    "ato declaratorio executivo",
    "registro especial",
    "declara a inscricao",
    "concede o registro",
    "drf", "srrf", "defis", "efi2vit", "regesp",
    "delegacia da receita federal",
    "cnpj", "ncm", "mapa",
    "engarrafador",
    "marcas comerciais",
    "atualiza as marcas",
]

_BEBIDAS_WHITELIST_TERMS = [
    "lei", "decreto", "projeto de lei",
    "consulta publica", "audiencia publica",
    "campanha", "advertencia",
    "rotulagem", "publicidade", "propaganda",
    "tributacao", "aliquota",
    "saude publica",
    "controle de consumo", "controle de oferta",
    "pontos de venda",
    "seguranca viaria", "alcool e direcao",
    "monitoramento",
]


def _has_any(text_norm: str, patterns) -> bool:
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
    for pat in _PROF_RH_PATTERNS:
        if pat.search(nt):
            return True
    return False


def _is_bebidas_ato_irrelevante(texto_bruto: str) -> bool:
    nt = _normalize_ws(texto_bruto)
    if any(t in nt for t in _BEBIDAS_WHITELIST_TERMS):
        return False
    if any(t in nt for t in _BEBIDAS_EXCLUDE_TERMS):
        return True
    return False


def _is_ato_decisao_empresa_irrelevante(texto_bruto: str) -> bool:
    nt = _normalize_ws(texto_bruto)
    if _ATO_EMPRESA_DECISAO_REGEX.search(nt):
        return True
    if any(t in nt for t in _ATO_EMPRESA_EXCLUDE_TERMS):
        return True
    return False


# ---------------------------------------------------------------------------
# Download de conteúdo
# ---------------------------------------------------------------------------

CONTEUDO_MAX = int(os.getenv("DOU_CONTEUDO_MAX", "49500"))
_CONTENT_CACHE: dict[str, str] = {}

_HDR = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126 Safari/537.36"
    ),
}


def _baixar_conteudo_pagina(url: str) -> str:
    if not url:
        return ""
    if url in _CONTENT_CACHE:
        return _CONTENT_CACHE[url]
    try:
        r = requests.get(url, timeout=40, headers=_HDR, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for t in soup(["script", "style", "noscript"]):
            t.decompose()

        bloco = (
            soup.select_one("article#materia div.texto-dou")
            or soup.select_one("div.texto-dou")
        )
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
            "div#content-core", "div#content", "section#content",
        ]
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


# ---------------------------------------------------------------------------
# Raspagem — edição regular e extra
# ---------------------------------------------------------------------------

def _raspa_secoes(data: str, secoes: list[str], campo_secao: str = "secao") -> dict | None:
    """
    Raspagem genérica. Funciona tanto para edições regulares (DO1/DO2/DO3)
    quanto para edições extras (DO1E/DO2E/DO3E).
    `campo_secao` é a chave que será adicionada a cada item do jsonArray.
    """
    combined: dict[str, list] = {"jsonArray": []}

    for sec in secoes:
        try:
            url = f"https://www.in.gov.br/leiturajornal?data={data}&secao={sec}"
            page = requests.get(url, timeout=40, headers=_HDR, allow_redirects=True)
            page.raise_for_status()
            soup = BeautifulSoup(page.text, "html.parser")

            params = soup.find("script", {"id": "params"})
            raw_json = params.text.strip() if (params and params.text) else None

            if not raw_json:
                m = re.search(r'(\{"jsonArray"\s*:\s*\[.*?\]\s*\})', page.text, flags=re.S)
                raw_json = m.group(1).strip() if m else None

            if not raw_json:
                print(f"[{sec}] payload não encontrado. status={page.status_code}")
                continue

            j = json.loads(raw_json)
            arr = j.get("jsonArray", []) or []
            for it in arr:
                if isinstance(it, dict):
                    it[campo_secao] = sec
            combined["jsonArray"].extend(arr)
            print(f"[{sec}] itens: {len(arr)}")

        except Exception as e:
            print(f"Erro ao raspar seção {sec}: {e}")
            continue

    if combined["jsonArray"]:
        print(f"Total coletado: {len(combined['jsonArray'])} itens")
        return combined

    print(f"Nenhum item encontrado nas seções: {secoes}")
    return None


def raspa_dou(data: str | None = None, secoes: list[str] | None = None) -> dict | None:
    if data is None:
        data = datetime.now().strftime("%d-%m-%Y")
    if secoes is None:
        secoes = [s.strip() for s in (os.getenv("DOU_SECOES") or "DO1,DO2,DO3").split(",") if s.strip()]
    secoes = [s.upper() for s in secoes]
    print(f"Raspando edição regular — {data} — seções: {', '.join(secoes)}")
    return _raspa_secoes(data, secoes, campo_secao="secao")


def raspa_dou_extra(data: str | None = None, secoes: list[str] | None = None) -> dict | None:
    if data is None:
        data = datetime.now().strftime("%d-%m-%Y")
    if secoes is None:
        secoes = [s.strip() for s in (os.getenv("DOU_EXTRA_SECOES") or "DO1E,DO2E,DO3E").split(",") if s.strip()]
    secoes = [s.upper() for s in secoes]
    print(f"Raspando edição EXTRA — {data} — seções: {', '.join(secoes)}")
    return _raspa_secoes(data, secoes, campo_secao="secao")


# ---------------------------------------------------------------------------
# Palavras-chave gerais
# ---------------------------------------------------------------------------

PALAVRAS_GERAIS = [
    "Infância", "Criança", "Infantil", "Infâncias", "Crianças",
    "Educação", "Ensino", "Escolaridade",
    "Plano Nacional da Educação", "PNE", "Educacional",
    "Alfabetização", "Letramento",
    "Saúde", "Telessaúde", "Telemedicina",
    "Digital", "Digitais", "Prontuário",
    "Programa Saúde na Escola", "PSE",
    "Psicosocial", "Mental", "Saúde Mental", "Dados para a Saúde", "Morte Evitável",
    "Doenças Crônicas Não Transmissíveis", "Rotulagem de Bebidas Alcoólicas",
    "Educação em Saúde", "Bebidas Alcoólicas", "Imposto Seletivo",
    "Rotulagem de Alimentos", "Alimentos Ultraprocessados",
    "Publicidade Infantil", "Publicidade de Alimentos Ultraprocessados",
    "Tributação de Bebidas Alcoólicas", "Alíquota de Bebidas Alcoólicas",
    "Cigarro Eletrônico", "Controle de Tabaco", "Violência Doméstica",
    "Exposição a Fatores de Risco", "Departamento de Saúde Mental",
    "Hipertensão Arterial", "Alimentação Escolar", "PNAE", "Agora Tem Especialistas",
    "Alfabetização na Idade Certa", "Criança Alfabetizada", "Meta de Alfabetização",
    "Programa Criança Alfabetizada", "Pacto pela Alfabetização",
    "Recomposição das Aprendizagens em Alfabetização",
    "Alfabetização Matemática", "Analfabetismo Matemático",
    "Recomposição das Aprendizagens em Matemática",
    "Política Nacional de Matemática", "Saeb Matemática", "Ideb Matemática", "BNCC Matemática",
    "OBMEP", "Olimpíada Brasileira de Matemática das Escolas Públicas", "PNLD Matemática",
]
_PATTERNS_GERAL = [(kw, _wholeword_pattern(kw)) for kw in PALAVRAS_GERAIS]


# ---------------------------------------------------------------------------
# Palavras-chave por cliente
# ---------------------------------------------------------------------------

CLIENT_THEME_DATA = """
IAS|Educação|matemática; alfabetização; alfabetização matemática; recomposição de aprendizagem; plano nacional de educação; pne
ISG|Educação|tempo integral; pne; fundeb; ensino técnico profissionalizante; educação profissional e tecnológica; ept; ensino médio; propag; infraestrutura escolar; ensino fundamental integral; alfabetização integral; escola em tempo integral; programa escola em tempo integral; ensino fundamental em tempo integral
IU|Educação|recomposição da aprendizagem; educação em tempo integral; fundeb; educação e equidade; pne; educação profissional e tecnológica; ensino técnico profissionalizante; ept
Reúna|Educação|matemática; alfabetização; alfabetização matemática; recomposição de aprendizagem; plano nacional de educação; emendas parlamentares; pne
REMS|Esportes|esporte e desenvolvimento social; esporte e educação; esporte e equidade; paradesporto; desenvolvimento social; esporte educacional
FMCSV|Primeira infância|criança; criança feliz; alfabetização; creche; conanda; maternidade; parentalidade; paternidade; primeira infância; infantil; infância; fundeb; educação básica; plano nacional de educação; pne; homeschooling
IEPS|Saúde|sus; sistema único de saúde; equidade em saúde; atenção primária à saúde; aps; vigilância epidemiológica; planos de saúde; caps; seguros de saúde; populações vulneráveis; desigualdades sociais; organização do sus; políticas públicas em saúde; governança do sus; regionalização em saúde; população negra em saúde; saúde indígena; povos originários; saúde da pessoa idosa; envelhecimento ativo; atenção primária; saúde da criança; saúde do adolescente; saúde da mulher; saúde do homem; saúde da pessoa com deficiência; saúde da população lgbtqia+; financiamento da saúde; emendas e orçamento da saúde; emendas parlamentares; ministério da saúde; trabalhadores e profissionais de saúde; força de trabalho em saúde; política de recursos humanos em saúde; formação profissional de saúde; cuidados primários em saúde; emergências climáticas e ambientais em saúde; emergências climáticas; mudanças ambientais; adaptação climática; saúde ambiental; políticas climáticas; vigilância em saúde; epidemiológica; emergência em saúde; estado de emergência; saúde suplementar; seguradoras; planos populares; anvisa; ans; sandbox regulatório; cartões e administradoras de benefícios em saúde; economia solidária em saúde mental; pessoa em situação de rua; saúde mental; fiscalização de comunidades terapêuticas; rede de atenção psicossocial; raps; unidades de acolhimento; assistência multiprofissional; centros de convivência; cannabis; canabidiol; tratamento terapêutico; desinstitucionalização; manicômios; hospitais de custódia; saúde mental na infância; adolescência; escolas; comunidades escolares; protagonismo juvenil; dependência química; vícios; ludopatia; treinamento; capacitação em saúde mental; intervenções terapêuticas em saúde mental; internet e redes sociais na saúde mental; violência psicológica; surto psicótico
Manual|Saúde|ozempic; wegovy; mounjaro; telemedicina; telessaúde; cbd; cannabis medicinal; cfm; conselho federal de medicina; farmácia magistral; medicamentos manipulados; minoxidil; emagrecedores; retenção de receita; tirzepatida; liraglutida
Mevo|Saúde|prontuário eletrônico; dispensação eletrônica; telessaúde; assinatura digital; certificado digital; controle sanitário; prescrição por enfermeiros; doenças crônicas; responsabilização de plataformas digitais; regulamentação de marketplaces; segurança cibernética; inteligência artificial; digitalização do sus; venda e distribuição de medicamentos; bula digital; atesta cfm; sistemas de controle de farmácia; sngpc; farmacêutico remoto; medicamentos isentos de prescrição (mips); rnds - rede nacional de dados em saúde; interoperabilidade; listas de substâncias entorpecentes, psicotrópicas, precursoras e outras; substâncias entorpecentes; substâncias psicotrópicas; substâncias precursoras; substâncias sob controle especial; tabela sus; saúde digital; seidigi; icp-brasil; farmácia popular; cmed
Umane|Saúde|sus; sistema único de saúde; atenção primária à saúde; aps; vigilância epidemiológica; planos de saúde; caps; equidade em saúde; populações vulneráveis; desigualdades sociais; organização do sus; políticas públicas em saúde; governança do sus; regionalização em saúde; população negra em saúde; saúde indígena; povos originários; saúde da pessoa idosa; envelhecimento ativo; atenção primária; saúde da criança; saúde do adolescente; saúde da mulher; saúde do homem; saúde da pessoa com deficiência; saúde da população lgbtqia+; financiamento da saúde; emendas e orçamento da saúde; emendas parlamentares; ministério da saúde; trabalhadores e profissionais de saúde; força de trabalho em saúde; política de recursos humanos em saúde; formação profissional de saúde; cuidados primários em saúde; emergências climáticas e ambientais em saúde; emergências climáticas; mudanças ambientais; adaptação climática; saúde ambiental; políticas climáticas; vigilância em saúde; epidemiológica; emergência em saúde; estado de emergência; saúde suplementar; seguradoras; planos populares; anvisa; ans; sandbox regulatório; cartões e administradoras de benefícios em saúde; conass; conasems
Cactus|Saúde|saúde mental; saúde mental para meninas; saúde mental para juventude; saúde mental para mulheres; pse; eca; rede de atenção psicossocial; raps; caps; centro de apoio psicossocial; programa saúde na escola; bullying; cyberbullying; eca digital
Vital Strategies|Saúde|saúde mental; dados para a saúde; morte evitável; doenças crônicas não transmissíveis; rotulagem de bebidas alcoólicas; educação em saúde; bebidas alcoólicas; imposto seletivo; dcnts; rotulagem de alimentos; alimentos ultraprocessados; publicidade infantil; publicidade de alimentos ultraprocessados; tributação de bebidas alcoólicas; alíquota de bebidas alcoólicas; cigarro eletrônico; controle de tabaco; violência doméstica; exposição a fatores de risco; departamento de saúde mental; hipertensão arterial; saúde digital; violência contra crianças; violência contra mulheres; feminicídio; cop 30
Coletivo Feminista|Direitos reprodutivos|aborto; nascituro; gestação acima de 22 semanas; interrupção legal da gestação; interrupção da gestação; resolução 258 conanda; vida por nascer; vida desde a concepção; criança por nascer; infanticídio; feticídio; assistolia fetal; medicamento abortivo; misoprostol; citotec; cytotec; mifepristona; ventre; assassinato de bebês; luto parental; síndrome pós aborto
IDEC|Saúde|defesa do consumidor; ação civil pública; sac; reforma tributária; ultraprocessados; doenças crônicas não transmissíveis; dcnts; obesidade; codex alimentarius; gordura trans; adoçantes; edulcorantes; rotulagem de alimentos; transgênicos; organismos geneticamente modificados; (ogms); marketing e publicidade de alimentos; comunicação mercadológica; escolas e alimentação escolar; bebidas açucaradas; refrigerante; programa nacional de alimentação escolar; pnae; educação alimentar e nutricional; ean; agrotóxicos; pesticidas; defensivos fitossanitários; tributação de alimentos não saudáveis; desertos alimentares; desperdício de alimentos; segurança alimentar e nutricional; san; direito humano à alimentação; fome; sustentabilidade; mudança climática; plástico; gestão de resíduos; economia circular; desmatamento; greenwashing; energia elétrica; encargos tarifários; subsídios na tarifa de energia; descontos na tarifa de energia; energia pré-paga; abertura do mercado de energia para consumidor cativo; mercado livre de energia; qualidade do serviço de energia; serviço de energia; tarifa social de energia elétrica; geração térmica; combustíveis fósseis; transição energética; descarbonização da matriz elétrica; descarbonização; gases de efeito estufa; acordo de paris; objetivos do desenvolvimento sustentável; reestruturação do setor de energia; reforma do setor elétrico; modernização do setor elétrico; itens de custo da tarifa de energia elétrica; universalização do acesso à energia; eficiência energética; geração distribuída; carvão mineral; painel solar; crédito imobiliário; crédito consignado; publicidade de crédito; cartão de crédito; pagamento de fatura; parcelamento com e sem juros; cartões pré-pagos; programas de fidelidade; cheque especial; taxa de juros; contrato de crédito; endividamento de jovens; crédito estudantil; endividamento de idosos; crédito por meio de aplicativos; abertura e movimentação de conta bancária; cobrança de serviços sem autorização; cadastro positivo; contratação de serviços bancários com imposição de seguros e títulos de capitalização; acessibilidade aos canais de serviços bancários; serviços bancários; caixa eletrônico; internet banking; aplicativos móveis; contratação de pacotes de contas bancárias; acesso à informação em caso de negativa de crédito; plano de saúde; saúde suplementar; medicamentos isentos de prescrição; mip; medicamentos antibióticos; antimicrobianos; propriedade intelectual; patentes; licença compulsória; preços de medicamentos; complexo econômico-industrial da saúde; saúde digital; prontuário eletrônico; rede nacional de dados em saúde; rnds; datasus; proteção de dados pessoais; telessaúde; telecomunicações; internet; tv por assinatura; serviço de acesso condicionado (seac); telefonia móvel; telefonia fixa; tv digital; lei geral de proteção de dados (lgpd); autoridade nacional de proteção de dados (anpd); reconhecimento facial; lei geral de telecomunicações; bens reversíveis; fundo de universalização dos serviços de telecomunicações (fust); provedores de acesso; franquia de internet; marco civil da internet; neutralidade de rede; zero rating; privacidade; lei de acesso à informação; regulação de plataformas digitais; desinformação; fake news; dados biométricos; vazamento de dados; telemarketing; serviço de valor adicionado
""".strip()


def _parse_client_keywords(text: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        cliente, _tema, kws = [x.strip() for x in line.split("|", 2)]
        out.setdefault(cliente, [])
        for kw in [k.strip() for k in kws.split(";") if k.strip()]:
            if kw not in out[cliente]:
                out[cliente].append(kw)
    return out


CLIENT_KEYWORDS = _parse_client_keywords(CLIENT_THEME_DATA)

CLIENT_PATTERNS: list[tuple] = []
for _cli, _kws in CLIENT_KEYWORDS.items():
    for _kw in _kws:
        _pat = _wholeword_pattern(_kw)
        if _pat:
            CLIENT_PATTERNS.append((_pat, _cli, _kw))


# ---------------------------------------------------------------------------
# Busca de termos — geral
# ---------------------------------------------------------------------------

# Seções que NÃO entram nos resultados gerais, por tipo de edição
_SECAO_GERAL_BLOQUEADAS = {"DO3", "DO3E"}


def procura_termos(conteudo_raspado: dict | None, campo_secao: str = "secao") -> dict | None:
    """
    Busca palavras-chave gerais. Funciona para edição regular e extra.
    `campo_secao` deve corresponder ao campo usado na raspagem ("secao" ou "secao" — ambos
    são chamados de "secao" agora, pois _raspa_secoes usa campo_secao="secao" em ambos os casos).
    """
    if not conteudo_raspado or "jsonArray" not in conteudo_raspado:
        print("Nenhum conteúdo para analisar (geral).")
        return None

    URL_BASE = "https://www.in.gov.br/en/web/dou/-/"
    resultados_por_palavra: dict[str, list] = {kw: [] for kw in PALAVRAS_GERAIS}
    algum = False

    for r in conteudo_raspado["jsonArray"]:
        titulo = r.get("title", "Título não disponível")
        resumo = r.get("content", "")
        link = URL_BASE + (r.get("urlTitle", "") or "")
        data_pub = (r.get("pubDate", "") or "")[:10]
        secao = (r.get(campo_secao, "") or "").strip().upper()

        if secao in _SECAO_GERAL_BLOQUEADAS:
            continue

        if _is_blocked(titulo + " " + resumo):
            continue

        texto_norm = _normalize_ws(titulo + " " + resumo)
        conteudo_pagina = None

        for palavra, patt in _PATTERNS_GERAL:
            if not (patt and patt.search(texto_norm)):
                continue

            if palavra.strip().lower() == "bebidas alcoólicas":
                if conteudo_pagina is None:
                    conteudo_pagina = _baixar_conteudo_pagina(link)
                if _is_bebidas_ato_irrelevante(f"{titulo}\n{resumo}\n{conteudo_pagina or ''}"):
                    continue

            if conteudo_pagina is None:
                conteudo_pagina = _baixar_conteudo_pagina(link)

            if _is_ato_decisao_empresa_irrelevante(f"{titulo}\n{resumo}\n{conteudo_pagina or ''}"):
                continue

            resultados_por_palavra[palavra].append({
                "date": data_pub,
                "title": titulo,
                "href": link,
                "abstract": resumo,
                "content_page": conteudo_pagina or "",
                "secao": secao,
            })
            algum = True

    if not algum:
        print("Nenhum resultado encontrado (geral).")
        return None

    print("Palavras-chave gerais encontradas.")
    return resultados_por_palavra


# ---------------------------------------------------------------------------
# Busca de termos — por cliente
# ---------------------------------------------------------------------------

def procura_termos_clientes(conteudo_raspado: dict | None, campo_secao: str = "secao") -> dict[str, list]:
    if not conteudo_raspado or "jsonArray" not in conteudo_raspado:
        print("Nenhum conteúdo para analisar (clientes).")
        return {}

    URL_BASE = "https://www.in.gov.br/en/web/dou/-/"
    agreg: dict[tuple, dict] = {}

    for r in conteudo_raspado["jsonArray"]:
        titulo = r.get("title", "Título não disponível")
        resumo = r.get("content", "")
        link = URL_BASE + (r.get("urlTitle", "") or "")
        data_pub = (r.get("pubDate", "") or "")[:10]
        secao = (r.get(campo_secao, "") or "").strip().upper()

        if not link:
            continue
        if _is_blocked(titulo + " " + resumo):
            continue

        texto_norm = _normalize_ws(titulo + " " + resumo)

        hits = []
        for pat, cliente, kw in CLIENT_PATTERNS:
            if not pat.search(texto_norm):
                continue
            # DO3 / DO3E só para Mevo
            if secao in {"DO3", "DO3E"} and cliente != "Mevo":
                continue
            hits.append((cliente, kw))

        if not hits:
            continue

        conteudo_pagina = _baixar_conteudo_pagina(link)
        alltxt = f"{titulo}\n{resumo}\n{conteudo_pagina or ''}"

        if any(kw.strip().lower() == "bebidas alcoólicas" for _, kw in hits):
            if _is_bebidas_ato_irrelevante(alltxt):
                continue

        if _is_ato_decisao_empresa_irrelevante(alltxt):
            continue

        for cliente, kw in hits:
            key = (cliente, link)
            if key not in agreg:
                agreg[key] = {
                    "date": data_pub,
                    "cliente": cliente,
                    "kws": set(),
                    "title": titulo,
                    "href": link,
                    "abstract": resumo,
                    "content_page": conteudo_pagina or "",
                    "secao": secao,
                }
            agreg[key]["kws"].add(kw)

    por_cliente: dict[str, list] = {c: [] for c in CLIENT_KEYWORDS}
    for (_cli, _href), d in agreg.items():
        kws_join = "; ".join(sorted(d["kws"], key=lambda x: _normalize_ws(x)))
        por_cliente[d["cliente"]].append([
            d["date"],
            d["cliente"],
            kws_join,
            d["title"],
            d["href"],
            d["abstract"],
            d["content_page"],
            "",
            "",
            d["secao"],
        ])

    return por_cliente


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------

def _gs_client_from_env():
    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if not raw:
        jf = "credentials.json"
        if os.path.exists(jf):
            creds = Credentials.from_service_account_file(jf, scopes=scopes)
            return gspread.authorize(creds)
        raise RuntimeError("Secret GOOGLE_APPLICATION_CREDENTIALS_JSON não encontrado.")
    info = json.loads(raw)
    if "private_key" in info and "\\n" in info["private_key"]:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


COLS_GERAL = ["Data", "Palavra-chave", "Portaria", "Link", "Resumo", "Conteúdo", "Seção"]
COLS_CLIENTE = ["Data", "Cliente", "Palavra-chave", "Portaria", "Link", "Resumo", "Conteúdo", "Alinhamento", "Justificativa", "Seção"]


def _ensure_header(ws, header: list[str]) -> None:
    current = ws.row_values(1)
    if current == header:
        return
    ws.resize(rows=max(2, ws.row_count), cols=len(header))
    ws.update("1:1", [header])


def _ws_gid(ws) -> str:
    try:
        return str(ws.id)
    except Exception:
        return ""


def salva_na_base(palavras_raspadas: dict | None) -> tuple[int, list, object | None, object | None]:
    """Salva resultados gerais na planilha (com deduplicação). Retorna (qtd, itens, sh, ws)."""
    if not palavras_raspadas:
        print("Sem resultados gerais para salvar.")
        return 0, [], None, None

    gc = _gs_client_from_env()
    planilha_id = os.getenv("PLANILHA")
    if not planilha_id:
        raise RuntimeError("Env PLANILHA não definido.")

    sh = gc.open_by_key(planilha_id)
    try:
        ws = sh.worksheet("Página1")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Página1", rows="2000", cols=str(len(COLS_GERAL)))

    _ensure_header(ws, COLS_GERAL)

    link_idx = COLS_GERAL.index("Link")
    palavra_idx = COLS_GERAL.index("Palavra-chave")

    all_vals = ws.get_all_values()
    existing: set[tuple] = set()
    if len(all_vals) > 1:
        for row in all_vals[1:]:
            if len(row) > link_idx:
                existing.add((
                    row[link_idx].strip(),
                    row[palavra_idx].strip() if len(row) > palavra_idx else "",
                ))

    rows_to_insert = []
    inserted_items = []

    for palavra, lista in (palavras_raspadas or {}).items():
        for item in lista:
            href = (item.get("href", "") or "").strip()
            key = (href, palavra)
            if not href or key in existing:
                continue
            rows_to_insert.append([
                item.get("date", ""),
                palavra,
                item.get("title", ""),
                href,
                item.get("abstract", ""),
                item.get("content_page", ""),
                item.get("secao", ""),
            ])
            inserted_items.append({
                "date": item.get("date", ""),
                "secao": item.get("secao", ""),
                "keyword": palavra,
                "title": item.get("title", ""),
                "href": href,
                "abstract": item.get("abstract", ""),
            })
            existing.add(key)

    if rows_to_insert:
        ws.insert_rows(rows_to_insert, row=2, value_input_option="USER_ENTERED")
        print(f"{len(rows_to_insert)} linhas adicionadas (geral).")
    else:
        print("Nenhuma linha nova (geral).")

    return len(rows_to_insert), inserted_items, sh, ws


def _append_dedupe_por_cliente(sh, sheet_name: str, rows: list[list]) -> tuple[int, list]:
    if not rows:
        return 0, []

    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=str(max(100, len(rows) + 10)), cols=len(COLS_CLIENTE))

    _ensure_header(ws, COLS_CLIENTE)

    link_idx = COLS_CLIENTE.index("Link")
    kw_idx = COLS_CLIENTE.index("Palavra-chave")
    cli_idx = COLS_CLIENTE.index("Cliente")

    all_vals = ws.get_all_values()
    existing: set[tuple] = set()
    if len(all_vals) > 1:
        for row in all_vals[1:]:
            if len(row) > link_idx:
                existing.add((
                    row[link_idx].strip(),
                    row[kw_idx].strip() if len(row) > kw_idx else "",
                    row[cli_idx].strip() if len(row) > cli_idx else "",
                ))

    new_rows = []
    inserted_items = []
    for r in rows:
        if len(r) <= link_idx:
            continue
        href = (r[link_idx] or "").strip()
        kw = (r[kw_idx] or "").strip()
        cli = (r[cli_idx] or "").strip()
        key = (href, kw, cli)
        if not href or key in existing:
            continue
        new_rows.append(r)
        inserted_items.append({
            "date": r[0],
            "cliente": cli,
            "keyword": kw,
            "title": r[3],
            "href": href,
            "abstract": r[5],
        })
        existing.add(key)

    if not new_rows:
        return 0, []

    ws.insert_rows(new_rows, row=2, value_input_option="USER_ENTERED")
    return len(new_rows), inserted_items


def salva_por_cliente(por_cliente: dict) -> tuple[int, dict, object | None, dict]:
    plan_id = os.getenv("PLANILHA_CLIENTES")
    if not plan_id:
        print("PLANILHA_CLIENTES não definido; pulando saída por cliente.")
        return 0, {}, None, {}

    gc = _gs_client_from_env()
    sh = gc.open_by_key(plan_id)

    gids: dict[str, str] = {}
    for cli in CLIENT_KEYWORDS:
        try:
            ws = sh.worksheet(cli)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=cli, rows="2", cols=len(COLS_CLIENTE))
        _ensure_header(ws, COLS_CLIENTE)
        gids[cli] = _ws_gid(ws)

    total_new = 0
    inserted_map: dict[str, list] = {}

    for cli, rows in (por_cliente or {}).items():
        n, items = _append_dedupe_por_cliente(sh, cli, rows)
        if n > 0:
            total_new += n
            inserted_map[cli] = items

    return total_new, inserted_map, sh, gids


# ---------------------------------------------------------------------------
# E-mail
# ---------------------------------------------------------------------------

EMAIL_RE = re.compile(r'<?("?)([^"\s<>@]+@[^"\s<>@]+\.[^"\s<>@]+)\1>?$')


def _sanitize_emails(raw_list: str) -> list[str]:
    if not raw_list:
        return []
    parts = re.split(r"[,\n;]+", raw_list)
    emails, seen = [], set()
    for it in parts:
        s = unicodedata.normalize("NFKC", it)
        s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s).strip().strip("'").strip('"')
        if not s:
            continue
        m = EMAIL_RE.match(s)
        candidate = (m.group(2) if m else s).strip()
        if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", candidate) and candidate.lower() not in seen:
            seen.add(candidate.lower())
            emails.append(candidate.lower())
    return emails


def _brevo_client():
    api_key = os.getenv("BREVO_API_KEY")
    if not api_key:
        return None
    cfg = Configuration()
    cfg.api_key["api-key"] = api_key
    return TransactionalEmailsApi(ApiClient(configuration=cfg))


def _gs_tab_url(sheet_id: str, gid: str | None) -> str:
    if not sheet_id:
        return ""
    if not gid:
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"


def _unique_hrefs(*inserted_lists) -> int:
    hrefs: set[str] = set()
    for lst in inserted_lists:
        if isinstance(lst, list):
            for it in lst:
                h = (it.get("href", "") or "").strip()
                if h:
                    hrefs.add(h)
        elif isinstance(lst, dict):
            for items in lst.values():
                for it in items:
                    h = (it.get("href", "") or "").strip()
                    if h:
                        hrefs.add(h)
    return len(hrefs)


def _build_html_email(
    inserted_geral: list,
    inserted_clientes: dict,
    planilha_id: str,
    planilha_gid: str | None,
    planilha_clientes_id: str,
    titulo: str,
) -> str:
    """Constrói o HTML de um e-mail (regular ou extra — chamado separadamente para cada)."""
    total_itens = _unique_hrefs(inserted_geral, inserted_clientes)
    if total_itens == 0:
        return ""

    n_geral = len(inserted_geral)
    n_clientes = sum(len(v) for v in inserted_clientes.values())

    geral_url = _gs_tab_url(planilha_id, planilha_gid)
    clientes_url = _gs_tab_url(planilha_clientes_id, None)

    # Monta tabela de acionamentos: palavra-chave → destino(s)
    kw_geral: dict[str, bool] = {}
    kw_clientes: dict[str, set] = {}

    for it in inserted_geral:
        kw = (it.get("keyword", "") or "").strip()
        if kw:
            kw_geral[kw] = True

    for cli, items in inserted_clientes.items():
        for it in items:
            kw = (it.get("keyword", "") or "").strip()
            if kw:
                kw_clientes.setdefault(kw, set()).add(cli)

    all_kws = sorted(set(list(kw_geral) + list(kw_clientes)), key=lambda x: x.lower())

    lines = []
    for kw in all_kws:
        in_g = kw_geral.get(kw, False)
        clients = sorted(kw_clientes.get(kw, set()))
        if in_g and clients:
            destino = f"Geral + Cliente: {', '.join(clients)}"
        elif in_g:
            destino = "Geral"
        else:
            destino = f"Cliente: {', '.join(clients)}"
        lines.append(f"<li><b>{html.escape(kw)}</b> → {html.escape(destino)}</li>")

    acionamentos_html = (
        "<ul style='margin:6px 0 0 18px'>" + "".join(lines) + "</ul>"
        if lines else "<p style='margin:6px 0 0'>—</p>"
    )

    return f"""
    <html>
      <body style="font-family: Arial, Helvetica, sans-serif; color:#111; line-height:1.35;">
        <div style="max-width:820px;">
          <h2 style="margin:0 0 6px 0;">{html.escape(titulo)}</h2>
          <p style="margin:0 0 14px 0;"><b>{total_itens}</b> itens novos encontrados.</p>

          <div style="padding:12px; border:1px solid #e5e7eb; border-radius:10px; margin-bottom:12px;">
            <div>Entradas na planilha geral: <b>{n_geral}</b></div>
            <div>Entradas por cliente: <b>{n_clientes}</b></div>
            <div style="margin-top:10px;">
              <a href="{html.escape(geral_url)}" target="_blank"
                 style="display:inline-block; padding:8px 10px; border:1px solid #111;
                        border-radius:8px; text-decoration:none; margin-right:8px;">
                 Planilha geral
              </a>
              <a href="{html.escape(clientes_url)}" target="_blank"
                 style="display:inline-block; padding:8px 10px; border:1px solid #111;
                        border-radius:8px; text-decoration:none;">
                 Planilha por cliente
              </a>
            </div>
          </div>

          <div style="padding:12px; border:1px solid #e5e7eb; border-radius:10px;">
            <b>Acionamentos (palavra-chave → destino)</b>
            {acionamentos_html}
          </div>
        </div>
      </body>
    </html>
    """


def envia_email(
    inserted_geral: list,
    inserted_clientes: dict,
    planilha_id: str,
    planilha_gid: str | None,
    planilha_clientes_id: str,
    subject: str,
    titulo_html: str,
) -> None:
    """Envia um único e-mail. Chamado separadamente para regular e extra."""
    api = _brevo_client()
    sender_email = os.getenv("EMAIL")
    raw_dest = os.getenv("DESTINATARIOS", "")
    if not (api and sender_email and raw_dest):
        print("Dados de e-mail incompletos; pulando envio.")
        return

    total = _unique_hrefs(inserted_geral, inserted_clientes)
    if total == 0:
        print("Nada novo — e-mail não será enviado.")
        return

    html_body = _build_html_email(
        inserted_geral=inserted_geral,
        inserted_clientes=inserted_clientes,
        planilha_id=planilha_id,
        planilha_gid=planilha_gid,
        planilha_clientes_id=planilha_clientes_id,
        titulo=titulo_html,
    )
    if not html_body:
        print("HTML vazio — pulando envio.")
        return

    destinatarios = _sanitize_emails(raw_dest)
    for dest in destinatarios:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}],
                sender={"email": sender_email},
                subject=subject,
                html_content=html_body,
            ))
            print(f"✅ E-mail enviado para {dest}")
        except (ApiException, Exception) as e:
            print(f"❌ Falha ao enviar para {dest}: {e}")


# ---------------------------------------------------------------------------
# Entrypoints
# ---------------------------------------------------------------------------

def executar_regular():
    """Raspa e processa a edição regular do DOU. Envia um e-mail próprio."""
    conteudo = raspa_dou()

    geral = procura_termos(conteudo)
    _qtd_g, ins_g, _sh, ws_geral = salva_na_base(geral)

    por_cliente = procura_termos_clientes(conteudo)
    _qtd_c, ins_c, _sh_c, _gids = salva_por_cliente(por_cliente)

    hoje = datetime.now().strftime("%d-%m-%Y")
    total = _unique_hrefs(ins_g, ins_c)
    envia_email(
        inserted_geral=ins_g,
        inserted_clientes=ins_c,
        planilha_id=os.getenv("PLANILHA", ""),
        planilha_gid=_ws_gid(ws_geral) if ws_geral else None,
        planilha_clientes_id=os.getenv("PLANILHA_CLIENTES", ""),
        subject=f"DOU Regular — {hoje} | {total} itens novos",
        titulo_html="📋 DOU — Edição Regular",
    )


def executar_extra():
    """Raspa e processa a edição extra do DOU. Envia um e-mail próprio."""
    conteudo = raspa_dou_extra()

    geral = procura_termos(conteudo)
    _qtd_g, ins_g, _sh, ws_geral = salva_na_base(geral)

    por_cliente = procura_termos_clientes(conteudo)
    _qtd_c, ins_c, _sh_c, _gids = salva_por_cliente(por_cliente)

    hoje = datetime.now().strftime("%d-%m-%Y")
    hora = datetime.now().strftime("%H:%M")
    total = _unique_hrefs(ins_g, ins_c)
    envia_email(
        inserted_geral=ins_g,
        inserted_clientes=ins_c,
        planilha_id=os.getenv("PLANILHA", ""),
        planilha_gid=_ws_gid(ws_geral) if ws_geral else None,
        planilha_clientes_id=os.getenv("PLANILHA_CLIENTES", ""),
        subject=f"DOU Extra — {hoje} {hora} | {total} itens novos",
        titulo_html="⚡ DOU — Edição Extra",
    )


def executar_tudo():
    """Raspa edição regular + extra, enviando um e-mail para cada."""
    executar_regular()
    executar_extra()


if __name__ == "__main__":
    import sys
    modo = sys.argv[1] if len(sys.argv) > 1 else "tudo"

    if modo == "regular":
        executar_regular()
    elif modo == "extra":
        executar_extra()
    else:
        executar_tudo()
