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


# ✅ manter tudo que o outro código bloqueia (CREF/CONFEF + CNE & CES) + extras que você pediu (CFMV + contratação de professor)
EXCLUDE_PATTERNS = [
    # --- do seu código "base" ---
    _wholeword_pattern("Conselho Regional de Educação Física"),
    _wholeword_pattern("Conselho Federal de Educação Física"),
    _wholeword_pattern("Conselho Regional de Educacao Fisica"),
    _wholeword_pattern("Conselho Federal de Educacao Fisica"),
    re.compile(r"\bCREF\b", re.I),
    re.compile(r"\bCONFEF\b", re.I),

    # --- extras que você pediu no DOU EXTRA ---
    _wholeword_pattern("Conselho Federal de Medicina Veterinária"),
    _wholeword_pattern("Conselho Federal de Medicina Veterinaria"),
    re.compile(r"\bCFMV\b", re.I),

    _wholeword_pattern("contratação de professor"),
    _wholeword_pattern("contratacao de professor"),
    _wholeword_pattern("contratação de professores"),
    _wholeword_pattern("contratacao de professores"),
    _wholeword_pattern("contratação de docente"),
    _wholeword_pattern("contratacao de docente"),
    _wholeword_pattern("contratação de docentes"),
    _wholeword_pattern("contratacao de docentes"),
    _wholeword_pattern("admissão de professor"),
    _wholeword_pattern("admissao de professor"),
    _wholeword_pattern("nomeação de professor"),
    _wholeword_pattern("nomeacao de professor"),
    _wholeword_pattern("designação de professor"),
    _wholeword_pattern("designacao de professor"),
    _wholeword_pattern("professor substituto"),
    _wholeword_pattern("professor temporário"),
    _wholeword_pattern("professor temporario"),
    _wholeword_pattern("processo seletivo simplificado"),
    re.compile(r"\bPSS\b", re.I),
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


def _has_any(text_norm: str, patterns) -> bool:
    return any(p and p.search(text_norm) for p in patterns)


def _is_blocked(text: str) -> bool:
    if not text:
        return False
    nt = _normalize_ws(text)

    # bloqueios diretos
    for pat in EXCLUDE_PATTERNS:
        if pat and pat.search(nt):
            return True

    # regra do seu código base: CNE + CES juntos
    if _has_any(nt, _CNE_PATTERNS) and _has_any(nt, _CES_PATTERNS):
        return True

    return False


_BEBIDAS_EXCLUDE_TERMS = [
    "ato declaratorio executivo",
    "registro especial",
    "declara a inscricao", "concede o registro",
    "drf", "srrf", "defis", "efi2vit", "regesp",
    "delegacia da receita federal",
    "cnpj", "ncm", "mapa",
    "engarrafador", "produtor", "importador",
    "marcas comerciais", "atualiza as marcas"
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
    "monitoramento"
]


def _is_bebidas_ato_irrelevante(texto_bruto: str) -> bool:
    nt = _normalize_ws(texto_bruto)
    if any(t in nt for t in _BEBIDAS_WHITELIST_TERMS):
        return False
    if any(t in nt for t in _BEBIDAS_EXCLUDE_TERMS):
        return True
    return False


CONTEUDO_MAX = int(os.getenv("DOU_CONTEUDO_MAX", "49500"))
_CONTENT_CACHE = {}

_HDR = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
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
            textos.append("\n\n".join(ps) if len(ps) >= 2 else el.get_text(" ", strip=True))

        txt = max(textos, key=len) if textos else soup.get_text(" ", strip=True)
        txt = re.sub(r"[ \t]+", " ", txt).strip()
        if CONTEUDO_MAX and len(txt) > CONTEUDO_MAX:
            txt = txt[:CONTEUDO_MAX] + "…"
        _CONTENT_CACHE[url] = txt
        return txt
    except Exception:
        return ""


def raspa_dou_extra(data=None, secoes=None):
    if data is None:
        data = datetime.now().strftime("%d-%m-%Y")
    if secoes is None:
        secoes = [
            s.strip()
            for s in (os.getenv("DOU_EXTRA_SECOES") or "DO1E,DO2E,DO3E").split(",")
            if s.strip()
        ]

    print(f"Raspando edição EXTRA do dia {data} nas seções: {', '.join(secoes)}…")
    combined = {"jsonArray": []}

    for sec in secoes:
        try:
            url = f"http://www.in.gov.br/leiturajornal?data={data}&secao={sec}"
            page = requests.get(url, timeout=40, headers=_HDR)
            if page.status_code != 200:
                continue

            soup = BeautifulSoup(page.text, "html.parser")
            params = soup.find("script", {"id": "params"})
            if not params:
                continue

            j = json.loads(params.text)
            arr = j.get("jsonArray", [])
            if arr:
                for it in arr:
                    if isinstance(it, dict) and "secao_extra" not in it:
                        it["secao_extra"] = sec
                combined["jsonArray"].extend(arr)
        except Exception:
            continue

    if combined["jsonArray"]:
        print(f"OK, coletadas {len(combined['jsonArray'])} entradas (extra).")
        return combined

    print("Nenhum item encontrado na(s) seção(ões) extra.")
    return None


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
    "OBMEP", "Olimpíada Brasileira de Matemática das Escolas Públicas", "PNLD Matemática"
]
_PATTERNS_GERAL = [(kw, _wholeword_pattern(kw)) for kw in PALAVRAS_GERAIS]

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
Umane|Saúde|sus; sistema único de saúde; atenção primária à saúde; aps; vigilância epidemiológica; planos de saúde; caps; equidade em saúde; populações vulneráveis; desigualdades sociais; organização do sus; políticas públicas em saúde; governança do sus; regionalização em saúde; população negra em saúde; saúde indígena; povos originários; saúde da pessoa idosa; envelhecimento ativo; atenção primária; saúde da criança; saúde do adolescente; saúde da mulher; saúde do homem; saúde da pessoa com deficiência; saúde da população lgbtqia+; financiamento da saúde; emendas e orçamento da saúde; emendas parlamentares; ministério da saúde; trabalhadores e profissionais de saúde; força de trabalho em saúde; política de recursos humanos em saúde; cuidados primários em saúde; emergências climáticas e ambientais em saúde; emergências climáticas; mudanças ambientais; adaptação climática; saúde ambiental; políticas climáticas; vigilância em saúde; epidemiológica; emergência em saúde; estado de emergência; saúde suplementar; seguradoras; planos populares; anvisa; ans; sandbox regulatório; cartões e administradoras de benefícios em saúde; conass; conasems
Cactus|Saúde|saúde mental; saúde mental para meninas; saúde mental para juventude; saúde mental para mulheres; pse; eca; rede de atenção psicossocial; raps; caps; centro de apoio psicossocial; programa saúde na escola; bullying; cyberbullying; eca digital
Vital Strategies|Saúde|saúde mental; dados para a saúde; morte evitável; doenças crônicas não transmissíveis; rotulagem de bebidas alcoólicas; educação em saúde; bebidas alcoólicas; imposto seletivo; dcnts; rotulagem de alimentos; alimentos ultraprocessados; publicidade infantil; publicidade de alimentos ultraprocessados; tributação de bebidas alcoólicas; alíquota de bebidas alcoólicas; cigarro eletrônico; controle de tabaco; violência doméstica; exposição a fatores de risco; departamento de saúde mental; hipertensão arterial; saúde digital; violência contra crianças; violência contra mulheres; feminicídio; cop 30
Coletivo Feminista|Direitos reprodutivos|aborto; nascituro; gestação acima de 22 semanas; interrupção legal da gestação; interrupção da gestação; resolução 258 conanda; vida por nascer; vida desde a concepção; criança por nascer; infanticídio; feticídio; assistolia fetal; medicamento abortivo; misoprostol; citotec; cytotec; mifepristona; ventre; assassinato de bebês; luto parental; síndrome pós aborto
IDEC|Saúde|defesa do consumidor; ação civil pública; sac; reforma tributária; ultraprocessados; doenças crônicas não transmissíveis; dcnts; obesidade; codex alimentarius; gordura trans; adoçantes; edulcorantes; rotulagem de alimentos; transgênicos; organismos geneticamente modificados; (ogms); marketing e publicidade de alimentos; comunicação mercadológica; escolas e alimentação escolar; bebidas açucaradas; refrigerante; programa nacional de alimentação escolar; pnae; educação alimentar e nutricional; ean; agrotóxicos; pesticidas; defensivos fitossanitários; tributação de alimentos não saudáveis; desertos alimentares; desperdício de alimentos; segurança alimentar e nutricional; san; direito humano à alimentação; fome; sustentabilidade; mudança climática; plástico; gestão de resíduos; economia circular; desmatamento; greenwashing; energia elétrica; encargos tarifários; subsídios na tarifa de energia; descontos na tarifa de energia; energia pré-paga; abertura do mercado de energia para consumidor cativo; mercado livre de energia; qualidade do serviço de energia; serviço de energia; tarifa social de energia elétrica; geração térmica; combustíveis fósseis; transição energética; descarbonização da matriz elétrica; descarbonização; gases de efeito estufa; acordo de paris; objetivos do desenvolvimento sustentável; reestruturação do setor de energia; reforma do setor elétrico; modernização do setor elétrico; itens de custo da tarifa de energia elétrica; universalização do acesso à energia; eficiência energética; geração distribuída; carvão mineral; painel solar; crédito imobiliário; crédito consignado; publicidade de crédito; cartão de crédito; pagamento de fatura; parcelamento com e sem juros; cartões pré-pagos; programas de fidelidade; cheque especial; taxa de juros; contrato de crédito; endividamento de jovens; crédito estudantil; endividamento de idosos; crédito por meio de aplicativos; abertura e movimentação de conta bancária; cobrança de serviços sem autorização; cadastro positivo; contratação de serviços bancários com imposição de seguros e títulos de capitalização; acessibilidade aos canais de serviços bancários; serviços bancários; caixa eletrônico; internet banking; aplicativos móveis; contratação de pacotes de contas bancárias; acesso à informação em caso de negativa de crédito; plano de saúde; saúde suplementar; medicamentos isentos de prescrição; mip; medicamentos antibióticos; antimicrobianos; propriedade intelectual; patentes; licença compulsória; preços de medicamentos; complexo econômico-industrial da saúde; saúde digital; prontuário eletrônico; rede nacional de dados em saúde; rnds; datasus; proteção de dados pessoais; telessaúde; telecomunicações; internet; tv por assinatura; serviço de acesso condicionado (seac); telefonia móvel; telefonia fixa; tv digital; lei geral de proteção de dados (lgpd); autoridade nacional de proteção de dados (anpd); reconhecimento facial; lei geral de telecomunicações; bens reversíveis; fundo de universalização dos serviços de telecomunicações (fust); provedores de acesso; franquia de internet; marco civil da internet; neutralidade de rede; zero rating; privacidade; lei de acesso à informação; regulação de plataformas digitais; desinformação; fake news; dados biométricos; vazamento de dados; telemarketing; serviço de valor adicionado
""".strip()


def _parse_client_keywords(text: str):
    out = {}
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

CLIENT_PATTERNS = []
for cli, kws in CLIENT_KEYWORDS.items():
    for kw in kws:
        pat = _wholeword_pattern(kw)
        if pat:
            CLIENT_PATTERNS.append((pat, cli, kw))


def procura_termos_geral(conteudo_raspado):
    if conteudo_raspado is None or "jsonArray" not in conteudo_raspado:
        print("Nenhum conteúdo para analisar (geral extra).")
        return None

    print("Buscando palavras-chave (geral, whole-word, título+resumo)…")
    URL_BASE = "https://www.in.gov.br/en/web/dou/-/"
    resultados_por_palavra = {kw: [] for kw in PALAVRAS_GERAIS}
    algum = False

    for r in conteudo_raspado["jsonArray"]:
        titulo = r.get("title", "Título não disponível")
        resumo = r.get("content", "")
        link = URL_BASE + r.get("urlTitle", "")
        data_pub = (r.get("pubDate", "") or "")[:10]
        secao_extra = r.get("secao_extra", "")

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
                if _is_blocked(conteudo_pagina):
                    continue

                resultados_por_palavra[palavra].append({
                    "date": data_pub,
                    "secao_extra": secao_extra,
                    "title": titulo,
                    "href": link,
                    "abstract": resumo,
                    "content_page": conteudo_pagina or ""
                })
                algum = True

    if not algum:
        print("Nenhum resultado encontrado (geral extra).")
        return None

    print("Palavras-chave (geral extra) encontradas.")
    return resultados_por_palavra


def procura_termos_clientes(conteudo_raspado):
    if conteudo_raspado is None or "jsonArray" not in conteudo_raspado:
        print("Nenhum conteúdo para analisar (clientes extra).")
        return {}

    print("Buscando palavras-chave por cliente (whole-word, título+resumo)…")
    URL_BASE = "https://www.in.gov.br/en/web/dou/-/"

    agreg = {}

    for r in conteudo_raspado["jsonArray"]:
        titulo = r.get("title", "Título não disponível")
        resumo = r.get("content", "")
        link = URL_BASE + r.get("urlTitle", "")
        data_pub = (r.get("pubDate", "") or "")[:10]
        secao_extra = r.get("secao_extra", "")

        if _is_blocked(titulo + " " + resumo):
            continue

        texto_norm = _normalize_ws(titulo + " " + resumo)
        conteudo_pagina = None

        for pat, cliente, kw in CLIENT_PATTERNS:
            if not pat.search(texto_norm):
                continue

            if kw.strip().lower() == "bebidas alcoólicas":
                if conteudo_pagina is None:
                    conteudo_pagina = _baixar_conteudo_pagina(link)
                alltxt = f"{titulo}\n{resumo}\n{conteudo_pagina or ''}"
                if _is_bebidas_ato_irrelevante(alltxt):
                    continue

            if conteudo_pagina is None:
                conteudo_pagina = _baixar_conteudo_pagina(link)
            if _is_blocked(conteudo_pagina):
                continue

            k = (cliente, link)
            if k not in agreg:
                agreg[k] = {
                    "date": data_pub,
                    "cliente": cliente,
                    "title": titulo,
                    "href": link,
                    "abstract": resumo,
                    "content_page": conteudo_pagina or "",
                    "secao_extra": secao_extra,
                    "kws": set()
                }
            agreg[k]["kws"].add(kw)

    por_cliente = {c: [] for c in CLIENT_KEYWORDS.keys()}

    for (cliente, _href), d in agreg.items():
        kws_join = "; ".join(sorted(d["kws"], key=lambda x: x.lower()))
        por_cliente[cliente].append([
            d["date"],
            d["cliente"],
            kws_join,
            d["title"],
            d["href"],
            d["abstract"],
            d["content_page"],
            "",
            "",
            d["secao_extra"],
        ])

    return por_cliente


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
        raise RuntimeError("Secret GOOGLE_APPLICATION_CREDENTIALS_JSON não encontrado e credentials.json não existe.")

    info = json.loads(raw)
    if "private_key" in info and "\\n" in info["private_key"]:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


COLS_GERAL = ["Data", "Palavra-chave", "Portaria", "Link", "Resumo", "Conteúdo", "Seção"]
COLS_CLIENTE = ["Data", "Cliente", "Palavra-chave", "Portaria", "Link", "Resumo", "Conteúdo", "Alinhamento", "Justificativa", "Seção"]


def _ensure_header(ws, header):
    first = ws.row_values(1)
    if first != header:
        ws.resize(rows=max(2, ws.row_count), cols=len(header))
        ws.update("1:1", [header])


def _ws_gid(ws) -> str:
    try:
        return str(ws.id)
    except Exception:
        return ""


def salva_geral_dedupe(palavras_raspadas):
    if not palavras_raspadas:
        print("Sem palavras encontradas para salvar (geral extra).")
        return 0, [], None, None

    gc = _gs_client_from_env()
    planilha_id = os.getenv("PLANILHA")
    if not planilha_id:
        raise RuntimeError("Env PLANILHA não definido (use apenas a key entre /d/ e /edit).")

    sh = gc.open_by_key(planilha_id)

    try:
        ws = sh.worksheet("Página1")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Página1", rows="2000", cols=str(len(COLS_GERAL)))

    _ensure_header(ws, COLS_GERAL)

    link_idx = COLS_GERAL.index("Link")
    palavra_idx = COLS_GERAL.index("Palavra-chave")

    all_vals = ws.get_all_values()
    existing = set()
    if len(all_vals) > 1:
        for r in all_vals[1:]:
            if len(r) > link_idx:
                existing.add((r[link_idx].strip(), r[palavra_idx].strip() if len(r) > palavra_idx else ""))

    rows_to_insert = []
    inserted_items = []

    for palavra, lista in (palavras_raspadas or {}).items():
        for item in lista:
            href = (item.get("href", "") or "").strip()
            key = (href, palavra)
            if not href or key in existing:
                continue

            row = [
                item.get("date", ""),
                palavra,
                item.get("title", ""),
                href,
                item.get("abstract", ""),
                item.get("content_page", ""),
                item.get("secao_extra", ""),
            ]
            rows_to_insert.append(row)
            inserted_items.append({
                "date": item.get("date", ""),
                "secao_extra": item.get("secao_extra", ""),
                "keyword": palavra,
                "title": item.get("title", ""),
                "href": href,
                "abstract": item.get("abstract", "")
            })
            existing.add(key)

    if rows_to_insert:
        ws.insert_rows(rows_to_insert, row=2, value_input_option="USER_ENTERED")
        print(f"{len(rows_to_insert)} linhas adicionadas (geral extra).")
    else:
        print("Nenhuma linha nova (geral extra).")

    return len(rows_to_insert), inserted_items, sh, ws


def _append_dedupe_por_cliente(sh, sheet_name: str, rows):
    if not rows:
        return 0, []

    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=str(max(100, len(rows) + 10)), cols=len(COLS_CLIENTE))

    _ensure_header(ws, COLS_CLIENTE)

    link_idx = COLS_CLIENTE.index("Link")
    cliente_idx = COLS_CLIENTE.index("Cliente")

    all_vals = ws.get_all_values()
    existing = set()
    if len(all_vals) > 1:
        for r in all_vals[1:]:
            if len(r) > link_idx:
                href = r[link_idx].strip()
                cli = r[cliente_idx].strip() if len(r) > cliente_idx else ""
                if href and cli:
                    existing.add((href, cli))

    new_rows = []
    inserted_items = []

    for r in rows:
        if len(r) <= link_idx:
            continue
        href = (r[link_idx] or "").strip()
        cli = (r[cliente_idx] or "").strip()
        if not href or not cli:
            continue

        key = (href, cli)
        if key in existing:
            continue

        new_rows.append(r)
        inserted_items.append({
            "date": r[0],
            "cliente": cli,
            "keyword": r[2],
            "title": r[3],
            "href": href,
            "abstract": r[5]
        })
        existing.add(key)

    if not new_rows:
        return 0, []

    ws.insert_rows(new_rows, row=2, value_input_option="USER_ENTERED")
    return len(new_rows), inserted_items


def salva_por_cliente(por_cliente):
    plan_id = os.getenv("PLANILHA_CLIENTES")
    if not plan_id:
        print("PLANILHA_CLIENTES não definido; pulando saída por cliente (extra).")
        return 0, {}, None, {}

    gc = _gs_client_from_env()
    sh = gc.open_by_key(plan_id)

    gids = {}
    for cli in CLIENT_KEYWORDS.keys():
        try:
            ws = sh.worksheet(cli)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=cli, rows="2", cols=len(COLS_CLIENTE))
        _ensure_header(ws, COLS_CLIENTE)
        gids[cli] = _ws_gid(ws)

    total_new = 0
    inserted_map = {}

    for cli, rows in (por_cliente or {}).items():
        n, inserted_items = _append_dedupe_por_cliente(sh, cli, rows)
        if n > 0:
            total_new += n
            inserted_map[cli] = inserted_items

    return total_new, inserted_map, sh, gids


EMAIL_RE = re.compile(r'<?("?)([^"\s<>@]+@[^"\s<>@]+\.[^"\s<>@]+)\1>?$')


def _sanitize_emails(raw_list: str):
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
        if re.match(r"^[^@\s]+@[^@\s<>@]+\.[^@\s<>@]+$", candidate) and candidate.lower() not in seen:
            seen.add(candidate.lower())
            emails.append(candidate.lower())
    return emails


def _gs_tab_url(sheet_id: str, gid: str | None):
    if not sheet_id:
        return ""
    if not gid:
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"


def _unique_item_count(inserted_general, inserted_clients_map) -> int:
    hrefs = set()
    for it in (inserted_general or []):
        h = (it.get("href", "") or "").strip()
        if h:
            hrefs.add(h)
    for _cli, lst in (inserted_clients_map or {}).items():
        for it in (lst or []):
            h = (it.get("href", "") or "").strip()
            if h:
                hrefs.add(h)
    return len(hrefs)


def _split_kws_cell(cell: str):
    if not cell:
        return []
    parts = [p.strip() for p in cell.split(";")]
    return [p for p in parts if p]


def _build_email_minimo_html(
    inserted_general,
    inserted_clients_map,
    planilha_id,
    planilha_gid,
    planilha_clientes_id,
):
    qtd_geral = len(inserted_general or [])
    qtd_clientes = sum(len(v) for v in (inserted_clients_map or {}).values())
    itens_novos = _unique_item_count(inserted_general, inserted_clients_map)

    if itens_novos <= 0 and qtd_geral <= 0 and qtd_clientes <= 0:
        return ""

    kw_to_general = {}
    kw_to_clients = {}

    for it in (inserted_general or []):
        kw = (it.get("keyword", "") or "").strip()
        if kw:
            kw_to_general[kw] = True

    for cli, lst in (inserted_clients_map or {}).items():
        for it in (lst or []):
            raw_kw = (it.get("keyword", "") or "").strip()
            for kw in _split_kws_cell(raw_kw):
                kw_to_clients.setdefault(kw, set()).add(cli)

    all_kws = sorted(set(list(kw_to_general.keys()) + list(kw_to_clients.keys())), key=lambda x: x.lower())
    palavras_acionadas = len(all_kws)

    clientes_afetados = sorted(set(inserted_clients_map.keys()), key=lambda x: x.lower())
    clientes_txt = f" ({', '.join(clientes_afetados)})" if clientes_afetados else ""

    geral_url = _gs_tab_url(planilha_id, planilha_gid)
    clientes_url = _gs_tab_url(planilha_clientes_id, None)

    lines = []
    for kw in all_kws:
        in_g = kw_to_general.get(kw, False)
        clients = sorted(list(kw_to_clients.get(kw, set())), key=lambda x: x.lower())

        if in_g and clients:
            destino = f"Geral + Cliente: {', '.join(clients)}"
        elif in_g:
            destino = "Geral"
        elif clients:
            destino = f"Cliente: {', '.join(clients)}"
        else:
            destino = "—"

        lines.append(f"<li><b>{html.escape(kw)}</b> → {html.escape(destino)}</li>")

    acionamentos_html = (
        "<ul style='margin:8px 0 0 18px; padding:0;'>" + "".join(lines) + "</ul>"
        if lines else
        "<p style='margin:8px 0 0 0;'>—</p>"
    )

    body = f"""
    <html>
      <body style="font-family: Arial, Helvetica, sans-serif; color:#111; line-height:1.35;">
        <div style="max-width:820px;">
          <h2 style="margin:0 0 6px 0;">Edição Extra do DOU</h2>
          <p style="margin:0 0 14px 0;">Atualização automática baseada em palavras-chave monitoradas.</p>

          <div style="padding:12px; border:1px solid #e5e7eb; border-radius:10px; margin:0 0 12px 0;">
            <div>Itens novos: <b>{itens_novos}</b></div>
            <div>Entradas na planilha geral: <b>{qtd_geral}</b></div>
            <div>Entradas por cliente: <b>{qtd_clientes}</b>{html.escape(clientes_txt) if clientes_txt else ""}</div>
            <div>Palavras-chave acionadas: <b>{palavras_acionadas}</b></div>

            <div style="margin-top:10px;">
              <a href="{html.escape(geral_url)}" target="_blank"
                 style="display:inline-block; padding:8px 10px; border:1px solid #111; border-radius:8px; text-decoration:none; margin-right:8px;">
                 Abrir planilha geral
              </a>
              <a href="{html.escape(clientes_url)}" target="_blank"
                 style="display:inline-block; padding:8px 10px; border:1px solid #111; border-radius:8px; text-decoration:none;">
                 Ver abas por cliente
              </a>
            </div>
          </div>

          <div style="padding:12px; border:1px solid #e5e7eb; border-radius:10px;">
            <div style="margin:0 0 6px 0;"><b>Acionamentos (palavra-chave → destino)</b></div>
            {acionamentos_html}
          </div>
        </div>
      </body>
    </html>
    """
    return body


def envia_email_brevo_extra_minimo(
    inserted_general,
    inserted_clients_map,
    planilha_id,
    planilha_gid,
    planilha_clientes_id,
):
    qtd_geral = len(inserted_general or [])
    qtd_clientes = sum(len(v) for v in (inserted_clients_map or {}).values())
    itens_novos = _unique_item_count(inserted_general, inserted_clients_map)

    if itens_novos <= 0 and qtd_geral <= 0 and qtd_clientes <= 0:
        print("Nada novo — e-mail (extra) não será enviado.")
        return

    api_key = os.getenv("BREVO_API_KEY")
    sender_email = os.getenv("EMAIL")
    raw_dest = os.getenv("DESTINATARIOS", "")
    if not (api_key and sender_email and raw_dest):
        print("Dados de e-mail incompletos; pulando envio (extra).")
        return

    destinatarios = _sanitize_emails(raw_dest)
    hoje = datetime.now().strftime("%d-%m-%Y")
    subject = f"DOU EXTRA — {hoje} | {itens_novos} itens novos"

    html_body = _build_email_minimo_html(
        inserted_general=inserted_general or [],
        inserted_clients_map=inserted_clients_map or {},
        planilha_id=planilha_id,
        planilha_gid=planilha_gid,
        planilha_clientes_id=planilha_clientes_id,
    )

    if not html_body:
        print("HTML vazio — pulando envio (extra).")
        return

    cfg = Configuration()
    cfg.api_key["api-key"] = api_key
    api = TransactionalEmailsApi(ApiClient(configuration=cfg))

    for dest in destinatarios:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}],
                sender={"email": sender_email},
                subject=subject,
                html_content=html_body
            ))
            print(f"✅ [DOU EXTRA] E-mail enviado para {dest}")
        except (ApiException, Exception) as e:
            print(f"❌ Falha ao enviar (extra) para {dest}: {e}")


if __name__ == "__main__":
    conteudo = raspa_dou_extra()

    geral = procura_termos_geral(conteudo)
    _qtd_geral, inserted_general, _sh_geral, ws_geral = salva_geral_dedupe(geral)

    por_cliente = procura_termos_clientes(conteudo)
    _qtd_clientes, inserted_clients_map, _sh_cli, _client_gids = salva_por_cliente(por_cliente)

    planilha_id = os.getenv("PLANILHA") or ""
    planilha_clientes_id = os.getenv("PLANILHA_CLIENTES") or ""
    planilha_gid = _ws_gid(ws_geral) if ws_geral else None

    envia_email_brevo_extra_minimo(
        inserted_general=inserted_general,
        inserted_clients_map=inserted_clients_map,
        planilha_id=planilha_id,
        planilha_gid=planilha_gid,
        planilha_clientes_id=planilha_clientes_id,
    )
