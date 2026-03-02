import os, re, json, time
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from google import genai
from string import Template

# CONFIG
GENAI_API_KEY     = os.getenv("GENAI_API_KEY", "")
MODEL_NAME        = "gemini-2.5-flash"
PLANILHA_CLIENTES = os.getenv("PLANILHA_CLIENTES")
SKIP_SHEETS       = {"Giro de notícias"}

# Colunas esperadas
COL_DATA     = "Data"
COL_CLIENTE  = "Cliente"
COL_PALAVRA  = "Palavra-chave"
COL_PORTARIA = "Portaria"
COL_LINK     = "Link"
COL_RESUMO   = "Resumo"
COL_CONTEUDO = "Conteúdo"
COL_ALINH    = "Alinhamento"
COL_JUST     = "Justificativa"
COL_SECAO    = "Seção"

COLS_CANONICAL = [
    COL_DATA, COL_CLIENTE, COL_PALAVRA, COL_PORTARIA,
    COL_LINK, COL_RESUMO, COL_CONTEUDO,
    COL_ALINH, COL_JUST, COL_SECAO,
]

BATCH_SIZE = int(os.getenv("ALIGN_BATCH", "25"))
SLEEP_SEC  = float(os.getenv("ALIGN_SLEEP", "0.10"))

# MAPA: ABA -> (NOME, DESCRIÇÃO)
CLIENTE_DESCRICOES = {
    "IU": ("Instituto Unibanco (IU)",
           "O Instituto Unibanco (IU) é uma organização sem fins lucrativos que atua no fortalecimento da gestão educacional, desenvolvendo projetos como o Jovem de Futuro, oferecendo apoio técnico a secretarias estaduais de educação e produzindo conhecimento para aprimorar políticas públicas. Seu foco está tanto no cenário federal, acompanhando os debates sobre o financiamento da educação, programas nacionais de educação, regulação educacional e diretrizes definidas por órgãos como o Conselho Nacional de Educação, quanto subnacional, olhando para 6 estados prioritários (RS, MG, ES, CE, PI e GO). O IU apoia iniciativas de recomposição de aprendizagens, infraestrutura escolar, inclusão digital, educação ambiental, mudanças do clima e valorização de profissionais da educação."),
    "FMCSV": ("Fundação Maria Cecilia Souto Vidigal (FMCSV)",
              "A Fundação Maria Cecilia Souto Vidigal (FMCSV) é uma organização da sociedade civil dedicada ao fortalecimento da primeira infância no Brasil. Sua atuação concentra-se na integração entre produção de conhecimento, advocacy e apoio à formulação e implementação de políticas públicas, com o objetivo de assegurar o desenvolvimento integral de crianças de 0 a 6 anos. A Fundação acompanha o debate sobre educação domiciliar (homeschooling), posicionando-se de forma contrária a avanços nessa pauta. Além disso, participa ativamente da construção e implementação da Política Nacional Integrada da Primeira Infância. Entre suas iniciativas, destaca-se o programa 'Primeira Infância Primeiro', que disponibiliza dados, evidências e ferramentas para gestores públicos e candidatos, contribuindo para a qualificação do debate e das políticas voltadas à infância."),
    "IEPS": ("Instituto de Estudos para Políticas de Saúde (IEPS)",
             "O Instituto de Estudos para Políticas de Saúde (IEPS) é uma organização independente e sem fins lucrativos dedicada a aprimorar políticas de saúde no Brasil, combinando pesquisa aplicada, produção de evidências e advocacy em temas como atenção primária, saúde digital e financiamento do SUS. Com especialização em políticas públicas de saúde, o IEPS possui uma atuação centrada no fortalecimento do SUS enquanto sistema universal e equitativo. Seus núcleos de observação abrangem: a organização e governança federativa do SUS (modelo tripartite, emendas parlamentares); estruturação da APS, financiamento per capita e regionalização; equidade e enfrentamento de desigualdades; força de trabalho em saúde; saúde mental e Rede de Atenção Psicossocial; regulação da Anvisa e ANS; saúde suplementar; emergências sanitárias; e impactos das mudanças climáticas na saúde. As BETs são destaque no monitoramento para 2026, especialmente na incidência junto à Frente Parlamentar Mista para a Promoção da Saúde Mental."),
    "IAS": ("Instituto Ayrton Senna (IAS)",
            "O Instituto Ayrton Senna (IAS) é um centro de inovação em educação que atua em pesquisa e desenvolvimento, disseminação em larga escala e influência em políticas públicas, com foco em aprendizagem acadêmica e competências socioemocionais na rede pública."),
    "ISG": ("Instituto Sonho Grande (ISG)",
            "O Instituto Sonho Grande (ISG) é uma organização sem fins lucrativos e apartidária voltada à expansão e qualificação do ensino médio integral em redes públicas. Atua em parceria com secretarias estaduais de educação, oferecendo apoio na revisão curricular, na formação de equipes escolares e na implementação de modelos de gestão orientados a resultados. Além disso, o Instituto mantém foco no fortalecimento da infraestrutura das escolas públicas de tempo integral e na promoção de políticas voltadas à alfabetização. Também acompanha o debate sobre educação domiciliar (homeschooling), posicionando-se de forma contrária à sua ampliação."),
    "Reúna": ("Instituto Reúna",
              "O Instituto Reúna desenvolve pesquisas e ferramentas para apoiar redes e escolas na implementação de políticas educacionais alinhadas à BNCC, com foco em currículo, materiais de apoio e formação de professores."),
    "Reuna": ("Instituto Reúna",
              "O Instituto Reúna desenvolve pesquisas e ferramentas para apoiar redes e escolas na implementação de políticas educacionais alinhadas à BNCC, com foco em currículo, materiais de apoio e formação de professores."),
    "REMS": ("REMS – Rede Esporte pela Mudança Social",
             "A REMS – Rede Esporte pela Mudança Social articula organizações que usam o esporte como vetor de desenvolvimento humano, mobilizando atores e produzindo conhecimento para ampliar o impacto social dessa agenda no país. A rede atua desde 2007 no fortalecimento do campo do esporte para o desenvolvimento social, promovendo a troca de experiências, a sistematização de práticas e a realização de agendas coletivas, bem como acompanhando e incidindo em debates sobre políticas públicas, financiamento, marcos regulatórios e programas governamentais relacionados ao esporte educacional, comunitário e de participação. Sua atuação abrange o nível federal, com articulação da pauta esportiva com áreas como educação, assistência social, saúde e desenvolvimento territorial."),
    "Manual": ("Manual (saúde)",
               "A Manual (saúde) é uma plataforma digital voltada principalmente à saúde e bem-estar masculino, oferecendo atendimento online e tratamentos baseados em evidências (como saúde capilar, sono e saúde sexual), com prescrição médica e acompanhamento remoto. Possui atuação aprofundada em emagrecimento (foco em GLP-1 e redutores de apetite), disfunção erétil (consultas médicas e medicamentos manipulados) e queda capilar (Finasterida e Minoxidil). Tem interesse em promover inovação na área de saúde, principalmente em relação a manipuláveis e na expansão da telemedicina."),
    "Cactus": ("Instituto Cactus",
               "O Instituto Cactus é uma organização filantrópica e de direitos humanos, sem fins lucrativos e independente, que atua para ampliar e qualificar o ecossistema da saúde mental no Brasil, com foco prioritário em mulheres e adolescentes. Sua atuação organiza-se em duas frentes: o fomento estratégico (grant-making), financiando e co-criando iniciativas em saúde mental e produzindo evidências; e o advocacy, com foco na formulação, implementação e avaliação de políticas públicas e análise de projetos de lei. O Instituto também desenvolve ferramentas de apoio a gestores e promove ações de educação e mobilização social para reduzir o estigma em torno da saúde mental."),
    "Vital Strategies": ("Vital Strategies",
                         "A Vital Strategies é uma organização global de saúde pública que trabalha com governos e sociedade civil na concepção e implementação de políticas baseadas em evidências em áreas como doenças crônicas, segurança viária, qualidade do ar, dados vitais e comunicação de risco. Desde o ano passado tem focado na Reforma Tributária, em especial no Imposto Seletivo, buscando incidir sobre a alíquota em bebidas açucaradas, álcool e tabaco. Também trata do cuidado no trânsito relacionado ao uso de drogas, políticas de vedação de marketing de cigarros, dispositivos eletrônicos, ultraprocessados e bebidas alcoólicas. Na área tecnológica, investe em estudos que ligam Inteligência Artificial à jornada do paciente, com foco no combate ao feminicídio e no diagnóstico precoce de câncer. Acompanha ainda temas de saúde ambiental, qualidade do ar e intoxicação por chumbo."),
    "Mevo": ("Mevo",
             "A Mevo é uma healthtech brasileira que integra soluções de saúde digital, da prescrição eletrônica à compra e entrega de medicamentos, conectando médicos, hospitais, farmácias e pacientes para tornar o cuidado mais simples, eficiente e rastreável. Seu foco está na construção de um ecossistema digital interoperável, atuando junto aos Poderes Legislativo e Executivo para contribuir com o fortalecimento de uma Rede Nacional de Dados em Saúde (RNDS) robusta e integrada. Também mantém diálogo com agências reguladoras, acompanhando debates sobre saúde digital, interoperabilidade de sistemas, proteção de dados e normativas que impactem suas soluções tecnológicas."),
    "Coletivo Feminista": ("Coletivo Feminista",
                           "O Coletivo Feminista (Nem Presa Nem Morta) é um movimento feminista que atua pela descriminalização e legalização do aborto no Brasil, articulando pesquisa, incidência política e mobilização social. Seus princípios ético-políticos abrangem a comunicação como direito e fundamento da democracia, a defesa do Estado democrático de direito, a compreensão de que maternidade não é dever e deve respeitar a liberdade de escolha, a promoção de uma atenção universal, equânime e integral à saúde — com ênfase no papel do SUS, no acesso a métodos contraceptivos e abortivos seguros e no respeito à autodeterminação reprodutiva —, além da defesa da descriminalização e legalização do aborto. Desde o final de 2024, o coletivo tem focado no novo Código Civil e no PLD3/2025, que susta a resolução 258 do CONANDA, atuando para evitar regressões inconstitucionais ligadas ao aborto, em especial quando se trata de crianças e adolescentes."),
    "IDEC": ("Instituto Brasileiro de Defesa do Consumidor (Idec)",
             "O Instituto Brasileiro de Defesa do Consumidor (Idec) é uma associação civil sem fins lucrativos e independente de empresas, partidos ou governos, fundada em 1987. Atua na defesa dos direitos dos consumidores e na promoção de relações de consumo éticas, seguras e sustentáveis, combinando advocacy, pesquisa e litigância estratégica, com foco em temas como saúde, alimentação, energia, telecomunicações e direitos digitais. Destaca-se na incidência em políticas públicas relacionadas à alimentação saudável, controle de ultraprocessados e agrotóxicos, rotulagem nutricional, transição energética justa e regulação de plataformas digitais. Também acompanha a regulação dos planos de saúde junto à ANS, a saúde digital do ponto de vista do consumidor, greenwashing, práticas abusivas de telemarketing e debates como o ReData."),
    "Umane": ("Umane",
              "A Umane é uma organização da sociedade civil isenta, apartidária e sem fins lucrativos que atua para fomentar a saúde pública de forma sistêmica no Brasil, com foco em ampliar equidade, eficiência e qualidade do sistema de saúde. Opera por meio de fomento a projetos, articulação com parceiros e um modelo que combina monitoramento e avaliação, uso de dados e tecnologia (telessaúde e IA) e advocacy. Suas frentes programáticas incluem o fortalecimento da Atenção Primária à Saúde (APS), a atenção integral às Doenças Crônicas Não Transmissíveis (DCNT) — com foco em doenças cardiovasculares, diabetes tipo 2, obesidade, subnutrição e dislipidemias — e a saúde da mulher, da criança e do adolescente, com ênfase no pré-natal, nos primeiros mil dias e no enfrentamento da má nutrição infantil e juvenil."),
    "ASBAI": ("Associação Brasileira de Alergia e Imunologia (ASBAI)",
              "A Associação Brasileira de Alergia e Imunologia (ASBAI) é uma entidade científica sem fins lucrativos que reúne médicos especialistas em alergia e imunologia clínica no Brasil. Atua na promoção do ensino, pesquisa e atualização profissional, elaborando diretrizes clínicas, posicionamentos técnicos e recomendações para o diagnóstico e tratamento de doenças alérgicas e imunológicas. Acompanha debates regulatórios junto ao Ministério da Saúde (especialmente a Conitec) e à Anvisa, em temas como incorporação de tecnologias, imunobiológicos, vacinas e protocolos clínicos. Sua principal linha de atuação neste momento é a incorporação da caneta de adrenalina autoinjetável no SUS e a obrigatoriedade de notificação de ocorrências de anafilaxia ao Ministério da Saúde."),
    "Infinis": ("Instituto Futuro é Infância Saudável (Infinis)",
                "O Instituto Futuro é Infância Saudável (Infinis) é a frente de filantropia estratégica e advocacy da Fundação José Luiz Setúbal (FJLS). A organização atua com base em evidências científicas para promover políticas públicas, fortalecer a sociedade civil e impulsionar soluções que assegurem saúde e bem-estar na infância. Sua atuação está estruturada em quatro eixos temáticos: segurança alimentar e enfrentamento da má nutrição; saúde mental; prevenção às violências; e fortalecimento da sociedade civil, alinhados aos ODS da ONU. Com foco na incidência política, busca contribuir para o aprimoramento e a efetiva implementação de políticas públicas, além de fomentar a transformação de comportamentos e o desenvolvimento de soluções locais sustentáveis."),
}

CLIENTE_DESCRICOES.setdefault("Reuna", CLIENTE_DESCRICOES["Reúna"])

# PROMPT
PROMPT = Template(r"""
Você é analista de políticas públicas e faz triagem de atos do DOU e matérias legislativas para um(a) cliente.

Missão/escopo do cliente:
$descricao

Tarefa:
Classificar o alinhamento do **Conteúdo** com a missão do cliente.

Regras de evidência:
- Use **apenas** o Conteúdo. Não use contexto externo.
- NÃO exija que o Conteúdo cubra TODA a missão do cliente.
  # Se o Conteúdo estiver claramente dentro de ao menos UMA frente/eixo relevante do cliente, marque "Alinha".
  # A ausência de menção a outras frentes (ex.: socioemocional) NÃO reduz automaticamente para "Parcial".
- Use "Parcial" apenas quando houver INSUFICIÊNCIA ou AMBIGUIDADE no texto para decidir.
- Se o texto for claramente de natureza incompatível com triagem temática (ex.: decisão sobre caso individual sem política pública; deferimento/indeferimento nominal; concessão pontual; nomeação/dispensa rotineira sem tema; mero expediente administrativo sem objeto; publicação que não permite inferir assunto), marque "Não se aplica".

Classes (escolha exatamente UMA):
- "Alinha": O objeto/tema do Conteúdo é claro e há evidência explícita de relação com pelo menos 1 frente/eixo do cliente.
- "Parcial": O Conteúdo sugere relação, mas é genérico, incompleto ou não permite identificar com segurança o objeto/tema.
- "Não Alinha": O tema é claro e não tem relação com a missão do cliente.
- "Não se aplica": O Conteúdo não é classificável por tema/escopo do cliente com base no texto (exemplos acima), ou é predominantemente ato individual/procedimental sem política pública inferível.

Formato de saída:
Retorne **somente** JSON válido neste formato:
{
  "alinhamento": "Alinha" | "Parcial" | "Não Alinha" | "Não se aplica",
  "justificativa": "1–3 frases citando elementos do Conteúdo (termos/trechos) que sustentam a decisão"
}

Conteúdo:
\"\"\"$conteudo\"\"\"
""".strip())


def _gs_client():
    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if raw:
        info = json.loads(raw)
        if "private_key" in info and "\\n" in info["private_key"]:
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(
            "credentials.json",
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
    return gspread.authorize(creds)


genai_client = genai.Client(api_key=GENAI_API_KEY)


def classify_text(cliente_nome: str, conteudo: str) -> dict:
    if not str(conteudo or "").strip():
        return {"alinhamento": "Parcial", "justificativa": "Conteúdo ausente ou vazio; não é possível concluir."}

    desc = CLIENTE_DESCRICOES.get(
        cliente_nome,
        f"Organização '{cliente_nome}' com foco temático conforme sua atuação pública.",
    )

    prompt = PROMPT.substitute(cliente=cliente_nome, descricao=desc, conteudo=conteudo)

    stream = genai_client.models.generate_content_stream(
        model=MODEL_NAME,
        contents=prompt,
        config={"response_mime_type": "application/json"},
    )
    raw = "".join((ch.text or "") for ch in stream).strip()

    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return {"alinhamento": "Parcial", "justificativa": "Saída sem JSON válido; revisão manual sugerida."}

    try:
        data = json.loads(m.group(0))
        a = str(data.get("alinhamento", "")).strip()
        j = str(data.get("justificativa", "")).strip()
        if a not in ("Alinha", "Parcial", "Não Alinha", "Não se aplica"):
            a = "Parcial"
        if not j:
            j = "Sem justificativa; revisar."
        return {"alinhamento": a, "justificativa": j}
    except Exception:
        return {"alinhamento": "Parcial", "justificativa": "Falha ao interpretar JSON; revisão manual sugerida."}


def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que todas as colunas canônicas existam, inclusive Seção."""
    for col in COLS_CANONICAL:
        if col not in df.columns:
            df[col] = ""
    # Reordena para a ordem canônica, preservando colunas extras no final
    extras = [c for c in df.columns if c not in COLS_CANONICAL]
    df = df[COLS_CANONICAL + extras]
    return df


def pick_conteudo(row: pd.Series) -> str:
    txt = str(row.get(COL_CONTEUDO, "") or "").strip()
    if not txt:
        txt = str(row.get(COL_RESUMO, "") or "").strip()
    if not txt:
        txt = str(row.get(COL_PORTARIA, "") or "").strip()
    return txt


def process_sheet(ws) -> None:
    title = ws.title
    if title in SKIP_SHEETS:
        print(f"[{title}] pulada.")
        return

    values = ws.get_all_values()
    if not values:
        print(f"[{title}] vazia.")
        return

    header, rows = values[0], values[1:]
    df = pd.DataFrame(rows, columns=header)
    df = _ensure_cols(df)

    if COL_CONTEUDO not in df.columns:
        print(f"[{title}] coluna '{COL_CONTEUDO}' não encontrada — nada a fazer.")
        return

    mask = (df[COL_ALINH].astype(str).str.strip() == "") & (
        (df[COL_CONTEUDO].astype(str).str.strip() != "")
        | (df[COL_RESUMO].astype(str).str.strip() != "")
        | (df[COL_PORTARIA].astype(str).str.strip() != "")
    )
    idxs = list(df[mask].index)
    if not idxs:
        print(f"[{title}] nenhuma linha pendente.")
        return

    print(f"[{title}] classificando {len(idxs)} linha(s)...")

    for chunk_start in range(0, len(idxs), BATCH_SIZE):
        chunk = idxs[chunk_start:chunk_start + BATCH_SIZE]
        for i in chunk:
            conteudo = pick_conteudo(df.loc[i])
            res = classify_text(title, conteudo)
            df.at[i, COL_ALINH] = res["alinhamento"]
            df.at[i, COL_JUST] = res["justificativa"]
            if SLEEP_SEC:
                time.sleep(SLEEP_SEC)

        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=False)
        ultima = idxs[min(chunk_start + BATCH_SIZE - 1, len(idxs) - 1)] + 2
        print(f"[{title}] ✅ salvo até a linha {ultima}")


def main():
    if not PLANILHA_CLIENTES:
        raise SystemExit("Defina PLANILHA_CLIENTES (apenas a key).")

    gc = _gs_client()
    sh = gc.open_by_key(PLANILHA_CLIENTES)

    for ws in sh.worksheets():
        process_sheet(ws)


if __name__ == "__main__":
    main()
