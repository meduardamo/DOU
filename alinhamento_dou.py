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
SKIP_SHEETS       = {"Giro de notícias", "Mevo"}

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

CLIENTE_DESCRICOES = {
    "IU": (
        "Instituto Unibanco (IU)",
        "O Instituto Unibanco (IU) é uma organização sem fins lucrativos que atua no fortalecimento da gestão educacional, desenvolvendo projetos como o Jovem de Futuro, oferecendo apoio técnico a secretarias estaduais de educação e produzindo conhecimento para aprimorar políticas públicas. Seu foco está tanto no cenário federal, acompanhando os debates sobre o financiamento da educação, programas nacionais de educação, regulação educacional e diretrizes definidas por órgãos como o Conselho Nacional de Educação, quanto subnacional, olhando para 6 estados prioritários (RS, MG, ES, CE, PI e GO). O IU apoia iniciativas de recomposição de aprendizagens, infraestrutura escolar, inclusão digital, educação ambiental, mudanças do clima e valorização de profissionais da educação.",
    ),
    "FMCSV": (
        "Fundação Maria Cecilia Souto Vidigal (FMCSV)",
        "A Fundação Maria Cecilia Souto Vidigal (FMCSV) é uma organização da sociedade civil dedicada ao fortalecimento da primeira infância no Brasil. Sua atuação concentra-se na integração entre produção de conhecimento, advocacy e apoio à formulação e implementação de políticas públicas, com o objetivo de assegurar o desenvolvimento integral de crianças de 0 a 6 anos. A Fundação acompanha o debate sobre educação domiciliar (homeschooling), posicionando-se de forma contrária a avanços nessa pauta. Além disso, participa ativamente da construção e implementação da Política Nacional Integrada da Primeira Infância. Desde 2007, a Fundação trabalha para garantir que todas as crianças brasileiras tenham uma infância saudável, com seus direitos plenamente assegurados. Com o lançamento da Agenda 2030 pela Organização das Nações Unidas (ONU), a instituição passou a alinhar suas estratégias à meta 4.2 dos Objetivos de Desenvolvimento Sustentável (ODS), que trata da garantia de acesso a cuidados e educação de qualidade na primeira infância. Entre suas iniciativas, destaca-se o programa 'Primeira Infância Primeiro', que disponibiliza dados, evidências e ferramentas para gestores públicos e candidatos, contribuindo para a qualificação do debate e das políticas voltadas à infância.",
    ),
    "IEPS": (
        "Instituto de Estudos para Políticas de Saúde (IEPS)",
        "O Instituto de Estudos para Políticas de Saúde (IEPS) é uma organização independente e sem fins lucrativos dedicada a aprimorar políticas de saúde no Brasil, combinando pesquisa aplicada, produção de evidências e advocacy em temas como atenção primária, saúde digital e financiamento do SUS. Com especialização em políticas públicas de saúde, o IEPS possui uma atuação centrada no fortalecimento do SUS enquanto sistema universal e equitativo. A organização se expande pela observação dos mais diversos temas, assim, é complexo delimitar seus núcleos de observação. No âmbito da análise da organização e governança federativa do SUS, com atenção especial ao modelo tripartite (União–Estados–Municípios), ao papel do Ministério da Saúde e às distorções introduzidas por emendas parlamentares no orçamento setorial. Existe um foco técnico na Estruturação da Atenção Primária à Saúde (APS), financiamento per capita e critérios redistributivos, regionalização como instrumento de coordenação federativa, e planejamento e alocação eficiente de recursos. Ademais, existe uma busca por equidade e enfrentamento de desigualdades, através do monitoramento de políticas criadas com ênfase em grupos historicamente vulnerabilizados, população negra, povos indígenas e originários, pessoas com deficiência, população LGBTQIA+, pessoas em situação de rua, e crianças, adolescentes, mulheres, homens e idosos. No campo da força de trabalho em saúde, a instituição aborda a relação entre disponibilidade de profissionais, organização federativa e financiamento do SUS. A análise envolve a escassez e a distribuição territorial de médicos, enfermeiros e demais categorias, considerando desigualdades regionais e capacidade instalada dos entes subnacionais. É necessário destaque para o Programa Agora Tem Especialistas, que vem sendo acompanhado desde sua instituição no ano passado. No âmbito da saúde mental, a atuação contempla a organização da Rede de Atenção Psicossocial e a consolidação do processo de desinstitucionalização. A fiscalização de comunidades terapêuticas é um tema de alta relevância. O uso terapêutico de cannabis e derivados é tratado no âmbito da regulação sanitária. Seguindo a nova ordem de prioridades de atuação do IEPS, especialmente em seu trabalho de incidência na Secretaria Executiva da Frente Parlamentar Mista para a Promoção da Saúde Mental, as BETs são destaque no monitoramento para 2026. A organização também realiza o acompanhamento das decisões da Anvisa e da ANS, com foco na regulação de produtos, serviços e operadoras de planos. No eixo de emergências sanitárias, a instituição acompanha políticas de vigilância epidemiológica, declaração de estados de emergência e coordenação federativa em situações de crise. Como as mudanças climáticas vêm sendo consideradas no planejamento das políticas de saúde, são observados debates relacionados a eventos extremos, deslocamentos populacionais e impactos sobre doenças transmissíveis e crônicas.",
    ),
    "IAS": (
        "Instituto Ayrton Senna (IAS)",
        "O Instituto Ayrton Senna (IAS) é um centro de inovação em educação que atua em pesquisa e desenvolvimento, disseminação em larga escala e influência em políticas públicas, com foco em aprendizagem acadêmica e competências socioemocionais na rede pública.",
    ),
    "ISG": (
        "Instituto Sonho Grande (ISG)",
        "O Instituto Sonho Grande (ISG) é uma organização sem fins lucrativos e apartidária voltada à expansão e qualificação do ensino médio integral em redes públicas. Atua em parceria com secretarias estaduais de educação, oferecendo apoio na revisão curricular, na formação de equipes escolares e na implementação de modelos de gestão orientados a resultados. Além disso, o Instituto mantém foco no fortalecimento da infraestrutura das escolas públicas de tempo integral e na promoção de políticas voltadas à alfabetização. Também acompanha o debate sobre educação domiciliar (homeschooling), posicionando-se de forma contrária à sua ampliação. Em colaboração com governos estaduais e organizações do terceiro setor, o ISG trabalha para ampliar o acesso ao Ensino Médio Integral em escolas públicas. Por meio de pesquisas e avaliações contínuas, analisa os impactos desse modelo educacional para a formação de jovens.",
    ),
    "Reúna": (
        "Instituto Reúna",
        "O Instituto Reúna desenvolve pesquisas e ferramentas para apoiar redes e escolas na implementação de políticas educacionais alinhadas à BNCC, com foco em currículo, materiais de apoio e formação de professores.",
    ),
    "REMS": (
        "Rede Esporte pela Mudança Social (REMS)",
        "A REMS – Rede Esporte pela Mudança Social articula organizações que usam o esporte como vetor de desenvolvimento humano, mobilizando atores e produzindo conhecimento para ampliar o impacto social dessa agenda no país. A rede atua desde 2007 no fortalecimento do campo do esporte para o desenvolvimento social, promovendo a troca de experiências, a sistematização de práticas e a realização de agendas coletivas, bem como acompanhando e incidindo em debates sobre políticas públicas, financiamento, marcos regulatórios e programas governamentais relacionados ao esporte educacional, comunitário e de participação. Sua atuação abrange o nível federal, com foco na qualificação da formulação e implementação de iniciativas, no fortalecimento técnico das organizações integrantes e no diálogo com gestores públicos e parlamentares, além da articulação da pauta esportiva com áreas como educação, assistência social, saúde e desenvolvimento territorial.",
    ),
    "Manual": (
        "Manual",
        "A Manual se posiciona como uma empresa de cuidado contínuo e personalizado, com foco em acesso facilitado, discrição e conveniência. Ela é uma plataforma digital voltada principalmente à saúde e bem-estar masculino, oferecendo atendimento online e tratamentos baseados em evidências (como saúde capilar, sono e saúde sexual), com prescrição médica e acompanhamento remoto. Tem um interesse em promover a inovação dentro da área de saúde, principalmente em relação a manipuláveis, como o princípio ativo GLP-1. Possui uma atuação aprofundada conectando clientes com médicos e tratamentos para emagrecimento (foco em GLP-1 e redutores de apetite), disfunção erétil (oferecendo serviços que incluem consultas médicas e medicamentos manipulados que impedem a ação da enzima PDE 5) e queda capilar (acompanhamento junto com o uso de Finasterida e Minoxidil). Por serem uma plataforma, também possuem interesse na expansão da telemedicina e inovações no campo tecnológico associado à saúde.",
    ),
    "Cactus": (
        "Instituto Cactus",
        "O Instituto Cactus é uma organização filantrópica e de direitos humanos, sem fins lucrativos e independente, que atua para ampliar e qualificar o ecossistema da saúde mental no Brasil, desenvolvendo projetos voltados à prevenção de agravos e à promoção do cuidado, com foco prioritário em mulheres e adolescentes. Sua atuação se organiza em duas frentes complementares: o fomento estratégico (grant-making), por meio do qual financia, co-cria e oferece suporte técnico a iniciativas que constroem e ampliam soluções e ferramentas em saúde mental, além de produzir evidências e incentivar inovações no campo da atenção psicossocial; e o advocacy, com foco na formulação, implementação e avaliação de políticas públicas, bem como na análise qualificada de projetos de lei. O Instituto também realiza incidência política para fortalecer a agenda da saúde mental no debate público e institucional, desenvolve ferramentas de apoio a gestores e governos e promove ações de educação, sensibilização e mobilização social, que objetivam reduzir o estigma e consolidar uma narrativa mais humanizada sobre o tema no país.",
    ),
    "Vital Strategies": (
        "Vital Strategies",
        "A Vital Strategies é uma organização global de saúde pública que trabalha com governos e sociedade civil na concepção e implementação de políticas baseadas em evidências em áreas como doenças crônicas, segurança viária, qualidade do ar, dados vitais e comunicação de risco. A organização trabalha com base em dados para a saúde e mortes evitáveis, então os temas são muito intersetoriais. Desde o ano passado tem focado na Reforma Tributária, em especial no Imposto Seletivo, buscando incidir sobre a alíquota em bebidas açucaradas, álcool e tabaco. O intuito é atingir um consumo zero sobre conteúdos que geram malefícios à saúde, como no caso de DCNTs, como hipertensão arterial. Para além desta campanha, também trata do cuidado no trânsito, observando acidentes que estão relacionados ao uso de drogas (lícitas ou não). Políticas sobre vedação total ou parcial de marketing, publicidade e rotulagem de cigarros, dispositivos eletrônicos para fumar, alimentos ultraprocessados e bebidas alcoólicas também são de interesse. Na área de desenvolvimento tecnológico, tem investido nos estudos que ligam Inteligência Artificial à jornada do paciente, buscando dois pontos principais: combate ao feminicídio e diagnóstico precoce de câncer. Com a incidência sobre a COP30 no ano passado, temas como saúde ambiental, qualidade do ar e intoxicação por chumbo também são acompanhados pela organização.",
    ),
    "Mevo": (
        "Mevo",
        "A Mevo é uma healthtech brasileira que integra soluções de saúde digital, da prescrição eletrônica à compra e entrega de medicamentos, conectando médicos, hospitais, farmácias e pacientes para tornar o cuidado mais simples, eficiente e rastreável. Seu foco está na construção de um ecossistema digital interoperável, atuando de forma contínua junto aos Poderes Legislativo e Executivo para contribuir com o fortalecimento de uma Rede Nacional de Dados em Saúde (RNDS) robusta e integrada. A empresa também mantém diálogo com agências reguladoras, com o objetivo de promover um ambiente normativo que viabilize uma rede de dados e de prescrição eletrônica interoperável e de alcance universal. Nesse contexto, acompanha e contribui para debates regulatórios relacionados à saúde digital, interoperabilidade de sistemas, proteção de dados e normativas que impactem o funcionamento de suas soluções tecnológicas.",
    ),
    "Coletivo Feminista": (
        "Coletivo Feminista",
        "O Nem Presa Nem Morta é um movimento feminista que atua pela descriminalização e legalização do aborto no Brasil, articulando pesquisa, incidência política e mobilização social. Seus princípios ético-políticos abrangem a comunicação como direito e fundamento da democracia, a defesa do Estado democrático de direito, a compreensão de que maternidade não é dever e deve respeitar a liberdade de escolha, a promoção de uma atenção universal, equânime e integral à saúde — com ênfase no papel do SUS, no acesso a métodos contraceptivos e abortivos seguros e no respeito à autodeterminação reprodutiva —, além da defesa da descriminalização e legalização do aborto. Desde o final do ano passado, o coletivo tem focado em dois projetos essenciais: o novo Código Civil e o PLD3/2025, que susta a resolução 258 do Conselho Nacional dos Direitos da Criança e do Adolescente (CONANDA). Em ambos os projetos a atuação da organização tem sido em evitar regressões inconstitucionais ligadas ao aborto, em especial quando se trata de crianças e adolescentes.",
    ),
    "IDEC": (
        "Instituto Brasileiro de Defesa do Consumidor (Idec)",
        "O Instituto Brasileiro de Defesa do Consumidor (Idec) é uma associação civil sem fins lucrativos, fundada em 1987, independente de empresas, partidos ou governos, que atua na defesa dos direitos dos consumidores e na promoção de relações de consumo éticas, seguras e sustentáveis. Sua atuação combina advocacy, pesquisa e litigância estratégica, com foco em temas como saúde, alimentação, energia, telecomunicações e direitos digitais, sendo os temas pautados muitas vezes transversais a todas as áreas. O Idec se destaca na formulação e incidência em políticas públicas relacionadas à promoção da alimentação adequada e saudável, ao controle de ultraprocessados e agrotóxicos, à rotulagem nutricional, à transição energética justa e à regulação de plataformas digitais. Também acompanha de perto a regulação dos planos de saúde, atuando junto à ANS, e a saúde digital do ponto de vista do direito do consumidor. Pauta temas como greenwashing e práticas abusivas de telemarketing. Paralelamente, produz estudos, materiais técnicos e eventos voltados à informação e mobilização da sociedade, mantendo diálogo com o Legislativo por meio de parcerias e incidência em projetos de lei, inclusive em debates como os relacionados ao ReData.",
    ),
    "Umane": (
        "Umane",
        "A Umane é uma organização da sociedade civil isenta, apartidária e sem fins lucrativos que atua para fomentar a saúde pública de forma sistêmica no Brasil, com foco em ampliar equidade, eficiência e qualidade do sistema de saúde. Sua missão é apoiar iniciativas transformadoras de prevenção de doenças e promoção da saúde que melhorem a qualidade de vida da população, operando por meio de fomento a projetos, articulação com uma rede de parceiros e um modelo de trabalho que combina monitoramento e avaliação, uso de dados e tecnologia (como telessaúde e uso de IA, com foco sempre na inovação dentro da área de saúde) e advocacy/comunicação para fortalecer políticas públicas. As frentes programáticas explicitadas pela Umane incluem o fortalecimento da Atenção Primária à Saúde (APS), a atenção integral às Doenças Crônicas Não Transmissíveis (DCNT), sendo o foco as doenças cardiovasculares, diabetes tipo 2, obesidade, subnutrição e dislipidemias; e a saúde da mulher, da criança e do adolescente, com ênfase na articulação entre os níveis de atenção à saúde para o pré-natal, no acompanhamento integral dos primeiros mil dias e no enfrentamento da má nutrição infantil e juvenil.",
    ),
    "ASBAI": (
        "Associação Brasileira de Alergia e Imunologia (ASBAI)",
        "A Associação Brasileira de Alergia e Imunologia (ASBAI) é uma entidade científica sem fins lucrativos que reúne médicos especialistas em alergia e imunologia clínica no Brasil. Atua na promoção do ensino, pesquisa e atualização profissional nessas áreas, elaborando diretrizes clínicas, posicionamentos técnicos e recomendações para o diagnóstico e tratamento de doenças alérgicas e imunológicas. Seu foco está tanto no cenário nacional, acompanhando debates regulatórios junto ao Ministério da Saúde (especialmente a Conitec) e à Anvisa, especialmente em temas como incorporação de tecnologias, imunobiológicos, vacinas, assistência farmacêutica e protocolos clínicos, quanto na articulação com sociedades médicas estaduais e internacionais. A ASBAI também promove congressos, cursos e campanhas de conscientização sobre condições como asma, rinite alérgica, dermatite atópica, alergias alimentares, imunodeficiências primárias e anafilaxia. Sua principal linha de atuação neste momento é sobre a incorporação da caneta de adrenalina autoinjetável no SUS e também a obrigatoriedade de notificação ao Ministério da Saúde de ocorrências de anafilaxia/choque anafilático.",
    ),
    "Infinis": (
        "Instituto Futuro é Infância Saudável (Infinis)",
        "O Instituto Futuro é Infância Saudável (Infinis) é a frente de filantropia estratégica e advocacy da Fundação José Luiz Setúbal (FJLS). A organização atua com base em evidências científicas para promover políticas públicas, fortalecer a sociedade civil e impulsionar soluções que assegurem saúde e bem-estar na infância. Sua atuação está estruturada em quatro eixos temáticos: segurança alimentar e enfrentamento da má nutrição; saúde mental; prevenção às violências; e fortalecimento da sociedade civil. Esses eixos estão alinhados aos Objetivos de Desenvolvimento Sustentável (ODS) da ONU, especialmente no que se refere à promoção da saúde, da equidade e da proteção de crianças e adolescentes. Com foco na incidência política, o Infinis busca contribuir para o aprimoramento e a efetiva implementação de políticas públicas, além de fomentar a transformação de comportamentos e o desenvolvimento de soluções locais sustentáveis. No campo do fortalecimento da sociedade civil, apoia a produção de pesquisas científicas e o desenvolvimento de organizações de infraestrutura que atuam no setor.",
    ),
    "UNFPA": (
        "Fundo de População das Nações Unidas (UNFPA)",
        "O Fundo de População das Nações Unidas (UNFPA) é a agência das Nações Unidas voltada à saúde sexual e reprodutiva e às questões de população e desenvolvimento. No Brasil, atua desde 1973 em cooperação com governos, organismos internacionais, sociedade civil e outros parceiros para apoiar a formulação, implementação e monitoramento de políticas públicas baseadas em direitos, evidências e redução de desigualdades. Sua missão é contribuir para um mundo em que todas as gestações sejam desejadas, todos os partos sejam seguros e cada jovem alcance seu potencial. A organização acompanha temas como saúde sexual e reprodutiva, direitos reprodutivos, planejamento reprodutivo, mortalidade materna, atenção obstétrica, acesso a contraceptivos e insumos de saúde, gravidez na adolescência, uniões infantis, violência baseada em gênero, feminicídio, exploração sexual, tráfico de pessoas, juventudes, juventude negra, igualdade racial, população e produção de dados para políticas públicas. Também atua em agendas transversais, como emergências humanitárias, mudanças climáticas, justiça climática, gênero e proteção de populações vulnerabilizadas. No campo da incidência pública, o UNFPA tende a priorizar debates relacionados à autonomia corporal, à garantia de direitos de meninas, mulheres, adolescentes e jovens, ao enfrentamento de desigualdades raciais e territoriais e ao uso de dados demográficos, censitários e populacionais para subsidiar a formulação, implementação e monitoramento de políticas públicas.",
    ),
}

CLIENTE_DESCRICOES.setdefault("Reuna", CLIENTE_DESCRICOES["Reúna"])

# ── PROMPT ────────────────────────────────────────────────────────────────────

PROMPT = Template(
"""Você é analista de políticas públicas e faz triagem de atos do DOU e matérias legislativas para um(a) cliente.

Missão/escopo do cliente:
$descricao

Tarefa:
Classificar o alinhamento do **Conteúdo** com a missão do cliente.

Regras de evidência:
- Use **apenas** o Conteúdo e a descrição do cliente acima. Não utilize conhecimento próprio sobre o cliente além do que está descrito neste prompt.
- NÃO exija que o Conteúdo cubra TODA a missão do cliente.
  # Se o Conteúdo estiver claramente dentro de ao menos UMA frente/eixo relevante do cliente, marque "Alinha".
  # A ausência de menção a outras frentes (ex.: socioemocional) NÃO reduz automaticamente para "Parcial".
- Use "Parcial" apenas nos dois casos descritos abaixo — não como classe-padrão para dúvidas genéricas.
- Se o texto for claramente de natureza incompatível com triagem temática (ex.: decisão sobre caso individual sem política pública; deferimento/indeferimento nominal; concessão pontual; nomeação/dispensa rotineira sem tema; mero expediente administrativo sem objeto; publicação que não permite inferir assunto), marque "Não se aplica".

Classes (escolha exatamente UMA):
- "Alinha": O objeto/tema do Conteúdo é claro e há evidência explícita de relação com pelo menos 1 frente/eixo do cliente.
- "Parcial": Use SOMENTE em um destes dois casos:
    (a) Ambiguidade temática — o Conteúdo trata de tema que poderia ou não se encaixar na missão, mas o texto é insuficiente para decidir com segurança;
    (b) Cobertura incompleta — o Conteúdo aborda parcialmente o tema do cliente, mas mistura substancialmente outras agendas não relacionadas, de modo que a relevância é real porém limitada.
- "Não Alinha": O tema é claro e não tem relação com a missão do cliente.
- "Não se aplica": O Conteúdo não é classificável por tema/escopo do cliente com base no texto (exemplos acima), ou é predominantemente ato individual/procedimental sem política pública inferível.

Restrições estruturais obrigatórias:
- NÃO inclua qualquer metacomentário sobre a classificação.
- NÃO mencione que está classificando, analisando ou respondendo ao prompt.
- A justificativa deve conter APENAS uma descrição objetiva do que o Conteúdo trata (objeto/tema).
- NÃO explique impactos potenciais, intenções do autor ou interpretações jurídicas.
- NÃO utilize linguagem avaliativa ou argumentativa.

Formato de saída:
Retorne **somente** JSON válido neste formato:
{
  "alinhamento": "Alinha" | "Parcial" | "Não Alinha" | "Não se aplica",
  "justificativa": "1–3 frases citando elementos do Conteúdo (termos/trechos) que sustentam a decisão"
}

Conteúdo:
<conteudo>
$conteudo
</conteudo>""".strip()
)


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

    # Sanitiza o conteúdo para não quebrar o delimitador XML do prompt.
    # Substitui </conteudo> por uma versão com zero-width space para neutralizar
    # qualquer fechamento prematuro da tag caso o texto do DOU contenha esse padrão.
    conteudo_safe = conteudo.replace("</conteudo>", "</conteudo\u200b>")

    prompt = PROMPT.substitute(cliente=cliente_nome, descricao=desc, conteudo=conteudo_safe)

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
    for col in COLS_CANONICAL:
        if col not in df.columns:
            df[col] = ""
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
