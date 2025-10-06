import os, re, json, time, random
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from google import genai
from string import Template

# CONFIG
GENAI_API_KEY   = os.getenv("GENAI_API_KEY", "")
MODEL_NAME      = "gemini-2.5-flash"
PLANILHA_CLIENTES = os.getenv("PLANILHA_CLIENTES")  # só a key
SKIP_SHEETS = {"Giro de notícias"}  # pular essa aba

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

BATCH_SIZE = int(os.getenv("ALIGN_BATCH", "25"))
SLEEP_SEC  = float(os.getenv("ALIGN_SLEEP", "0.10"))

# DESCRIÇÕES DOS CLIENTES
CLIENTE_DESCRICOES = {
    "IU": "O Instituto Unibanco (IU) é uma organização sem fins lucrativos que apoia redes estaduais de ensino na melhoria da gestão educacional por meio de projetos como o Jovem de Futuro, produção de conhecimento e apoio técnico a secretarias de educação.",
    "FMCSV": "A Fundação Maria Cecilia Souto Vidigal (FMCSV) atua pela causa da primeira infância no Brasil, conectando pesquisa, advocacy e apoio a políticas públicas para garantir o desenvolvimento integral de crianças de 0 a 6 anos; iniciativas como o “Primeira Infância Primeiro” oferecem dados e ferramentas para gestores e candidatos.",
    "IEPS": "O Instituto de Estudos para Políticas de Saúde (IEPS) é uma organização independente e sem fins lucrativos dedicada a aprimorar políticas de saúde no Brasil, combinando pesquisa aplicada, produção de evidências e advocacy em temas como atenção primária, saúde digital e financiamento do SUS.",
    "IAS": "O Instituto Ayrton Senna (IAS) é um centro de inovação em educação que atua em pesquisa e desenvolvimento, disseminação em larga escala e influência em políticas públicas, com foco em aprendizagem acadêmica e competências socioemocionais na rede pública.",
    "ISG": "O Instituto Sonho Grande (ISG) é uma organização sem fins lucrativos e apartidária voltada à expansão e qualificação do ensino médio integral em redes públicas; trabalha em parceria com estados para revisão curricular, formação de equipes e gestão orientada a resultados.",
    "Reúna": "O Instituto Reúna desenvolve pesquisas e ferramentas para apoiar redes e escolas na implementação de políticas educacionais alinhadas à BNCC, com foco em currículo, materiais de apoio e formação de professores.",
    "REMS": "A REMS – Rede Esporte pela Mudança Social articula organizações que usam o esporte como vetor de desenvolvimento humano, mobilizando atores e produzindo conhecimento para ampliar o impacto social dessa agenda no país.",
    "Manual": "A Manual (saúde) é uma plataforma digital voltada principalmente à saúde masculina, oferecendo atendimento online e tratamentos baseados em evidências (como saúde capilar, sono e saúde sexual), com prescrição médica e acompanhamento remoto.",
    "Cactus": "O Instituto Cactus é uma entidade filantrópica e de direitos humanos que atua de forma independente em saúde mental, priorizando adolescentes e mulheres, por meio de advocacy e fomento a projetos de prevenção e promoção de cuidado em saúde mental.",
    "Vital Strategies": "A Vital Strategies é uma organização global de saúde pública que trabalha com governos e sociedade civil na concepção e implementação de políticas baseadas em evidências em áreas como doenças crônicas, segurança viária, qualidade do ar, dados vitais e comunicação de risco.",
    "Mevo": "A Mevo é uma healthtech brasileira que integra soluções de saúde digital (da prescrição eletrônica à compra/entrega de medicamentos) conectando médicos, hospitais, farmácias e pacientes para tornar o cuidado mais simples e rastreável.",
    "Coletivo Feminista": "O Nem Presa Nem Morta (NPNM) é um movimento feminista que atua pela descriminalização e legalização do aborto no Brasil, articulando pesquisa, incidência política e mobilização social. Seus princípios ético-políticos abrangem a comunicação como direito e fundamento da democracia, a defesa do Estado democrático de direito, a compreensão de que maternidade não é dever e deve respeitar a liberdade de escolha, a promoção de uma atenção universal, equânime e integral à saúde — com ênfase no papel do SUS, no acesso a métodos contraceptivos e abortivos seguros e no respeito à autodeterminação reprodutiva —, além da defesa da descriminalização e legalização do aborto.",
}

# alias sem acento, caso a aba venha como "Reuna"
CLIENTE_DESCRICOES.setdefault("Reuna", CLIENTE_DESCRICOES["Reúna"])

# PROMPT
PROMPT = Template("""
Você é analista de políticas públicas. Avalie a coerência do texto abaixo com a missão do(a) $cliente.

Missão/escopo do cliente:
$descricao

Use apenas o que está no **Conteúdo** (texto extraído do DOU). Se o texto for insuficiente, responda "Parcial".
Classifique o alinhamento em exatamente um destes valores:
- "Alinha"
- "Parcial"
- "Não Alinha"

Explique brevemente (1–3 frases) com base no Conteúdo.

Retorne **somente** JSON válido neste formato:
{
  "alinhamento": "Alinha" | "Parcial" | "Não Alinha",
  "justificativa": "texto explicativo baseado no Conteúdo"
}

Conteúdo:
\"\"\"$conteudo\"\"\"
""".strip())

# CLIENTES / SHEETS
def _gs_client():
    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if raw:
        info = json.loads(raw)
        if "private_key" in info and "\\n" in info["private_key"]:
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file("credentials.json",
                                                     scopes=["https://www.googleapis.com/auth/spreadsheets",
                                                             "https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds)

genai_client = genai.Client(api_key=GENAI_API_KEY)

def classify_text(cliente_nome: str, conteudo: str) -> dict:
    if not str(conteudo or "").strip():
        return {"alinhamento": "Parcial", "justificativa": "Conteúdo ausente ou vazio; não é possível concluir."}
    desc = CLIENTE_DESCRICOES.get(cliente_nome, f"Organização '{cliente_nome}' com foco temático conforme sua atuação pública.")
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
        a = str(data.get("alinhamento","")).strip()
        j = str(data.get("justificativa","")).strip()
        if a not in ("Alinha","Parcial","Não Alinha"):
            a = "Parcial"
        if not j: j = "Sem justificativa; revisar."
        return {"alinhamento": a, "justificativa": j}
    except Exception:
        return {"alinhamento": "Parcial", "justificativa": "Falha ao interpretar JSON; revisão manual sugerida."}

def ensure_output_cols(df: pd.DataFrame) -> pd.DataFrame:
    if COL_ALINH not in df.columns: df[COL_ALINH] = ""
    if COL_JUST  not in df.columns: df[COL_JUST]  = ""
    return df

def pick_conteudo(row: pd.Series) -> str:
    """Fonte principal é a COLUNA 'Conteúdo' (fallback: Resumo → Portaria)."""
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
    df = ensure_output_cols(df)

    if COL_CONTEUDO not in df.columns:
        print(f"[{title}] coluna '{COL_CONTEUDO}' não encontrada — nada a fazer.")
        return

    # linhas a processar: sem Alinhamento e com algum texto em Conteúdo/Resumo/Portaria
    mask = (df[COL_ALINH].astype(str).str.strip() == "") & (
        (df.get(COL_CONTEUDO,"").astype(str).str.strip() != "") |
        (df.get(COL_RESUMO,"").astype(str).str.strip() != "") |
        (df.get(COL_PORTARIA,"").astype(str).str.strip() != "")
    )
    idxs = list(df[mask].index)
    if not idxs:
        print(f"[{title}] nenhuma linha pendente.")
        return

    print(f"[{title}] classificando {len(idxs)} linha(s)...")
    for chunk_start in range(0, len(idxs), BATCH_SIZE):
        for i in idxs[chunk_start:chunk_start+BATCH_SIZE]:
            conteudo = pick_conteudo(df.loc[i])
            res = classify_text(title, conteudo)  # usa NOME DA ABA como cliente
            df.at[i, COL_ALINH] = res["alinhamento"]
            df.at[i, COL_JUST]  = res["justificativa"]
            if SLEEP_SEC: time.sleep(SLEEP_SEC)

        # grava até a última linha do mini-lote
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=False)
        print(f"[{title}] ✅ salvo até a linha {idxs[min(chunk_start+BATCH_SIZE-1, len(idxs)-1)]+2}")

def main():
    if not PLANILHA_CLIENTES:
        raise SystemExit("Defina PLANILHA_CLIENTES (apenas a key).")
    gc = _gs_client()
    sh = gc.open_by_key(PLANILHA_CLIENTES)

    for ws in sh.worksheets():
        process_sheet(ws)

if __name__ == "__main__":
    main()
