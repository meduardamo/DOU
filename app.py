from datetime import datetime
from urllib.parse import quote
import re
import inspect

import streamlit as st
from google import genai

import yaml
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth


# tem que ser a primeira chamada Streamlit do arquivo
st.set_page_config(page_title="Gerador de Envios", layout="wide")


# ========== AUTH (login único) ==========
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.load(f, Loader=SafeLoader)

if not isinstance(config, dict) or "credentials" not in config or "cookie" not in config:
    st.error("config.yaml inválido. Verifique estrutura/indentação.")
    st.stop()

authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
)

sig = inspect.signature(authenticator.login)
params = sig.parameters

try:
    if "location" in params:
        login_result = authenticator.login(location="main")
    else:
        login_result = authenticator.login("Login", "main")
except TypeError:
    login_result = authenticator.login("Login", "main")

if isinstance(login_result, (tuple, list)) and len(login_result) == 3:
    name, authentication_status, username = login_result
else:
    authentication_status = st.session_state.get("authentication_status", None)
    name = st.session_state.get("name", "")
    username = st.session_state.get("username", "")

if authentication_status is False:
    st.error("Usuário ou senha inválidos.")
    st.stop()

if authentication_status is None:
    st.info("Faça login para continuar.")
    st.stop()


# ========== APP ==========
# Cole sua chave aqui (não commite isso em repo público)
GEMINI_API_KEY = "AIzaSyA_gyoB7mqXvLAXF0ygOfLqeWyjju4cdvo"
GEMINI_MODEL = "gemini-2.5-flash"

# Paleta Eixo
EIXO = {
    "preto": "#000000",
    "cinza": "#999999",
    "gelo": "#e1e1e1",
    "vinho": "#962E4D",
    "azul": "#192D4E",
    "amarelo": "#E8A600",
    "amarelo_claro": "#f0d46c",
    "vermelho": "#B84349",
}

# Coloque este arquivo na mesma pasta do app.py
LOGO_PATH = "Marca_eixo_vetor_Logo horizontal magenta.png"

AREAS = [
    "Política",
    "Economia",
    "Orçamento",
    "Tributação",
    "Energia",
    "Infraestrutura",
    "Meio Ambiente",
    "Agricultura",
    "Indústria e Comércio",
    "Trabalho e Renda",
    "Assistência Social",
    "Segurança Pública",
    "Educação",
    "Primeira Infância",
    "Saúde",
    "Direitos Humanos",
    "Mulheres",
    "Infância e Adolescência",
    "Tecnologia e Inovação",
    "Comunicações",
    "Justiça",
    "Judiciário",
    "Relações Internacionais",
    "Câmara dos Deputados",
    "Senado Federal",
    "Congresso Nacional",
    "Poder Executivo",
    "Agências Reguladoras",
    "ANS",
    "ANVISA",
    "Subnacional",
]

UFS = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
    "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"
]

st.markdown(
    f"""
<style>
:root {{
  --eixo-preto: {EIXO["preto"]};
  --eixo-cinza: {EIXO["cinza"]};
  --eixo-gelo: {EIXO["gelo"]};
  --eixo-vinho: {EIXO["vinho"]};
  --eixo-azul: {EIXO["azul"]};
  --eixo-amarelo: {EIXO["amarelo"]};
  --eixo-amarelo-claro: {EIXO["amarelo_claro"]};
  --eixo-vermelho: {EIXO["vermelho"]};
}}

h1, h2, h3 {{
  letter-spacing: -0.02em;
}}

div[data-testid="stForm"] {{
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 14px;
  padding: 14px 14px 8px 14px;
  background: rgba(255,255,255,0.02);
}}

.stButton > button {{
  border-radius: 12px !important;
  border: 1px solid rgba(255,255,255,0.14) !important;
  background: linear-gradient(90deg, var(--eixo-vinho), rgba(150,46,77,0.75)) !important;
  color: white !important;
  font-weight: 600 !important;
  padding: 10px 14px !important;
}}

.stButton > button:hover {{
  filter: brightness(1.05);
}}

small, .stCaption {{
  color: rgba(255,255,255,0.7) !important;
}}

hr {{
  border: 0;
  border-top: 1px solid rgba(255,255,255,0.08);
}}

div[data-testid="stCodeBlock"] > pre {{
  max-height: 520px !important;
  overflow: auto !important;
  border-radius: 14px !important;
}}
</style>
""",
    unsafe_allow_html=True
)


def data_br(dt: datetime) -> str:
    return dt.strftime("%d/%m/%Y")


def montar_header(is_alerta: bool, area: str, uf: str | None) -> str:
    prefixo = "Alerta" if is_alerta else "Envio"
    if area == "Subnacional" and uf:
        return f"{prefixo} | Eixo | Subnacional | {uf}"
    return f"{prefixo} | Eixo | {area}"


def limpar_prefixo_alerta_envio(resumo: str) -> str:
    s = (resumo or "").strip()

    s = re.sub(
        r"^(ALERTA|ENVIO)\s*(?:[-–—]|:)?\s*[^:\n]{0,60}:\s*",
        "",
        s,
        flags=re.IGNORECASE
    )

    s = re.sub(
        r"^(ALERTA|ENVIO)\s*(?:[-–—]|:)\s*",
        "",
        s,
        flags=re.IGNORECASE
    )

    return s.strip()


@st.cache_resource
def get_gemini_client():
    if not GEMINI_API_KEY or GEMINI_API_KEY.strip() == "COLE_SUA_CHAVE_AQUI":
        raise RuntimeError("Cole sua GEMINI_API_KEY no topo do app.py antes de usar.")
    return genai.Client(api_key=GEMINI_API_KEY)


def gerar_resumo_gemini(texto: str, is_alerta: bool, area: str) -> str:
    client = get_gemini_client()

    if is_alerta:
        instrucao = (
            "Gere um texto para WhatsApp, factual e direto, em português do Brasil.\n"
            "Formato:\n"
            "- Escreva em parágrafos curtos (1 a 3 parágrafos), não em 'linha por linha'.\n"
            "- Preferência: 1 parágrafo para o fato principal + 1 parágrafo de contexto/impacto imediato.\n"
            "Regras:\n"
            "- Sem opinião, sem adjetivação inflada, sem especulação.\n"
            "- Não invente nada; use somente o que estiver no texto.\n"
            "- Preserve exatamente números, datas, nomes, cargos, órgãos e decisões.\n"
            "- Se algo estiver incerto, sinalize com 'segundo X'/'de acordo com X' (sem extrapolar).\n"
            "- Não comece com rótulos/títulos do tipo 'ALERTA', 'ENVIO', 'ALERTA <ÁREA>:' etc.\n"
            "- Comece direto pelo fato principal (quem fez o quê + o que mudou/decidiu + consequência imediata).\n"
            "- Evite repetição e rodeios.\n"
            "- Extensão: até 120–220 palavras (pode ser menos se o texto for curto).\n"
            "- Sem bullet points, sem emojis, sem meta-comentários.\n"
        )
    else:
        instrucao = (
            "Gere um texto para WhatsApp, factual e claro, em português do Brasil.\n"
            "Formato:\n"
            "- Escreva em parágrafos curtos (2 a 4 parágrafos), não em 'linha por linha'.\n"
            "- Estrutura sugerida: (1) fato principal; (2) detalhes/medidas; (3) contexto mínimo; (4) próximos passos/impactos.\n"
            "Regras:\n"
            "- Sem opinião, sem adjetivação inflada, sem especulação.\n"
            "- Não invente nada; use somente o que estiver no texto.\n"
            "- Preserve exatamente números, datas, nomes, cargos, órgãos e decisões.\n"
            "- Se algo estiver incerto, sinalize com 'segundo X'/'de acordo com X' (sem extrapolar).\n"
            "- Não comece com rótulos/títulos do tipo 'ALERTA', 'ENVIO', 'ENVIO <ÁREA>:' etc.\n"
            "- Comece direto pelo fato principal e mantenha uma sequência lógica.\n"
            "- Evite repetição e rodeios.\n"
            "- Extensão: até 220–420 palavras (pode ser menos se o texto for curto).\n"
            "- Sem bullet points, sem emojis, sem meta-comentários.\n"
        )

    prompt = f"""
Você é um analista que produz envios padronizados para WhatsApp.

Contexto:
- O cabeçalho (Alerta/Envio | Eixo | Área) + a data + o título já serão adicionados fora do modelo.
- Sua tarefa aqui é gerar apenas o corpo do texto (resumo), em parágrafos.

Área: {area}
Instruções: {instrucao}

TEXTO:
{texto}
""".strip()

    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt
    )

    resumo = (resp.text or "").strip()
    resumo = limpar_prefixo_alerta_envio(resumo)
    return resumo


def compilar_envio(
    is_alerta: bool,
    area: str,
    uf: str | None,
    titulo: str,
    resumo: str,
    analise_eixo: str | None,
) -> str:
    header = montar_header(is_alerta=is_alerta, area=area, uf=uf)
    dt = data_br(datetime.now())

    header_fmt = f"*{header}*"
    titulo_fmt = f"*{titulo.strip()}*"

    partes = []
    partes.append(header_fmt)
    partes.append(dt)
    partes.append("")
    partes.append(titulo_fmt)
    partes.append("")
    partes.append(resumo.strip())

    if analise_eixo and analise_eixo.strip():
        partes.append("")
        partes.append("ANÁLISE EIXO")
        partes.append(analise_eixo.strip())

    return "\n".join(partes)


def whatsapp_share_link(message: str) -> str:
    return f"https://wa.me/?text={quote(message)}"


@st.dialog("Enviar no WhatsApp")
def dialog_whatsapp(message: str):
    st.caption("Vai abrir o WhatsApp com o texto já preenchido. O envio final acontece por lá.")
    st.link_button("Abrir WhatsApp", whatsapp_share_link(message), use_container_width=True)


with st.sidebar:
    authenticator.logout("Sair", "sidebar")
    st.caption(f"Logado como: {name} ({username})")

    st.markdown("---")
    try:
        st.image(LOGO_PATH, width="stretch")
    except Exception:
        st.caption("Logo não encontrada. Coloque o arquivo na mesma pasta do app.py:")
        st.code(LOGO_PATH, language="text")

    st.markdown("---")
    st.caption(
        "Cole o texto da notícia, escolha tipo e área, e o app gera um envio/alerta padronizado com IA.\n"
        "O resultado já sai no formato para copiar e colar no WhatsApp."
    )


if "resultado_final" not in st.session_state:
    st.session_state["resultado_final"] = ""

st.title("Gerador de Envios")

col_esq, col_dir = st.columns([1.25, 1])

with col_esq:
    st.subheader("Preenchimento do envio")

    st.markdown("### Classificação")

    is_alerta = st.radio(
        "Tipo",
        ["Envio", "Alerta"],
        index=0,
        horizontal=True,
        key="tipo_radio",
    ) == "Alerta"

    area = st.selectbox(
        "Área",
        AREAS,
        index=0,
        key="area_select",
    )

    uf = None
    if area.strip() == "Subnacional":
        uf = st.selectbox(
            "UF (obrigatório se Subnacional)",
            UFS,
            index=0,
            key="uf_select",
        )

    st.markdown("---")

    with st.form("form_envio", clear_on_submit=False):
        texto = st.text_area(
            "Texto da notícia (até 10 mil caracteres)",
            max_chars=10_000,
            height=240,
            placeholder="Cole aqui o texto da notícia..."
        )

        st.markdown("### Campos")

        titulo = st.text_input(
            "Título (obrigatório)",
            value="",
            placeholder="Ex.: Haddad descarta candidatura em 2026..."
        )

        analise_eixo = st.text_area(
            "Análise Eixo (opcional)",
            height=120,
            placeholder="Se quiser, escreva aqui uma análise curta e objetiva."
        )

        submitted = st.form_submit_button("Gerar envio/alerta")

    if submitted:
        erros = []
        if not texto.strip():
            erros.append("Cole o texto da notícia.")
        if not titulo.strip():
            erros.append("Preencha o título (obrigatório).")
        if area.strip() == "Subnacional" and not uf:
            erros.append("Selecione a UF (obrigatório para Subnacional).")

        if erros:
            for e in erros:
                st.error(e)
        else:
            with st.spinner("Gerando resumo com IA e compilando o envio..."):
                try:
                    resumo = gerar_resumo_gemini(texto=texto, is_alerta=is_alerta, area=area)
                    st.session_state["resultado_final"] = compilar_envio(
                        is_alerta=is_alerta,
                        area=area,
                        uf=uf,
                        titulo=titulo,
                        resumo=resumo,
                        analise_eixo=analise_eixo
                    )
                    st.success("Envio gerado.")
                except Exception as e:
                    st.error(f"Erro ao gerar com Gemini: {e}")

with col_dir:
    st.subheader("Resultado")

    if not st.session_state["resultado_final"].strip():
        st.info("Preencha o formulário e clique em “Gerar envio/alerta”.")
    else:
        st.markdown("**Copiar:**")
        st.code(st.session_state["resultado_final"], language="text")

        c1, c2 = st.columns([1, 1])

        with c1:
            if st.button("Enviar no WhatsApp", use_container_width=True):
                dialog_whatsapp(st.session_state["resultado_final"])

        with c2:
            st.download_button(
                "Baixar como .txt",
                data=st.session_state["resultado_final"].encode("utf-8"),
                file_name="envio_padronizado.txt",
                mime="text/plain",
                use_container_width=True
            )