"""DOU da MegaEdu: raspagem própria, com as regras lidas da planilha deles.

As regras ficam na aba "Regras DOU" da planilha de monitoramento federal
(secret PLANILHA_MEGAEDU_DOU), uma por linha, e a equipe da MegaEdu pode
mexer nelas sem mudar código. Colunas lidas:

    ID | Tema | Tipo | Órgão | Termos (basta um) | Precisa ter também |
    O que pega | Ignorar se tiver | Seção do DOU | Observação

Tipo "Palavra-chave": basta um dos termos, separados por ";". "Precisa ter
também" aceita três formas:
    - "até ~150 caracteres de: a; b; c"   um desses perto do termo
    - "Só para as siglas X/Y: a, b ou c por perto"   idem, só para X e Y
    - "FNDE no texto E "registro de preços" no texto"   todos no ato
"Ignorar se tiver": se o trecho a ignorar contém o próprio termo (o nome de
uma coordenação que tem "educação digital" no meio), ele é apagado antes de
procurar o termo; senão, o ato sai se tiver o trecho.

Tipo "Cargo": "cargo ou função de X". Pega nomeação, exoneração, designação,
dispensa e tornar sem efeito no cargo X, quem quer que seja a pessoa.
Tipo "Pessoa": nome da pessoa com verbo de ato de pessoal antes, na mesma
frase; o nome só na assinatura não conta.

Lê as seções 1, 2 e 3 (e as extras) de ontem e de hoje, grava o que for novo
na aba "Atos encontrados" (da planilha do secret PLANILHA_MEGAEDU_ACHADOS ou,
sem ele, da própria planilha das regras) e manda email só quando entra linha. Rodar de novo
no mesmo dia não repete nada: o dedup é pela planilha (link + regra).

Uso:
    python megaedu_dou.py            raspa, grava e manda email
    python megaedu_dou.py --seco     raspa e só imprime (sem planilha, sem email)
    python megaedu_dou.py --regras   imprime as regras como o robô entendeu
"""

import os
import re
import sys
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import dou_comum as C
from dou_comum import norm

SHEET_ID = os.getenv("PLANILHA_MEGAEDU_DOU", "").strip()
# Onde gravar os atos. Vazio = na própria planilha das regras.
SHEET_ACHADOS = os.getenv("PLANILHA_MEGAEDU_ACHADOS", "").strip() or SHEET_ID
ABA_REGRAS = "Regras DOU"
ABA_ACHADOS = "Atos encontrados"
SECOES = ["DO1", "DO2", "DO3", "DO1E", "DO2E", "DO3E"]

COLS = ["Data", "Seção", "Regra", "Tema", "O que pega", "Movimento", "Nome",
        "Órgão", "Ato", "Trecho", "Link"]
LARGURAS = [90, 60, 60, 120, 260, 100, 220, 260, 280, 520, 120]

JANELA_PADRAO = 150  # "por perto", quando a regra não diz quantos caracteres
VAZIO = {"", "-", "—", "–"}


# ---------------------------------------------------------------------------
# Termo -> expressão regular sobre o texto normalizado
# ---------------------------------------------------------------------------

FEM = "qqfem"  # marca de "(a)" que sobrevive ao norm()


def _prepara(texto: str) -> str:
    """Tira aspas e siglas entre parênteses, marca "(a)" e normaliza."""
    t = re.sub(r"[\"'“”‘’]", " ", texto or "")
    t = re.sub(r"(\w)\((?:a|o)\)", r"\1" + FEM, t)
    t = re.sub(r"\([^)]*\)", " ", t)
    return norm(t)


def _token(tok: str) -> str:
    if tok.endswith(FEM):
        base = tok[: -len(FEM)]
        return base[:-1] + "[oa]" if base.endswith("o") else base + "a?"
    return re.escape(tok)


def termo_rx(termo: str) -> re.Pattern | None:
    """ "Lei nº 14.180" casa "lei n 14 180", "lei no 14 180" e "lei 14 180"."""
    toks = _prepara(termo).split()
    if not toks:
        return None
    partes = []
    for i, tok in enumerate(toks):
        prox = toks[i + 1] if i + 1 < len(toks) else ""
        if tok in ("n", "no") and prox[:1].isdigit():
            partes.append("(?:no? )?")
            continue
        partes.append(_token(tok) + " ")
    corpo = "".join(partes).rstrip(" ")
    # Plural do fim: "escola" casa "escolas", "matrícula" casa "matrículas".
    if toks[-1].isalpha():
        corpo += "(?:e?s)?"
    return re.compile(r"\b" + corpo + r"\b")


def _lista(texto: str) -> list[str]:
    return [p.strip() for p in re.split(r";", texto or "") if p.strip() and p.strip() not in VAZIO]


# ---------------------------------------------------------------------------
# Regras
# ---------------------------------------------------------------------------

class Regra:
    def __init__(self, linha: dict):
        g = lambda c: (linha.get(c) or "").strip()
        self.id = g("ID")
        self.tema = g("Tema")
        self.tipo = norm(g("Tipo"))
        self.orgao = g("Órgão")
        self.o_que_pega = g("O que pega")
        self.secoes = set(re.findall(r"[123]", g("Seção do DOU"))) or {"1", "2", "3"}
        self.problemas: list[str] = []

        self.termos = []          # (texto, regex)
        self.contexto = []        # regex que precisa estar perto
        self.contexto_so = set()  # termos (norm) a que o contexto se aplica; vazio = todos
        self.janela = JANELA_PADRAO
        self.exigidos = []        # regex que precisa estar em qualquer lugar do ato
        self.apagar = []          # trechos apagados antes de procurar o termo
        self.excluir = []         # trechos que tiram o ato
        self.cargo_rx = None
        self.nome_rx = None

        if self.tipo.startswith("cargo"):
            self._le_cargo(g("Termos (basta um)"))
        elif self.tipo.startswith("pessoa"):
            self._le_pessoa(g("Termos (basta um)"))
        else:
            self.termos = [(t, termo_rx(t)) for t in _lista(g("Termos (basta um)"))]
            self.termos = [(t, rx) for t, rx in self.termos if rx]
            if not self.termos:
                self.problemas.append("sem termos")
            self._le_contexto(g("Precisa ter também"))
        self._le_ignorar(g("Ignorar se tiver"))

    # -- Precisa ter também --------------------------------------------------
    def _le_contexto(self, txt: str):
        if txt in VAZIO:
            return
        m = re.match(r"(?is)\s*at[eé]\s*~?\s*(\d+)\s*caracteres\s+de\s*:\s*(.+)", txt)
        if m:
            self.janela = int(m.group(1))
            self.contexto = [termo_rx(t) for t in _lista(m.group(2))]
            self.contexto = [rx for rx in self.contexto if rx]
            return
        m = re.match(r"(?is)\s*s[oó]\s+para\s+as?\s+siglas?\s+(.+?)\s*:\s*(.+)", txt)
        if m:
            self.contexto_so = {norm(s) for s in re.split(r"[/,]|\be\b", m.group(1)) if norm(s)}
            resto = re.sub(r"(?i)\bpor perto\b\.?", "", m.group(2))
            partes = re.split(r",|\s+ou\s+|\s+e\s+", resto)
            self.contexto = [rx for rx in (termo_rx(p) for p in partes if p.strip()) if rx]
            return
        if re.search(r"\bno texto\b", txt, re.I):
            partes = re.split(r"\s+E\s+", txt)
            for p in partes:
                p = re.sub(r"(?i)\bno texto\b", "", p)
                rx = termo_rx(p)
                if rx:
                    self.exigidos.append(rx)
            if self.exigidos:
                return
        self.problemas.append(f'não entendi "Precisa ter também": {txt}')

    # -- Ignorar se tiver ----------------------------------------------------
    def _le_ignorar(self, txt: str):
        if txt in VAZIO:
            return
        for parte in _lista(txt):
            p = norm(parte)
            # Instruções descritivas cobertas pelo tipo da regra.
            if self.nome_rx is not None and "assinatura" in p:
                continue
            citado = re.search(r"['\"“‘]([^'\"”’]+)['\"”’]", parte)
            alvo = citado.group(1) if citado else parte
            rx = termo_rx(alvo)
            if not rx:
                continue
            contem_termo = any(t_rx.search(norm(alvo)) for _t, t_rx in self.termos)
            (self.apagar if contem_termo else self.excluir).append(rx)

    # -- Cargo -----------------------------------------------------------------
    # Como o cargo aparece escrito no ato, a partir do jeito curto da planilha.
    APELIDOS = [
        (r"\bdo mec\b", "(?:do ministerio da educacao|deste ministerio)"),
        (r"\bda segape\b", "(?:da segape|de gestao da informacao inovacao e avaliacao de politicas educacionais)"),
        (r"\bdepto\b", "departamento"),
    ]
    PREFIXOS = [  # cargo -> trecho opcional que o ato costuma pôr depois
        (r"ministr\[oa\]", "(?:de estado )?(?:chefe )?"),
        (r"diretora\?", "(?:da diretoria )?"),
        (r"coordenadora\? geral", "(?:da coordenacao geral )?"),
    ]

    def _le_cargo(self, txt: str):
        cargo = re.sub(r"(?i)^\s*cargo\s+ou\s+fun[cç][aã]o\s+de\s+", "", txt).strip()
        if not cargo:
            self.problemas.append("cargo vazio")
            return
        toks = _prepara(cargo).split()
        rx = " ".join(_token(t) for t in toks)
        for de, para in self.APELIDOS:
            rx = re.sub(de, para.replace("\\", "\\\\"), rx)
        for de, opcional in self.PREFIXOS:
            rx = re.sub(r"^(" + de + r") ", lambda m: m.group(1) + " " + opcional, rx)
        self.cargo_rx = re.compile("^" + rx + r"\b")
        self.cargo_txt = cargo

    # -- Pessoa ----------------------------------------------------------------
    def _le_pessoa(self, txt: str):
        toks = _prepara(txt).split()
        if len(toks) < 2:
            self.problemas.append("nome curto demais")
            return
        # Até 3 palavras entre as partes: "Leonardo Barchini" casa
        # "Leonardo Osvaldo Barchini Rosa".
        self.nome_rx = re.compile(r"\b" + r"(?: [a-z]+){0,3} ".join(map(re.escape, toks)) + r"\b")
        self.nome_txt = txt

    def __repr__(self):
        return f"<{self.id} {self.tipo} seções={sorted(self.secoes)}>"


def le_regras(sh) -> list[Regra]:
    ws = sh.worksheet(ABA_REGRAS)
    vals = ws.get_all_values()
    cab = [c.strip() for c in vals[0]]
    regras = []
    for r in vals[1:]:
        linha = {c: (r[i] if i < len(r) else "") for i, c in enumerate(cab)}
        if not (linha.get("ID") or "").strip():
            continue
        regras.append(Regra(linha))
    return regras


# ---------------------------------------------------------------------------
# Casamento
# ---------------------------------------------------------------------------

VERBO_PESSOAL_RX = re.compile(
    r"\b(?:nomear|nomeia|nomeado|nomeada|exonerar|exonera|exonerado|exonerada|designar|designa|"
    r"designado|designada|dispensar|dispensa|dispensado|dispensada|ceder|cede|cedido|cedida|"
    r"requisitar|requisita|requisitado|requisitada|aposentar|aposenta|tornar sem efeito|"
    r"torna sem efeito|substituir|substitui|nomeou|exonerou|designou|dispensou)\b")


def _paragrafos(texto: str) -> list[str]:
    return [p.strip() for p in re.split(r"\n+", texto or "") if p.strip()]


def _trecho(paragrafos: list[str], rx: re.Pattern, limite=500) -> str:
    """A frase com o termo (e a seguinte, se couber); o parágrafo inteiro só
    quando o termo atravessa frases."""
    for p in paragrafos:
        if not rx.search(norm(p)):
            continue
        frases = re.split(r"(?<=[.;])\s+", p)
        for k, f in enumerate(frases):
            if rx.search(norm(f)):
                t = f
                if k + 1 < len(frases) and len(t) + len(frases[k + 1]) < limite:
                    t += " " + frases[k + 1]
                break
        else:
            t = p
        return t[:limite] + ("…" if len(t) > limite else "")
    return ""


def _secao_num(secao: str) -> str:
    m = re.search(r"[123]", secao or "")
    return m.group(0) if m else ""


def casa_palavra(regra: Regra, texto_n: str, hier_n: str) -> re.Pattern | None:
    """Devolve o regex do termo que casou, ou None."""
    t = texto_n
    for rx in regra.apagar:
        t = rx.sub(" ", t)
    if any(rx.search(t) or rx.search(hier_n) for rx in regra.excluir):
        return None
    if regra.exigidos and not all(rx.search(t) for rx in regra.exigidos):
        return None
    for termo, rx in regra.termos:
        for m in rx.finditer(t):
            precisa = regra.contexto and (not regra.contexto_so or norm(termo) in regra.contexto_so)
            if not precisa:
                return rx
            janela = t[max(0, m.start() - regra.janela): m.end() + regra.janela]
            if any(c.search(janela) for c in regra.contexto):
                return rx
    return None


def casa_cargo(regra: Regra, it: dict, texto: str) -> list[dict]:
    import transicao_dou as T

    m = re.search(r"\bresolve\b|\bdecreta\b|\bart\.\s*1", texto, re.I)
    corpo = texto[m.start():] if m else texto
    verbos = list(T.VERBO_RX.finditer(corpo))
    achados = []
    for i, v in enumerate(verbos):
        fim = verbos[i + 1].start() if i + 1 < len(verbos) else len(corpo)
        seg = corpo[v.start():fim].strip()
        seg_n = norm(seg)
        if any(rx.search(seg_n) for rx in regra.excluir):
            continue
        verbo = re.sub(r"\s+", " ", v.group(1).upper())
        for cm in T.CARGO_RX.finditer(seg):
            cargo_n = norm(T._sem_codigo(cm.group("cargo")))
            if not regra.cargo_rx.match(cargo_n):
                continue
            fim_frase = re.compile(r"\.(?=\s|$)").search(seg, cm.end())
            trecho = re.sub(r"\s+", " ", seg[: fim_frase.end()] if fim_frase else seg)
            achados.append({"Movimento": T.MOVIMENTO.get(verbo, verbo.title()),
                            "Nome": T._caixa_normal(T._nome(seg, verbo)),
                            "Trecho": trecho[:800]})
            break
    return achados


def casa_pessoa(regra: Regra, texto: str) -> list[dict]:
    """Nome com verbo de pessoal antes, na mesma frase e a até 30 palavras.

    Frase = entre ponto, ponto e vírgula ou dois-pontos ("resolve:"). A
    assinatura fica sozinha depois do último ponto, sem verbo antes do nome.
    "Designar FULANA ... substituta eventual de ANA" é movimento da Fulana,
    não da Ana: com "substitut" entre o verbo e o nome, não conta."""
    import transicao_dou as T

    achados = []
    corrido = re.sub(r"\s+", " ", texto or "")
    for frase in re.split(r"(?<=[.;:])\s+", corrido):
        fn = norm(frase)
        m = regra.nome_rx.search(fn)
        if not m:
            continue
        verbos = list(VERBO_PESSOAL_RX.finditer(fn[: m.start()]))
        if not verbos:
            continue
        v = verbos[-1]
        meio = fn[v.end(): m.start()]
        if len(meio.split()) > 30 or "substitut" in meio:
            continue
        mov = {"nome": "Nomeação", "exon": "Exoneração", "desig": "Designação",
               "dispe": "Dispensa", "tor": "Sem efeito", "ced": "Cessão",
               "requis": "Requisição", "aposent": "Aposentadoria", "substitui": "Substituição"}
        movimento = next((r for k, r in mov.items() if v.group(0).startswith(k)), v.group(0).capitalize())
        achados.append({"Movimento": movimento, "Nome": T._caixa_normal(regra.nome_txt),
                        "Trecho": frase[:800]})
    return achados


# ---------------------------------------------------------------------------
# Varredura
# ---------------------------------------------------------------------------

# Órgãos cujos atos são baixados inteiros, porque o termo pode estar depois do
# resumo da listagem (extrato de contrato da SEB que cita Escolas Conectadas
# no objeto, edital do INEP sobre o Gestão Presente, acórdão da Anatel sobre o
# FUST). Universidade, instituto federal, colégio e hospital ficam de fora.
ORGAOS_RX = re.compile(
    r"^(minist[eé]rio da educa[cç][aã]o|minist[eé]rio das comunica[cç][oõ]es|"
    r"presid[eê]ncia da rep[uú]blica|atos do poder (executivo|legislativo))", re.I)
ORGAOS_FORA_RX = re.compile(
    r"universidade|instituto federal|centro federal|col[eé]gio|hospital|"
    r"empresa brasileira de servi[cç]os hospitalares", re.I)


def _precisa_baixar(it: dict, regras: list[Regra]) -> list[Regra]:
    """Regras que podem casar com o item, pelo título, resumo e órgão da listagem."""
    import transicao_dou as T

    sec = _secao_num(it.get("secao"))
    hier = it.get("hierarchyStr") or ""
    resumo_n = norm(f'{it.get("title", "")} {it.get("content", "")}')
    pessoal = None
    candidatas = []
    orgao_inteiro = bool(ORGAOS_RX.search(hier)) and not ORGAOS_FORA_RX.search(hier)
    for r in regras:
        if sec not in r.secoes or r.problemas:
            continue
        if r.cargo_rx is not None or r.nome_rx is not None:
            if pessoal is None:
                pessoal = T._candidato(it)
            if pessoal or (r.nome_rx is not None and r.nome_rx.search(resumo_n)):
                candidatas.append(r)
            continue
        if any(rx.search(resumo_n) for _t, rx in r.termos) or orgao_inteiro:
            candidatas.append(r)
    return candidatas


def varre(datas: list[str], regras: list[Regra]) -> list[dict]:
    fila = []
    vistos = set()
    pares = [(d, s) for d in datas for s in SECOES]
    lidas, _faltou = C.edicoes(pares)
    for data, sec in pares:
        if (data, sec) not in lidas:
            continue
        itens = lidas[(data, sec)]
        n = 0
        for it in itens:
            lk = C.link(it)
            if lk in vistos:
                continue
            cand = _precisa_baixar(it, regras)
            if cand:
                vistos.add(lk)
                fila.append((it, cand))
                n += 1
        print(f"[{data} {sec}] {len(itens)} itens, {n} a baixar")

    with ThreadPoolExecutor(max_workers=4) as ex:
        textos = list(ex.map(lambda x: C.texto_ato(C.link(x[0]), por_paragrafo=True), fila))

    achados = []
    for (it, cand), texto in zip(fila, textos):
        if not texto:
            texto = it.get("content") or ""
        base = {
            "Data": it.get("pubDate", ""),
            "Seção": it.get("secao", ""),
            "Órgão": it.get("hierarchyStr", ""),
            "Ato": it.get("title", ""),
            "Link": C.link(it),
        }
        paragrafos = _paragrafos(texto)
        texto_n = norm(f'{it.get("title", "")}\n{texto}')
        hier_n = norm(it.get("hierarchyStr"))
        for r in cand:
            linhas = []
            if r.cargo_rx is not None:
                linhas = casa_cargo(r, it, texto)
            elif r.nome_rx is not None:
                linhas = casa_pessoa(r, texto)
            else:
                rx = casa_palavra(r, texto_n, hier_n)
                if rx:
                    linhas = [{"Movimento": "", "Nome": "",
                               "Trecho": _trecho(paragrafos, rx) or (it.get("content") or "")[:500]}]
            for ln in linhas:
                achados.append({**base, **ln, "Regra": r.id, "Tema": r.tema,
                                "O que pega": r.o_que_pega})
    return achados


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

ORDEM_TEMAS = ["Conectividade", "Educação digital", "Gestão escolar", "Ambos", "Institucional"]
NOME_TEMA = {"Ambos": "Conectividade e gestão escolar"}


def _html(novos: list[dict], url: str, avisos: list[str]) -> str:
    import email_layout as L
    from dou_unificado import _titulo_dou

    # Um item por ato, com todas as regras que casaram nele.
    por_ato: dict[str, dict] = {}
    for a in novos:
        d = por_ato.setdefault(a["Link"], {**a, "regras": []})
        d["regras"].append(f'{a["Regra"]} · {a["O que pega"]}')

    temas = ORDEM_TEMAS + sorted({a["Tema"] for a in por_ato.values()} - set(ORDEM_TEMAS))
    corpo = ""
    for tema in temas:
        its = [a for a in por_ato.values() if a["Tema"] == tema]
        if not its:
            continue
        corpo += L.secao(NOME_TEMA.get(tema, tema or "Sem tema"), len(its))
        for a in sorted(its, key=lambda x: (x["Seção"], x["Ato"])):
            if a["Movimento"]:
                titulo = f'{a["Movimento"]}: {a["Nome"]}' if a["Nome"] else a["Movimento"]
            else:
                titulo = _titulo_dou(a["Ato"])
            orgao = (a["Órgão"] or "").split("/")[0]
            meta = " · ".join(x for x in (a["Data"], a["Seção"], orgao,
                                          _titulo_dou(a["Ato"]) if a["Movimento"] else "") if x)
            corpo += L.item(titulo, href=a["Link"], rotulo=" | ".join(a["regras"]),
                            meta=meta, texto=a["Trecho"][:450] + ("…" if len(a["Trecho"]) > 450 else ""))
    n = len(por_ato)
    rodape = ""
    if avisos:
        rodape = "Regras que não foram lidas nesta edição: " + "; ".join(L.esc(x) for x in avisos) + "."
    return L.pagina(
        "DOU · MegaEdu", "MegaEdu no DOU",
        datetime.now(C.TZ_BR).strftime("%d/%m/%Y"), corpo,
        nums=[(n, "atos" if n > 1 else "ato")],
        previa=" · ".join(_titulo_dou(a["Ato"]) for a in list(por_ato.values())[:3]),
        botoes=[("Abrir a planilha", url)],
        rodape=rodape,
    )


def envia(novos: list[dict], url: str, avisos: list[str]) -> None:
    from dou_unificado import _pick_recipients, _send_email

    dest = _pick_recipients("DESTINATARIOS_MEGAEDU", env_fallback="DESTINATARIOS_MEGAEDU")
    if not dest:
        print("Secret DESTINATARIOS_MEGAEDU vazio; sem email.")
        return
    n = len({a["Link"] for a in novos})
    _send_email(f"DOU | MegaEdu: {n} {'atos' if n > 1 else 'ato'}", _html(novos, url, avisos), dest)


# ---------------------------------------------------------------------------

def _chave(a: dict) -> tuple:
    return (a.get("Link", ""), (a.get("Regra") or "").strip().upper(),
            norm(a.get("Movimento")), norm(a.get("Nome")))


def main():
    sh = C.abre_planilha(SHEET_ID, "PLANILHA_MEGAEDU_DOU")
    regras = le_regras(sh)
    avisos = [f"{r.id} ({'; '.join(r.problemas)})" for r in regras if r.problemas]
    for a in avisos:
        print("aviso: regra não lida:", a)
    if "--regras" in sys.argv:
        for r in regras:
            print(r, "termos:", [t for t, _ in r.termos], "contexto:", [x.pattern for x in r.contexto],
                  "só:", r.contexto_so, "janela:", r.janela, "exigidos:", [x.pattern for x in r.exigidos],
                  "apagar:", [x.pattern for x in r.apagar], "excluir:", [x.pattern for x in r.excluir],
                  "cargo:", r.cargo_rx and r.cargo_rx.pattern, "nome:", r.nome_rx and r.nome_rx.pattern)
        return

    datas = C.datas_padrao()
    achados = varre(datas, regras)
    print(f"{len(achados)} achados em {', '.join(datas)}")
    if "--seco" in sys.argv:
        for a in achados:
            print(json.dumps({k: v for k, v in a.items() if k not in ("Trecho", "O que pega")},
                             ensure_ascii=False))
            print("   ", a["Trecho"][:300])
        return

    sh_out = sh if SHEET_ACHADOS == SHEET_ID else C.abre_planilha(SHEET_ACHADOS, "PLANILHA_MEGAEDU_ACHADOS")
    ws = C.aba(sh_out, ABA_ACHADOS, COLS, LARGURAS)
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ACHADOS}/edit#gid={ws.id}"
    novos = C.grava_novos(ws, COLS, achados, _chave)
    print(f"+{len(novos)} linhas em {ABA_ACHADOS}." if novos else "nada novo na planilha.")
    if novos:
        envia(novos, url, avisos)


if __name__ == "__main__":
    main()
