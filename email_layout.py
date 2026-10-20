"""Moldura comum dos emails da Eixo (aqui, os do DOU).

Email não é página: Gmail corta <style>, flex e grid; Outlook só entende
tabela. Por isso tudo aqui é tabela com estilo inline, até 680px, alinhada à
esquerda e sem fundo em volta, para ler como corpo do email e não como cartão.
A fonte é Montserrat onde o cliente carrega (Apple Mail, iOS) e Arial no resto.

Cópia do compartilhado/email_layout.py do eixo-eleicoes. Mudou aqui,
muda lá.
"""

import html as _html
import re as _re
import unicodedata as _ud

# Paleta igual à do manual da área (seção 5): planilha, documento e email usam os mesmos valores.

MARINHO = "#192D4E"
VINHO = "#962E4D"
GELO = "#F4F3EF"
TINTA = "#202124"
CINZA = "#6B6B6B"
FILETE = "#DADAD4"
FONTE = "'Montserrat',Arial,Helvetica,sans-serif"
# Gmail só mostra imagem por URL pública; o arquivo mora no repo eixo-eleicoes.
LOGO_URL = ("https://raw.githubusercontent.com/meduardamo/eixo-eleicoes/main/"
            "assets/email/logo_eixogov_branca.png")


def esc(v) -> str:
    return _html.escape(str(v or ""), quote=True)


def numeros(pares) -> str:
    """Números do topo como texto corrido: "259 atos novos · 13 clientes". Zero e vazio saem."""
    return " · ".join(f"{esc(v)} {esc(r)}" for v, r in pares
                      if v not in (None, "", 0, "0"))


def _ancora(titulo) -> str:
    t = _ud.normalize("NFKD", str(titulo)).encode("ascii", "ignore").decode().lower()
    return "s-" + (_re.sub(r"[^a-z0-9]+", "-", t).strip("-") or "secao")


def secao(titulo, qtd=None) -> str:
    """Nível 2 da escala: maior que o título do item, para a seção mandar na lista.

    O comentário no começo é lido por pagina() para montar o sumário."""
    conta = (f'<span style="font:600 14px/1 {FONTE};color:{CINZA}"> &nbsp;{qtd}</span>'
             if qtd is not None else "")
    marca = esc(titulo).replace("--", "-")
    return (f'<!--secao:{marca}|{"" if qtd is None else qtd}-->'
            f'<tr><td id="{_ancora(titulo)}" style="padding:40px 40px 0">'
            f'<a name="{_ancora(titulo)}"></a>'
            f'<div style="height:1px;background:{FILETE};font-size:0;line-height:0">&nbsp;</div>'
            f'<div style="font:700 20px/1.3 {FONTE};color:{MARINHO};padding:16px 0 4px">'
            f'{esc(titulo)}{conta}</div></td></tr>')


def sumario(corpo_linhas) -> str:
    """Lista das seções com a contagem, cada uma levando à seção. Só com 2 ou mais."""
    secoes = _re.findall(r"<!--secao:(.*?)\|(.*?)-->", corpo_linhas)
    if len(secoes) < 2:
        return ""
    linhas = "".join(
        f'<tr><td style="padding:7px 0;border-top:1px solid {FILETE}">'
        f'<a href="#{_ancora(_html.unescape(t))}" style="font:600 13px/1.4 {FONTE};color:{TINTA};'
        f'text-decoration:none">{t}</a></td>'
        f'<td align="right" style="padding:7px 0;border-top:1px solid {FILETE};'
        f'font:600 13px/1.4 {FONTE};color:{CINZA}">{q}</td></tr>'
        for t, q in secoes)
    return (f'<tr><td style="padding:28px 40px 0">'
            f'<div style="font:700 14px/1.4 {FONTE};color:{MARINHO};padding-bottom:6px">Sumário</div>'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0">{linhas}</table>'
            f'</td></tr>')


def subsecao(titulo) -> str:
    """Nível 3: agrupa itens dentro da seção (UF, palavra-chave, tipo)."""
    return (f'<tr><td style="padding:20px 40px 0">'
            f'<div style="font:700 11px/1.4 {FONTE};color:{CINZA};letter-spacing:1.2px;'
            f'text-transform:uppercase;padding-bottom:4px">'
            f'{esc(titulo)}</div></td></tr>')


def item(titulo, href="", rotulo="", meta="", texto="", acao="", extra="") -> str:
    """Um item da lista. Texto já escapado entra em `extra` (HTML pronto).

    `acao` fica na assinatura por compatibilidade e não aparece: o título já é o link."""
    tit = esc(titulo)
    cor = TINTA
    if href and href.startswith("http"):
        cor = MARINHO
        tit = (f'<a href="{esc(href)}" style="color:{MARINHO};text-decoration:none">{tit}</a>')
    partes = []
    if rotulo:
        partes.append(f'<div style="font:600 12px/1.4 {FONTE};color:{VINHO};padding-bottom:4px">'
                      f'{esc(rotulo)}</div>')
    partes.append(f'<div style="font:700 15px/1.4 {FONTE};color:{cor}">{tit}</div>')
    if meta:
        partes.append(f'<div style="font:500 12px/1.5 {FONTE};color:{CINZA};padding-top:3px">{esc(meta)}</div>')
    if texto:
        partes.append(f'<div style="font:400 14px/1.6 {FONTE};color:{TINTA};padding-top:6px">{esc(texto)}</div>')
    if extra:
        partes.append(extra)
    # Lista corrida: fio fino embaixo de cada item, sem caixa.
    return (f'<tr><td style="padding:0 40px"><div style="padding:16px 0;border-bottom:1px solid {FILETE}">'
            f'{"".join(partes)}</div></td></tr>')


def nota(html_pronto) -> str:
    """Bloco de instrução para a equipe, discreto, antes da lista."""
    return (f'<tr><td style="padding:24px 40px 0"><div style="font:400 13px/1.6 {FONTE};'
            f'color:{TINTA};background:{GELO};padding:14px 16px">{html_pronto}</div></td></tr>')


def botao(texto, href) -> str:
    return (f'<a href="{esc(href)}" style="font:600 13px/1.4 {FONTE};color:{MARINHO};'
            f'text-decoration:underline;margin-right:20px">{esc(texto)}</a>')


def pagina(kicker, titulo, data, corpo_linhas, nums=(), previa="", botoes=(),
           rodape="") -> str:
    """Email inteiro. `corpo_linhas` é a soma de secao()/item()/nota() (linhas de tabela)."""
    bts = "".join(botao(t, u) for t, u in botoes if u and u.startswith("http"))
    # A faixa diz de que rotina é o email; se repete o título, fica só a marca.
    kicker = "" if kicker.strip().lower() == titulo.strip().lower() else kicker
    nums_txt = numeros(nums)
    linha_data = esc(data) + (f" · {nums_txt}" if nums_txt else "")
    bts_html = f'<div style="padding-top:14px">{bts}</div>' if bts else ""
    rod = f'<div style="padding-bottom:8px">{rodape}</div>' if rodape else ""
    return f"""<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light only">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap" rel="stylesheet">
<title>{esc(titulo)}</title></head>
<body style="margin:0;padding:0;background:#ffffff">
<div style="display:none;max-height:0;overflow:hidden;opacity:0">{esc(previa)}{"&#847;&zwnj;&nbsp;" * 90}</div>
<table role="presentation" width="680" cellpadding="0" cellspacing="0"
       style="width:100%;max-width:680px;background:#ffffff">
<tr><td style="background:{MARINHO};padding:18px 40px">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
  <td><img src="{LOGO_URL}" width="74" height="48" alt="EixoGov"
           style="display:block;border:0;width:74px;height:48px"></td>
  <td align="right" style="font:600 10px/1 {FONTE};color:#C9CFDA;letter-spacing:1.5px;text-transform:uppercase">{esc(kicker)}</td>
  </tr></table></td></tr>
<tr><td style="height:4px;background:{VINHO};font-size:0;line-height:0">&nbsp;</td></tr>
<tr><td style="padding:34px 40px 0">
  <div style="font:600 12px/1.4 {FONTE};color:{CINZA}">{linha_data}</div>
  <div style="font:700 28px/1.2 {FONTE};color:{MARINHO};padding-top:6px">{esc(titulo)}</div>
  {bts_html}
</td></tr>
{sumario(corpo_linhas)}
{corpo_linhas}
<tr><td style="padding:32px 40px 36px;font:400 11px/1.6 {FONTE};color:{CINZA}">
  {rod}Envio automático da Eixo
</td></tr>
</table></body></html>"""
