"""Moldura comum dos emails da Eixo (aqui, os do DOU).

Email não é página: Gmail corta <style>, flex e grid; Outlook só entende
tabela. Por isso tudo aqui é tabela com estilo inline, até 680px, alinhada à
esquerda e sem fundo em volta, para ler como corpo do email e não como cartão.
A fonte é Montserrat onde o cliente carrega (Apple Mail, iOS) e Arial no resto.

Cópia do compartilhado/email_layout.py do eixo-eleicoes. Mudou aqui,
muda lá.
"""

import html as _html

MARINHO = "#192D4E"
VINHO = "#962E4D"
GELO = "#F4F3EF"
TINTA = "#1B1B1A"
CINZA = "#6B6A65"
FILETE = "#E1E0D9"
FONTE = "'Montserrat',Arial,Helvetica,sans-serif"
# Gmail só mostra imagem por URL pública; o arquivo mora no repo eixo-eleicoes.
LOGO_URL = ("https://raw.githubusercontent.com/meduardamo/eixo-eleicoes/main/"
            "assets/email/logo_eixogov_branca.png")


def esc(v) -> str:
    return _html.escape(str(v or ""), quote=True)


def numeros(pares) -> str:
    """Linha de números do topo: [(valor, rótulo), ...]. Zero e vazio saem."""
    celulas = "".join(
        f'<td style="padding:0 28px 0 0;vertical-align:top">'
        f'<div style="font:700 26px/1 {FONTE};color:{MARINHO}">{esc(v)}</div>'
        f'<div style="font:600 10px/1.4 {FONTE};color:{CINZA};letter-spacing:1px;'
        f'text-transform:uppercase;padding-top:6px">{esc(r)}</div></td>'
        for v, r in pares if v not in (None, "", 0, "0")
    )
    if not celulas:
        return ""
    return (f'<table role="presentation" cellpadding="0" cellspacing="0" '
            f'style="margin:22px 0 0"><tr>{celulas}</tr></table>')


def secao(titulo, qtd=None) -> str:
    """Nível 2 da escala: maior que o título do item, para a seção mandar na lista."""
    conta = (f'<span style="font:600 14px/1 {FONTE};color:{CINZA}"> &nbsp;{qtd}</span>'
             if qtd is not None else "")
    return (f'<tr><td style="padding:40px 40px 0">'
            f'<div style="height:3px;width:32px;background:{VINHO};font-size:0;line-height:0">&nbsp;</div>'
            f'<div style="font:700 20px/1.3 {FONTE};color:{MARINHO};padding:10px 0 8px">'
            f'{esc(titulo)}{conta}</div></td></tr>')


def subsecao(titulo) -> str:
    """Nível 3: agrupa itens dentro da seção (UF, palavra-chave, tipo)."""
    return (f'<tr><td style="padding:20px 40px 0">'
            f'<div style="font:700 11px/1.4 {FONTE};color:{CINZA};letter-spacing:1.2px;'
            f'text-transform:uppercase;padding-bottom:4px">'
            f'{esc(titulo)}</div></td></tr>')


def item(titulo, href="", rotulo="", meta="", texto="", acao="", extra="") -> str:
    """Um item da lista. Texto já escapado entra em `extra` (HTML pronto)."""
    tit = esc(titulo)
    if href and href.startswith("http"):
        tit = (f'<a href="{esc(href)}" style="color:{TINTA};text-decoration:none">{tit}</a>')
    partes = []
    if rotulo:
        # Rótulo do item em caixa normal e vinho: não compete com a subseção em caixa alta.
        partes.append(f'<div style="font:600 12px/1.4 {FONTE};color:{VINHO};padding-bottom:4px">'
                      f'{esc(rotulo)}</div>')
    partes.append(f'<div style="font:700 16px/1.35 {FONTE};color:{TINTA}">{tit}</div>')
    if meta:
        partes.append(f'<div style="font:500 12px/1.5 {FONTE};color:{CINZA};padding-top:4px">{esc(meta)}</div>')
    if texto:
        partes.append(f'<div style="font:400 14px/1.6 {FONTE};color:#3A3936;padding-top:8px">{esc(texto)}</div>')
    if extra:
        partes.append(extra)
    if acao and href and href.startswith("http"):
        partes.append(f'<div style="padding-top:8px"><a href="{esc(href)}" style="font:600 12px/1 {FONTE};'
                      f'color:{MARINHO};text-decoration:underline">{esc(acao)}</a></div>')
    # Card: filete fino e canto leve. Sem barra colorida na lateral; a cor
    # fica para o rótulo e o link, que já dizem o que é cada coisa.
    return (f'<tr><td style="padding:6px 40px">'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'style="border:1px solid {FILETE};border-radius:8px;background:#ffffff">'
            f'<tr><td style="padding:16px 20px">{"".join(partes)}</td></tr></table></td></tr>')


def nota(html_pronto) -> str:
    """Bloco de instrução para a equipe, discreto, antes da lista."""
    return (f'<tr><td style="padding:24px 40px 0"><div style="font:400 13px/1.6 {FONTE};'
            f'color:#3A3936;background:{GELO};padding:14px 16px">{html_pronto}</div></td></tr>')


def botao(texto, href) -> str:
    return (f'<a href="{esc(href)}" style="display:inline-block;background:{MARINHO};color:#ffffff;'
            f'font:700 12px/1 {FONTE};letter-spacing:.5px;text-decoration:none;padding:12px 18px;'
            f'margin:0 8px 8px 0">{esc(texto)}</a>')


def pagina(kicker, titulo, data, corpo_linhas, nums=(), previa="", botoes=(),
           rodape="") -> str:
    """Email inteiro. `corpo_linhas` é a soma de secao()/item()/nota() (linhas de tabela)."""
    bts = "".join(botao(t, u) for t, u in botoes if u and u.startswith("http"))
    # A faixa diz de que rotina é o email; se repete o título, fica só a marca.
    kicker = "" if kicker.strip().lower() == titulo.strip().lower() else kicker
    bts_html = f'<tr><td style="padding:28px 40px 8px">{bts}</td></tr>' if bts else ""
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
  <div style="font:600 12px/1.4 {FONTE};color:{CINZA}">{esc(data)}</div>
  <div style="font:700 28px/1.2 {FONTE};color:{TINTA};padding-top:6px">{esc(titulo)}</div>
  {numeros(nums)}
</td></tr>
{bts_html}
{corpo_linhas}
<tr><td style="padding:32px 40px 36px;font:400 11px/1.6 {FONTE};color:{CINZA}">
  {rod}Envio automático da Eixo
</td></tr>
</table></body></html>"""
