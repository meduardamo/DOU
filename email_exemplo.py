"""Exemplo do email DOU Clientes para um endereço só, sem raspar nem gravar.

Lê a última data gravada nas abas da planilha de clientes e monta o email com
o mesmo layout da edição regular. Assunto começa com [EXEMPLO].
Uso: python email_exemplo.py endereco@exemplo.com
"""

import os
import sys
from datetime import datetime

import dou_unificado as D


def _data(s: str):
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            pass
    return None


def main(para: str) -> None:
    plan_id = os.getenv("PLANILHA_CLIENTES", "")
    sh = D._gs_client_from_env().open_by_key(plan_id)
    linhas = []
    for cli in D.CLIENT_KEYWORDS:
        try:
            vals = sh.worksheet(cli).get_all_values()
        except Exception:
            continue
        linhas += [r for r in vals[1:] if r and _data(r[0])]
    if not linhas:
        print("Planilha de clientes sem linhas com data.")
        return
    ultima = max(_data(r[0]) for r in linhas)
    c = D.COLS_CLIENTE
    por_cliente: dict[str, list[dict]] = {}
    for r in linhas:
        if _data(r[0]) != ultima:
            continue
        r = r + [""] * (len(c) - len(r))
        por_cliente.setdefault(r[c.index("Cliente")], []).append({
            "keyword": r[c.index("Palavra-chave")], "title": r[c.index("Portaria")],
            "href": r[c.index("Link")], "abstract": r[c.index("Resumo")],
            "secao": r[c.index("Seção")]})
    dia = ultima.strftime("%d/%m/%Y")
    html = D._build_html_email_clientes(por_cliente, plan_id, "Edição Regular", dia)
    total = sum(len(v) for v in por_cliente.values())
    D._send_email(f"[EXEMPLO] DOU Regular - {dia} | Clientes: {total}", html, [para])


if __name__ == "__main__":
    main(sys.argv[1])
