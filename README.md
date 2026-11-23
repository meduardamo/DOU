# DOU

Raspagem do Diário Oficial da União (DOU) e atualização em Google Sheets, incluindo edições extras e uma etapa auxiliar de alinhamento.

## Arquivos principais
- `dou_unificado.py`: rotina principal de raspagem do DOU e lógica de edições extras (captura/atualização)
- `alinhamento_dou.py`: rotinas auxiliares (ex.: classificação/alinhamento)
- `transicao_dou.py`: nomeação, exoneração e designação nos cargos acompanhados na transição federal, e atos de desenho ministerial (Lei 14.600, estrutura regimental); grava na aba "DOU federal" da planilha de transição e manda email quando há ato novo (workflow `transicao.yml`, secrets `PLANILHA_TRANSICAO` e `DESTINATARIOS_TRANSICAO`)
- `megaedu_dou.py`: raspagem própria da MegaEdu; lê as regras da aba "Regras DOU" da planilha de monitoramento federal (palavra-chave com termo de contexto e exclusão, troca de titular por cargo, movimentação por nome de pessoa), grava na aba "Atos encontrados" e manda email quando há ato novo (workflow `megaedu.yml`, secrets `PLANILHA_MEGAEDU_DOU` (regras), `PLANILHA_MEGAEDU_ACHADOS` (onde gravar; vazio = planilha das regras) e `DESTINATARIOS_MEGAEDU`). `--regras` mostra como cada regra foi lida; `--seco` raspa sem gravar nem mandar email
- `dou_comum.py`: peças comuns da transição e da MegaEdu (listagem de uma seção, texto do ato, planilha no padrão Eixo, gravação sem repetir)
- `.github/workflows/`: `regular.yml` e `extra.yml` (DOU por cliente), `transicao.yml`, `megaedu.yml`, `cargos_dou.yml`, `exemplo.yml`
- `requirements.txt`: dependências Python
