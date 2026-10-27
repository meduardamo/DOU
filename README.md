# DOU

Raspagem do Diário Oficial da União (DOU) e atualização em Google Sheets, incluindo edições extras e uma etapa auxiliar de alinhamento.

## Arquivos principais
- `dou_unificado.py`: rotina principal de raspagem do DOU e lógica de edições extras (captura/atualização)
- `alinhamento_dou.py`: rotinas auxiliares (ex.: classificação/alinhamento)
- `transicao_dou.py`: nomeação, exoneração e designação nos cargos acompanhados na transição federal, e atos de desenho ministerial (Lei 14.600, estrutura regimental); grava na aba "DOU federal" da planilha de transição e manda email quando há ato novo (workflow `transicao.yml`, secrets `PLANILHA_TRANSICAO` e `DESTINATARIOS_TRANSICAO`)
- `.github/workflows/main.yml`: execução automatizada via GitHub Actions
- `requirements.txt`: dependências Python
