# DOU

Raspagem do Diário Oficial da União (DOU) e atualização em Google Sheets, incluindo edições extras e uma etapa auxiliar de alinhamento.

## Arquivos principais
- `dou_unificado.py`: rotina principal de raspagem do DOU e lógica de edições extras (captura/atualização)
- `alinhamento_dou.py`: rotinas auxiliares (ex.: classificação/alinhamento)
- `.github/workflows/main.yml`: execução automatizada via GitHub Actions
- `requirements.txt`: dependências Python
