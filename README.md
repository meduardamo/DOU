# DOU

Coleta e organização de publicações do Diário Oficial da União (DOU), incluindo:
- raspagem do DOU (rotina principal)
- captura/atualização de edições extras
- etapa de “alinhamento”
- escrita/atualização em Google Sheets

## Arquivos principais
- `raspagemdou.py`: rotina principal de raspagem do DOU
- `dou_extra.py`: lógica de edições extras (captura/atualização)
- `alinhamento_dou.py`: rotinas auxiliares (ex.: classificação/alinhamento, se você usa)
- `.github/workflows/main.yml`: execução automatizada via GitHub Actions
- `requirements.txt`: dependências Python
