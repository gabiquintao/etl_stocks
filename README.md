# TP01 - ETL Stocks

## Identificação do(s) Autor(es)

- **Nome:** Gabriel Quintão de Araújo
- **Número:** 27978

---

## Descrição do Projeto

Este projeto implementa um sistema ETL (Extract, Transform, Load) para análise de dados de ações (stocks). O sistema extrai dados históricos de preços e indicadores técnicos, processa e transforma essas informações, e carrega os resultados numa base de dados PostgreSQL para análise posterior. Além disso, inclui outras vertentes como sistema email, dashboard Node-RED, entre outras.

---

## Estrutura de Ficheiros

### Documentação

- [`report/27978_report.pdf`](report/27978_report.pdf) - Relatório final do projeto

### Transformações ETL

#### Pentaho - Transformações

- [`pentaho/transformations/extract_api_data.ktr`](pentaho/transformations/extract_api_data.ktr) - Extração de dados de APIs
- [`pentaho/transformations/clean_and_normalize_data.ktr`](pentaho/transformations/clean_and_normalize_data.ktr) - Limpeza e normalização
- [`pentaho/transformations/calculate_technical_indicators.ktr`](pentaho/transformations/calculate_technical_indicators.ktr) - Cálculo de indicadores técnicos
- [`pentaho/transformations/load_to_postgresql.ktr`](pentaho/transformations/load_to_postgresql.ktr) - Carregamento para PostgreSQL

#### Pentaho - Jobs

- [`pentaho/jobs/master_etl_pipeline.kjb`](pentaho/jobs/master_etl_pipeline.kjb) - Job principal do pipeline ETL

#### Logs

- [`pentaho/logs`](pentaho/logs) - Logs depois de executar o Job

### Dados

#### Dados Temporários

- [`pentaho/data/temp_data`](pentaho/data/temp_data) - Dados temporários das transformações

## Node-RED

- [`dataint/ETL_STOCKS/nodered/comparative_table.json`](dataint/ETL_STOCKS/nodered/comparative_table.json) - Fluxo para tabelas comparativas
- [`dataint/ETL_STOCKS/nodered/db.json`](dataint/ETL_STOCKS/nodered/db.json) - Configuração de ligação à BD
- [`dataint/ETL_STOCKS/nodered/price_history.json`](dataint/ETL_STOCKS/nodered/price_history.json) - Processamento de histórico de preços
- [`dataint/ETL_STOCKS/nodered/rsi_indicators.json`](dataint/ETL_STOCKS/nodered/rsi_indicators.json) - Cálculo de indicadores RSI

### Scripts SQL

- [`sql/db.sql`](sql/db.sql) - Schema da base de dados
- [`sql/text_queries.sql`](sql/text_queries.sql) - Queries úteis
- [`sql/useful_queries.sql`](sql/useful_queries.sql) - Queries de análise

### Scripts de Execução

- [`start-nodered.bat`](start-nodered.bat) - Script para iniciar Node-RED (Windows)

### Outros

- [`.gitignore`](.gitignore) - Ficheiros ignorados pelo Git
- [`LICENSE`](LICENSE) - Licença do projeto

---

## Ferramentas Utilizadas

### Software Utilizado:

1. **Pentaho Data Integration (PDI/Kettle)** - versão 10.2
2. **Node-RED** - versão 4.1.1
3. **PostgreSQL** - versão 18
4. **Node.js** - versão 22.19.0

### Bibliotecas e Dependências:

- Node-RED: `node-red-contrib-postgres`

---

## Como Executar a Solução

### 1. **Pré-requisitos**

#### Instalação do PostgreSQL:

```bash
# Criar base de dados
createdb etl_stocks

# Executar schema
psql -d etl_stocks -f sql/db.sql
```

#### Instalação do Node-RED:

```bash
npm install -g node-red
npm install node-red-contrib-postgres
```

### 2. **Configuração**

#### Base de Dados:

1. Editar ficheiros de configuração com credenciais corretas
2. Garantir que PostgreSQL está a correr na porta 5432 (default)
3. Executar o script [`sql/db.sql`](sql/db.sql) para criar as tabelas

#### Node-RED:

1. Executar: `start-nodered.bat` (Windows) ou `node-red` (Linux/Mac)
2. Importar flows da pasta [`nodered/`](nodered/)

### 3. **Execução do Pipeline ETL**

#### Pentaho GUI

1. Abrir Spoon (interface gráfica do Pentaho)
2. Abrir o ficheiro [`master_etl_pipeline.kjb`](dataint/ETL_STOCKS/jobs/master_etl_pipeline.kjb)
3. Clicar em "Run"
4. Verificar logs na consola

#### Node-RED

1. Iniciar Node-RED: `node-red` ou executar [`start-nodered.bat`](start-nodered.bat)
2. Aceder a http://localhost:1880
3. Fazer Deploy e executar os flows

## Demonstração em Vídeo

🎥 **Link para o vídeo de demonstração:**

https://youtu.be/ECrITdBmVKI

**QR Code para o vídeo:**

![My QR Code](ETL Stocks - ISI - Gabriel Araújo 27978.png)

## Dados Utilizados

### Fonte dos Dados:

- **API:** Alpha Vantage
- **Ações analisadas:**
  "AAPL"
  "MSFT"
  "GOOGL"
  "AMZN"
  "TSLA"
  "META"
  "NVDA"
  "JPM"
  "V"
  "WMT"
  "IBM"
