# Airflow Medallion Architecture with BigQuery

Uma arquitetura completa de pipeline de dados usando Apache Airflow com o padrão Medallion (Bronze → Silver → Gold) e Google BigQuery.

## 📋 Visão Geral

Este projeto implementa uma solução escalável e modular de data pipeline com:

- **Bronze Layer**: Extração de dados via APIs externas usando Python
- **Silver Layer**: Transformação e limpeza de dados usando SQL
- **Gold Layer**: Agregações e métricas para consumo em dashboards

### Características

✅ **Configuração via YAML** - DAGs e tasks definidas em arquivos YAML, não código  
✅ **SQL em Arquivos** - Queries SQL separadas e versionáveis  
✅ **Extratores Plugáveis** - Fácil adicionar novos conectores para APIs  
✅ **Ambiente-aware** - Suporte para dev/staging/prod com prefixos automáticos  
✅ **Escalável** - Estrutura pronta para crescimento  
✅ **Auditável** - Todas as transformações documentadas e versionadas

## 📁 Estrutura de Diretórios

```
.
├── dags/                           # DAGs do Airflow
│   ├── bronze_dag.py              # DAG de extração (Bronze)
│   ├── silver_dag.py              # DAG de transformação (Silver)
│   ├── gold_dag.py                # DAG de agregação (Gold)
│   └── common/
│       ├── config_loader.py       # Carregador de configurações
│       ├── sql_executor.py        # Executor de SQLs
│       └── utils.py               # Utilidades comuns
├── extractors/                     # Extratores de dados (Bronze)
│   ├── base_extractor.py          # Classe base
│   └── connectors/
│       └── api_connector.py       # Exemplo de conector de API
├── configs/                        # Configurações
│   ├── env.yaml                   # Config global
│   ├── bronze/
│   │   ├── dag_config.yaml        # Config da DAG bronze
│   │   └── extractors/            # Configs dos extratores
│   ├── silver/
│   │   ├── dag_config.yaml        # Config da DAG silver
│   │   └── queries/               # SQLs de transformação
│   └── gold/
│       ├── dag_config.yaml        # Config da DAG gold
│       └── queries/               # SQLs de agregação
├── requirements.txt               # Dependências Python
├── .env.example                   # Exemplo de variáveis de ambiente
└── README.md                      # Este arquivo
```

## 🚀 Início Rápido

### 1. Clonar/Setup do Projeto

```bash
# Clone ou crie o diretório do projeto
mkdir airflow-medallion && cd airflow-medallion

# Copie os arquivos para este diretório
```

### 2. Criar Ambiente Virtual

```bash
# Python 3.9+
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

### 3. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 4. Configurar Variáveis de Ambiente

```bash
# Copiar arquivo de exemplo
cp .env.example .env

# Editar .env com suas credenciais
# - GCP_PROJECT_ID: seu projeto no Google Cloud
# - GOOGLE_APPLICATION_CREDENTIALS: caminho para arquivo de credenciais JSON
# - Outras variáveis específicas da sua setup
```

### 5. Obter Credenciais do GCP

```bash
# Criar service account e salvar JSON
# 1. Acesse Google Cloud Console
# 2. Crie um service account com permissões de BigQuery
# 3. Baixe a chave privada em JSON
# 4. Defina GOOGLE_APPLICATION_CREDENTIALS para apontar para este arquivo
```

### 6. Inicializar Airflow

```bash
# Definir diretório base do Airflow
export AIRFLOW_HOME=$(pwd)

# Inicializar banco de dados
airflow db init

# Criar usuário admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Iniciar webserver (acesso em http://localhost:8080)
airflow webserver -p 8080

# Em outro terminal, iniciar scheduler
airflow scheduler
```

## 🔧 Configuração

### Arquivo de Configuração Global (`configs/env.yaml`)

```yaml
environment: dev  # ou prod
dataset_prefix: "dev"  # Prefixo para datasets

airflow:
  owner: "data_team"
  email: "data@company.com"
  gcp_project_id: "seu-projeto"
```

### Configuração de DAG (`configs/{layer}/dag_config.yaml`)

Exemplo para a camada Silver:

```yaml
dag_id: silver_layer
schedule_interval: "0 2 * * *"
catchup: false

tasks:
  - task_id: transform_customers
    type: sql
    sql_file: customers.sql
    timeout_minutes: 30
    
  - task_id: transform_orders
    type: sql
    sql_file: orders.sql
    downstream_task: enrich_orders
```

### Arquivos SQL

SQL files em `configs/{layer}/queries/` são executados como-is contra BigQuery:

```sql
-- configs/silver/queries/customers.sql
CREATE OR REPLACE TABLE `{project}.dev_silver.customers` AS
SELECT
    customer_id,
    TRIM(UPPER(customer_name)) AS customer_name,
    ...
FROM `{project}.dev_bronze.customers`
WHERE customer_id IS NOT NULL
```

O placeholder `{project}` é automaticamente substituído pelo ID do projeto.

## 🔌 Criar um Novo Conector (Bronze)

### 1. Criar Extrator

```python
# extractors/connectors/my_api_connector.py
from extractors.base_extractor import BaseExtractor
import requests
import pandas as pd

class MyApiConnector(BaseExtractor):
    def extract(self) -> pd.DataFrame:
        """Extrai dados da API"""
        api_url = self.config['api']['base_url']
        api_key = self.config['api']['api_key']
        
        response = requests.get(
            f"{api_url}/data",
            headers={"Authorization": f"Bearer {api_key}"}
        )
        
        return pd.DataFrame(response.json())
```

### 2. Criar Configuração

```yaml
# configs/bronze/extractors/my_api_config.yaml
api:
  base_url: "https://api.example.com"
  api_key: "${AIRFLOW_VAR_API_KEY}"
  timeout_seconds: 30
```

### 3. Adicionar à DAG

```yaml
# configs/bronze/dag_config.yaml
tasks:
  - task_id: extract_my_data
    type: extractor
    extractor_module: extractors.connectors.my_api_connector
    config_file: my_api_config.yaml
```

## 📊 Adicionar Novas Transformações (Silver/Gold)

### 1. Criar Arquivo SQL

```sql
-- configs/silver/queries/new_table.sql
CREATE OR REPLACE TABLE `{project}.dev_silver.new_table` AS
SELECT
    ...
FROM `{project}.dev_bronze.source_table`
```

### 2. Adicionar à Configuração

```yaml
# configs/silver/dag_config.yaml
tasks:
  - task_id: transform_new_table
    type: sql
    sql_file: new_table.sql
    timeout_minutes: 30
```

Pronto! A DAG será automaticamente atualizada.

## 🧪 Testes e Validação

### Testar Carregamento de Configurações

```python
from dags.common.config_loader import get_config_loader

loader = get_config_loader()

# Carregar configs
env_config = loader.load_env_config()
dag_config = loader.load_dag_config("silver")
sql = loader.load_sql_file("silver", "customers.sql")

print(f"Environment: {env_config['environment']}")
print(f"DAG ID: {dag_config['dag_id']}")
print(f"SQL loaded: {len(sql)} characters")
```

### Listar DAGs

```bash
airflow dags list
```

### Testar uma Task

```bash
# Testar uma task sem executar toda a DAG
airflow tasks test silver_layer transform_customers 2024-01-01
```

## 🚨 Solução de Problemas

### DAG não aparece no Airflow

1. Verificar se os arquivos YAML estão no lugar correto
2. Validar sintaxe YAML (sem tabs, indentação correta)
3. Verificar logs: `airflow dags list --output table`

### Erro ao carregar módulo de extrator

```
ImportError: No module named 'extractors.connectors.my_connector'
```

- Verificar se o arquivo existe em `extractors/connectors/`
- Verificar se a classe está nomeada corretamente (CamelCase do nome do arquivo)
- Exemplo: `my_api_connector.py` → classe `MyApiConnector`

### Erro de credenciais GCP

```
google.auth.exceptions.DefaultCredentialsError
```

1. Definir `GOOGLE_APPLICATION_CREDENTIALS`
2. Ou configurar Application Default Credentials: `gcloud auth application-default login`

### Datasets não encontrados

Verificar se o prefixo de dataset está correto:

```bash
# Listar datasets
gcloud bigquery datasets list --project=seu-projeto

# Datasets esperados
# dev_bronze
# dev_silver
# dev_gold
```

## 📚 Referências

- [Apache Airflow Documentation](https://airflow.apache.org/)
- [Google Cloud BigQuery](https://cloud.google.com/bigquery/docs)
- [Medallion Architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion-architecture)

## 📝 Notas Importantes

### Performance

- Use pools para limitar execução paralela de queries pesadas
- Configure `default_pool` com limite apropriado
- Monitore custo de queries BigQuery

### Segurança

- Não commit do arquivo `.env`
- Use Airflow Variables para secrets em produção
- Mantenha credenciais GCP seguras

### Manutenção

- Revise e archive dados antigos regularmente
- Monitore custos de armazenamento BigQuery
- Mantenha histórico de mudanças em Git

## 📧 Suporte

Para dúvidas ou problemas, consulte:

1. Logs do Airflow: `AIRFLOW_HOME/logs/`
2. Console do Google Cloud: monitorar jobs BigQuery
3. Documentação do projeto

---

**Versão**: 1.0  
**Última atualização**: 2024
