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
├── configs/                        # Configurações
│   ├── env.yaml                   # Config global
│   ├── bronze/
│   │   ├── dag_config.yaml        # Config da DAG bronze
│   │   └── queries/            # Configs dos extratores
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

1. As dags são configuradas por arquivos YAMLS chamados de dag_config.yaml
2. O código de criação de cada tabela deve ser inserido na pasta configs/<camada>/queries em formato .sql
3. O processo de leitura da DAG é automático, todas as tabelas configuradas em dag_config irão aparecer na DAG
4. No airflow você pecisa configurar a conexão com o Bigquery, chamando-a de: google_cloud_default
5. É necessário configurar o pool no Airflow com o nome de: bigquery_queries

