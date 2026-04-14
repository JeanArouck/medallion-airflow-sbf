# """
# DAG de Gold - Agregações e métricas

# Esta DAG gerencia a criação de agregações, métricas e dados prontos
# para consumo em dashboards e análises (camada gold).

# Configurações são carregadas do arquivo: configs/gold/dag_config.yaml
# """

# from datetime import timedelta
# from airflow import DAG
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# from datetime import datetime, timedelta
# import logging

# from common.config_loader import get_config_loader
# from common.utils import (
#     build_task_dependencies,
#     validate_dag_config,
#     validate_task_config,
# )

# logger = logging.getLogger(__name__)

# # Configurações
# config_loader = get_config_loader()
# env_config = config_loader.load_env_config()
# dag_config = config_loader.load_dag_config("gold")

# # Validações
# validate_dag_config(dag_config)

# # Parâmetros globais
# default_args = {
#     "owner": env_config["airflow"]["owner"],
#     "email": env_config["airflow"]["email"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": env_config["airflow"]["default_retries"],
#     "retry_delay": timedelta(
#         minutes=env_config["airflow"]["default_retry_delay_minutes"]
#     ),
# }

# # Definição da DAG
# dag = DAG(
#     dag_id=dag_config["dag_id"],
#     description=dag_config.get("description", "Gold Layer DAG"),
#     default_args=default_args,
#     schedule=dag_config.get("schedule_interval", None),
#     catchup=dag_config.get("catchup", False),
#     tags=dag_config.get("tags", ["medallion", "gold"]),
#     start_date=datetime(2026, 4, 12),
# )

# # Criar tasks dinamicamente baseado na configuração
# tasks = {}

# for task_config in dag_config["tasks"]:
#     validate_task_config(task_config, "gold")

#     task_id = task_config["task_id"]
#     timeout_minutes = task_config.get("timeout_minutes", 30)
#     pool = task_config.get("pool", "default_pool")

#     if task_config["type"] == "sql":
#         # Carregar arquivo SQL
#         sql_filename = task_config["sql_file"]
#         sql = config_loader.load_sql_file("gold", sql_filename)

#         # Obter dataset com prefixo de ambiente
#         dataset = config_loader.get_dataset_name("gold")
#         project_id = env_config["airflow"]["gcp_project_id"]

#         # Substituir placeholders no SQL
#         sql = sql.replace("{project}", project_id)

#         # Configuração do job BigQuery
#         job_config = {
#             "query": {
#                 "query": sql,
#                 "useLegacySql": False,
#                 "allowLargeResults": True,
#                 "useQueryCache": True,
#             }
#         }

#         # Criar operador BigQuery
#         task = BigQueryInsertJobOperator(
#             task_id=task_id,
#             configuration=job_config,
#             location=env_config["airflow"].get("bigquery_location", "US"),
#             gcp_conn_id=env_config["airflow"]["gcp_conn_id"],
#             project_id=project_id,
#             execution_timeout=timedelta(minutes=timeout_minutes),
#             pool=pool,
#             dag=dag,
#         )
#         tasks[task_id] = task
#         logger.info(f"Task criada: {task_id} (tipo: sql, arquivo: {sql_filename})")

# # Construir dependências entre tasks baseado no YAML
# build_task_dependencies(tasks, dag_config["tasks"])

# logger.info(f"DAG '{dag_config['dag_id']}' criada com sucesso!")


"""
DAG de Gold - Agregações e métricas
Versão otimizada para Airflow 3 (sem parse pesado)
"""

from datetime import datetime, timedelta
from airflow import DAG


def create_dag():
    # 🔥 Lazy imports (evita timeout no parse)
    from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryInsertJobOperator,
    )
    from common.config_loader import get_config_loader
    from common.utils import (
        build_task_dependencies,
        validate_dag_config,
        validate_task_config,
    )

    # 🔹 Carregar configs (agora dentro da função)
    config_loader = get_config_loader()
    env_config = config_loader.load_env_config()
    dag_config = config_loader.load_dag_config("gold")

    # 🔹 Validação
    validate_dag_config(dag_config)

    # 🔹 Default args (simplificado)
    default_args = {
        "owner": env_config["airflow"]["owner"],
        "retries": env_config["airflow"]["default_retries"],
        "retry_delay": timedelta(
            minutes=env_config["airflow"]["default_retry_delay_minutes"]
        ),
    }

    # 🔹 Definição da DAG (determinística)
    dag = DAG(
        dag_id=dag_config["dag_id"],
        template_searchpath=["/opt/airflow/configs"],
        description=dag_config.get("description", "Gold Layer DAG"),
        schedule=dag_config.get("schedule_interval"),
        start_date=datetime(2024, 1, 1),  # ✅ fixo
        catchup=dag_config.get("catchup", False),
        tags=dag_config.get("tags", ["medallion", "gold"]),
        default_args=default_args,
    )

    tasks = {}

    # 🔹 Criar tasks dinamicamente
    for task_config in dag_config["tasks"]:
        validate_task_config(task_config, "gold")

        task_id = task_config["task_id"]
        timeout_minutes = task_config.get("timeout_minutes", 30)
        pool = task_config.get("pool", "default_pool")
        sql_filename = task_config["sql_file"]

        # ❌ NÃO carregar SQL manualmente
        # ✅ usar template do Airflow
        task = BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                "query": {
                    "query": f"{{% include 'gold/queries/{sql_filename}' %}}",
                    "useLegacySql": False,
                }
            },
            location=env_config["airflow"].get("bigquery_location", "US"),
            gcp_conn_id=env_config["airflow"]["gcp_conn_id"],
            project_id=env_config["airflow"]["gcp_project_id"],
            execution_timeout=timedelta(minutes=timeout_minutes),
            pool=pool,
            dag=dag,
        )

        tasks[task_id] = task

    # 🔹 Dependências
    build_task_dependencies(tasks, dag_config["tasks"])

    return dag


# 🔥 Registro da DAG (padrão factory)
dag = create_dag()