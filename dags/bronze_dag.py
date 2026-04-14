"""
DAG de bronze - Agregações e métricas
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
    dag_config = config_loader.load_dag_config("bronze")

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
        description=dag_config.get("description", "bronze Layer DAG"),
        schedule=dag_config.get("schedule_interval"),
        start_date=datetime(2024, 1, 1),  # ✅ fixo
        catchup=dag_config.get("catchup", False),
        tags=dag_config.get("tags", ["medallion", "bronze"]),
        default_args=default_args,
    )

    tasks = {}

    # 🔹 Criar tasks dinamicamente
    for task_config in dag_config["tasks"]:
        validate_task_config(task_config, "bronze")

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
                    "query": f"{{% include 'bronze/queries/{sql_filename}' %}}",
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