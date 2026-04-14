from datetime import timedelta
from typing import Dict, Any, Optional
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.config_loader import get_config_loader


class BigQuerySQLExecutor:
    """Executor para rodar SQLs no BigQuery baseado em configurações."""

    def __init__(self, project_id: str = None):
        """
        Inicializa o executor.

        Args:
            project_id: ID do projeto GCP. Se None, será extraído de credenciais.
        """
        self.project_id = project_id
        self.config_loader = get_config_loader()

    def create_bigquery_task(
        self,
        task_id: str,
        layer: str,
        sql_filename: str,
        task_config: Dict[str, Any],
        gcp_conn_id: str = "google_cloud_default",
    ) -> BigQueryInsertJobOperator:
        """
        Cria um BigQueryInsertJobOperator baseado na configuração.

        Args:
            task_id: ID da task no Airflow
            layer: Camada ('silver', 'gold')
            sql_filename: Nome do arquivo SQL
            task_config: Dicionário com configurações (timeout_minutes, pool, etc)
            gcp_conn_id: ID da conexão GCP no Airflow

        Returns:
            BigQueryInsertJobOperator configurado
        """
        # Carrega o SQL
        sql = self.config_loader.load_sql_file(layer, sql_filename)

        # Obtém configurações
        timeout_minutes = task_config.get('timeout_minutes', 30)
        pool = task_config.get('pool', 'default_pool')
        dataset = self.config_loader.get_dataset_name(layer)

        # Prepara a configuração do job
        job_config = {
            "query": {
                "query": sql,
                "useLegacySql": False,
            }
        }

        # Cria o operador
        return BigQueryInsertJobOperator(
            task_id=task_id,
            configuration=job_config,
            location="US",
            gcp_conn_id=gcp_conn_id,
            project_id=self.project_id,
            autodetect=True,
            use_legacy_sql=False,
            execution_timeout=timedelta(minutes=timeout_minutes),  # Converte para segundos
            pool=pool,
        )

    def read_sql_file(self, layer: str, sql_filename: str) -> str:
        """
        Lê um arquivo SQL.

        Args:
            layer: Camada ('silver', 'gold')
            sql_filename: Nome do arquivo SQL

        Returns:
            Conteúdo do SQL como string
        """
        return self.config_loader.load_sql_file(layer, sql_filename)


def create_bigquery_task(
    task_config: Dict[str, Any],
    layer: str,
    executor: Optional[BigQuerySQLExecutor] = None,
    gcp_conn_id: str = "google_cloud_default",
) -> BigQueryInsertJobOperator:
    """
    Função auxiliar para criar uma task BigQuery.

    Args:
        task_config: Dicionário com task_id, sql_file, etc
        layer: Camada ('silver', 'gold')
        executor: Instância do executor (criará uma nova se None)
        gcp_conn_id: ID da conexão GCP

    Returns:
        BigQueryInsertJobOperator
    """
    if executor is None:
        executor = BigQuerySQLExecutor()

    return executor.create_bigquery_task(
        task_id=task_config['task_id'],
        layer=layer,
        sql_filename=task_config['sql_file'],
        task_config=task_config,
        gcp_conn_id=gcp_conn_id,
    )
