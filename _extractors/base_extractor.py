from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import logging

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Classe base para todos os extratores de dados."""

    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa o extrator com configurações.

        Args:
            config: Dicionário com configurações específicas do extrator
        """
        self.config = config
        self.bq_client = None

    def get_bigquery_client(self, project_id: Optional[str] = None) -> bigquery.Client:
        """
        Obtém ou cria um cliente BigQuery.

        Args:
            project_id: ID do projeto GCP (opcional)

        Returns:
            google.cloud.bigquery.Client
        """
        if self.bq_client is None:
            self.bq_client = bigquery.Client(project=project_id)
        return self.bq_client

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Extrai dados da fonte.

        Returns:
            DataFrame com os dados extraídos
        """
        raise NotImplementedError("Subclasses devem implementar extract()")

    def load_to_bigquery(
        self,
        df: pd.DataFrame,
        table_id: str,
        dataset_id: str,
        project_id: str,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> None:
        """
        Carrega um DataFrame no BigQuery.

        Args:
            df: DataFrame com os dados
            table_id: ID da tabela (ex: customers)
            dataset_id: ID do dataset (ex: dev_bronze)
            project_id: ID do projeto GCP
            write_disposition: Como escrever (WRITE_TRUNCATE, WRITE_APPEND, etc)

        Raises:
            GoogleCloudError: Se houver erro ao carregar
        """
        client = self.get_bigquery_client(project_id)
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = write_disposition
        job_config.autodetect = True

        try:
            load_job = client.load_table_from_dataframe(
                df, full_table_id, job_config=job_config
            )
            load_job.result()  # Aguarda conclusão
            logger.info(f"✓ Dados carregados em {full_table_id}")
        except GoogleCloudError as e:
            logger.error(f"✗ Erro ao carregar dados em {full_table_id}: {str(e)}")
            raise

    def run(
        self,
        table_id: str,
        dataset_id: str,
        project_id: str,
    ) -> None:
        """
        Executa o fluxo completo: extrai e carrega dados.

        Args:
            table_id: ID da tabela destino
            dataset_id: Dataset destino
            project_id: Projeto GCP
        """
        logger.info(f"Iniciando extração para {project_id}.{dataset_id}.{table_id}")

        try:
            df = self.extract()
            logger.info(f"✓ {len(df)} linhas extraídas")

            self.load_to_bigquery(df, table_id, dataset_id, project_id)
            logger.info("✓ Extração e carga completadas com sucesso")

        except Exception as e:
            logger.error(f"✗ Erro durante extração/carga: {str(e)}")
            raise
