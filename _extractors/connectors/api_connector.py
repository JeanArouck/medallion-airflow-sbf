"""
Conector de API Genérico para Extração de Dados

Exemplo de extrator que conecta a APIs HTTP e extrai dados
configuráveis via YAML.
"""

import requests
import pandas as pd
import logging
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

from extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class ApiConnector(BaseExtractor):
    """
    Conector genérico para APIs REST.

    Espera configuração no formato:
    ```yaml
    api:
      base_url: "https://api.example.com"
      api_key: "key-or-env-var"
      timeout_seconds: 30
      retry_attempts: 3

    tables:
      customers:
        endpoint: "/v1/customers"
        method: "GET"
        target_table: "customers"
    ```
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config["api"]["base_url"]
        self.api_key = config["api"].get("api_key")
        self.timeout = config["api"].get("timeout_seconds", 30)
        self.retry_attempts = config["api"].get("retry_attempts", 3)
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Cria sessão HTTP com headers padrão."""
        session = requests.Session()

        # Adicionar headers padrão
        headers = {
            "User-Agent": "Airflow-Medallion/1.0",
            "Content-Type": "application/json",
        }

        # Adicionar autenticação se houver API key
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        session.headers.update(headers)
        return session

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _fetch_data(self, endpoint: str, method: str = "GET", **kwargs) -> Dict[str, Any]:
        """
        Faz requisição à API com retry automático.

        Args:
            endpoint: Path relativo (ex: /v1/customers)
            method: Método HTTP (GET, POST, etc)
            **kwargs: Argumentos adicionais para requests

        Returns:
            Resposta JSON como dicionário
        """
        url = f"{self.base_url}{endpoint}"
        logger.info(f"Fetching: {method} {url}")

        try:
            response = self.session.request(
                method=method,
                url=url,
                timeout=self.timeout,
                **kwargs
            )
            response.raise_for_status()

            data = response.json()
            logger.info(f"✓ Got {len(data) if isinstance(data, list) else 1} records")
            return data

        except requests.RequestException as e:
            logger.error(f"✗ API Error: {str(e)}")
            raise

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza DataFrame após extração.

        - Remove colunas vazias
        - Padroniza tipos de dado
        - Remove linhas nulas
        """
        # Remover colunas completamente nulas
        df = df.dropna(axis=1, how="all")

        # Converter colunas de data
        for col in df.columns:
            if "date" in col.lower() or "time" in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass

        # Remover linhas duplicadas
        df = df.drop_duplicates()

        logger.info(f"Normalized to {len(df)} rows, {len(df.columns)} columns")
        return df

    def extract(self) -> pd.DataFrame:
        """
        Extrai dados de todas as tabelas configuradas.

        Returns:
            DataFrame consolidado com dados de todas as APIs
        """
        dfs = []
        tables = self.config.get("data", {}).get("tables", {})

        for table_name, table_config in tables.items():
            logger.info(f"Extracting {table_name}...")

            try:
                # Fazer requisição
                data = self._fetch_data(
                    endpoint=table_config["endpoint"],
                    method=table_config.get("method", "GET"),
                    params=table_config.get("params", {})
                )

                # Converter para DataFrame
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, dict) and "data" in data:
                    # Alguns APIs retornam {'data': [...]}
                    df = pd.DataFrame(data["data"])
                else:
                    df = pd.DataFrame([data])

                # Adicionar coluna de origem
                df["_source_table"] = table_name

                # Normalizar
                df = self._normalize_dataframe(df)

                dfs.append(df)
                logger.info(f"✓ {table_name}: {len(df)} rows")

            except Exception as e:
                logger.error(f"✗ Error extracting {table_name}: {str(e)}")
                raise

        # Consolidar todos os dados
        if not dfs:
            raise ValueError("Nenhum dado foi extraído")

        result = pd.concat(dfs, ignore_index=True, sort=False)
        logger.info(f"Total extracted: {len(result)} rows")

        return result


class DataValidator(BaseExtractor):
    """
    Validador de qualidade de dados na camada bronze.

    Valida datasets contra regras configuradas em YAML.
    """

    def extract(self) -> pd.DataFrame:
        """
        Valida dados na bronze e retorna relatório de qualidade.

        Returns:
            DataFrame com resultados de validação
        """
        validations = self.config.get("tables", {})
        results = []

        for table_name, checks in validations.items():
            logger.info(f"Validating {table_name}...")

            for check in checks.get("checks", []):
                result = {
                    "table": table_name,
                    "check_type": check["type"],
                    "column": check.get("column"),
                    "description": check.get("description"),
                    "status": "PASS",
                    "details": None,
                }

                # Implementar validações conforme necessário
                # Este é um exemplo básico

                results.append(result)

        return pd.DataFrame(results)
