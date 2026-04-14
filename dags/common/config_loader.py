import os
import yaml
import importlib
from typing import Dict, Any, Type
from pathlib import Path


class ConfigLoader:
    """Carregador de configurações YAML para Airflow medallion pipeline."""

    def __init__(self, config_dir: str = None):
        """
        Inicializa o loader com o diretório de configurações.

        Args:
            config_dir: Caminho para o diretório de configs.
                       Se None, usa ../configs relativo a este arquivo.
        """
        if config_dir is None:
            # Caminho relativo: dags/common/config_loader.py -> ../configs
            self.config_dir = Path(__file__).parent.parent.parent / "configs"
        else:
            self.config_dir = Path(config_dir)

    def load_env_config(self) -> Dict[str, Any]:
        """Carrega configurações globais de ambiente (env.yaml)."""
        env_file = self.config_dir / "env.yaml"
        if not env_file.exists():
            raise FileNotFoundError(f"Arquivo de ambiente não encontrado: {env_file}")

        with open(env_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}

    def load_dag_config(self, layer: str) -> Dict[str, Any]:
        """
        Carrega configuração de uma camada (bronze, silver, gold).

        Args:
            layer: Nome da camada ('bronze', 'silver' ou 'gold')

        Returns:
            Dicionário com configurações da DAG
        """
        dag_file = self.config_dir / layer / "dag_config.yaml"
        if not dag_file.exists():
            raise FileNotFoundError(f"Configuração de DAG não encontrada: {dag_file}")

        with open(dag_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}

    def load_extractor_config(self, layer: str, config_filename: str) -> Dict[str, Any]:
        """
        Carrega configuração de um extrator específico.

        Args:
            layer: Nome da camada ('bronze')
            config_filename: Nome do arquivo de configuração (ex: connector_1_config.yaml)

        Returns:
            Dicionário com configurações do extrator
        """
        config_file = self.config_dir / layer / "extractors" / config_filename
        if not config_file.exists():
            raise FileNotFoundError(f"Configuração de extrator não encontrada: {config_file}")

        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}

    def load_sql_file(self, layer: str, sql_filename: str) -> str:
        """
        Carrega um arquivo SQL de uma camada.

        Args:
            layer: Nome da camada ('silver', 'gold')
            sql_filename: Nome do arquivo SQL (ex: customers.sql)

        Returns:
            Conteúdo do arquivo SQL como string
        """
        sql_file = self.config_dir / layer / "queries" / sql_filename
        if not sql_file.exists():
            raise FileNotFoundError(f"Arquivo SQL não encontrado: {sql_file}")

        with open(sql_file, 'r', encoding='utf-8') as f:
            return f.read()

    def get_dataset_name(self, layer: str) -> str:
        """
        Obtém o nome do dataset BigQuery para uma camada com prefixo de ambiente.

        Args:
            layer: Nome da camada ('bronze', 'silver', 'gold')

        Returns:
            Nome completo do dataset (ex: 'dev_silver')
        """
        env_config = self.load_env_config()
        prefix = env_config.get('dataset_prefix', 'dev')
        return f"{prefix}_{layer}"

    @staticmethod
    def load_extractor_class(module_path: str) -> Type:
        """
        Carrega dinamicamente uma classe de extrator.

        Args:
            module_path: Caminho do módulo (ex: 'extractors.connectors.connector_1')
                        O último componente será o nome da classe

        Returns:
            Classe do extrator
        """
        parts = module_path.split('.')
        class_name = ''.join(word.capitalize() for word in parts[-1].split('_'))
        module_name = '.'.join(parts[:-1])

        try:
            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            raise ImportError(f"Não foi possível carregar {class_name} de {module_name}") from e


# Singleton global para facilitar uso
_loader = None

def get_config_loader(config_dir: str = None) -> ConfigLoader:
    """Retorna instância do ConfigLoader (singleton)."""
    global _loader
    if _loader is None:
        _loader = ConfigLoader(config_dir)
    return _loader
