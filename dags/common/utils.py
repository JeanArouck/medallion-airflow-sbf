from typing import Dict, List, Any, Optional
from airflow.models import BaseOperator
from datetime import timedelta


def build_task_dependencies(
    tasks: Dict[str, BaseOperator],
    task_configs: List[Dict[str, Any]],
) -> None:
    """
    Constrói as dependências entre tasks baseado no YAML.

    A propriedade 'downstream_task' no YAML define para qual task a atual deve apontar.

    Args:
        tasks: Dicionário de tasks Airflow {task_id: operator}
        task_configs: Lista de configurações de tasks do YAML
    """
    for config in task_configs:
        task_id = config['task_id']
        downstream = config.get('downstream_task')

        if downstream and task_id in tasks and downstream in tasks:
            tasks[task_id] >> tasks[downstream]


def get_timedelta_from_config(minutes: Optional[int] = None) -> timedelta:
    """
    Converte minutos para timedelta.

    Args:
        minutes: Número de minutos (None = 5 minutos padrão)

    Returns:
        timedelta object
    """
    minutes = minutes or 5
    return timedelta(minutes=minutes)


def sanitize_task_id(name: str) -> str:
    """
    Sanitiza um nome para ser válido como task_id no Airflow.

    Args:
        name: Nome original

    Returns:
        Nome sanitizado (lowercase, underscores, sem caracteres especiais)
    """
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name.lower())


def validate_dag_config(config: Dict[str, Any]) -> bool:
    """
    Valida se a configuração de DAG tem os campos obrigatórios.

    Args:
        config: Dicionário de configuração da DAG

    Returns:
        True se válida, levanta exceção caso contrário

    Raises:
        ValueError: Se campos obrigatórios estão faltando
    """
    required_fields = ['dag_id', 'schedule_interval', 'tasks']

    for field in required_fields:
        if field not in config:
            raise ValueError(f"Campo obrigatório '{field}' não encontrado na configuração")

    if not isinstance(config['tasks'], list) or len(config['tasks']) == 0:
        raise ValueError("Campo 'tasks' deve ser uma lista não-vazia")

    return True


def validate_task_config(config: Dict[str, Any], layer: str) -> bool:
    """
    Valida configuração de uma task específica.

    Args:
        config: Dicionário de configuração da task
        layer: Camada ('bronze', 'silver', 'gold')

    Returns:
        True se válida, levanta exceção caso contrário

    Raises:
        ValueError: Se campos obrigatórios estão faltando
    """
    required_fields = ['task_id', 'type']
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Campo obrigatório '{field}' não encontrado na task")

    task_type = config['type']

    # Validação específica por tipo
    if task_type == 'sql':
        if 'sql_file' not in config:
            raise ValueError("Task tipo 'sql' deve ter campo 'sql_file'")
    elif task_type == 'extractor':
        if 'extractor_module' not in config:
            raise ValueError("Task tipo 'extractor' deve ter campo 'extractor_module'")
        if 'config_file' not in config:
            raise ValueError("Task tipo 'extractor' deve ter campo 'config_file'")
    else:
        raise ValueError(f"Tipo de task inválido: {task_type}")

    return True
