from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("shopbrasil.callbacks")


def callback_sucesso(context: dict[str, Any]) -> None:
    """Registra log estruturado ao concluir a task com sucesso."""
    ti = context.get("task_instance")
    dag_id = context["dag"].dag_id if context.get("dag") else "desconhecida"
    task_id = ti.task_id if ti else "desconhecida"
    logical_date = str(context.get("logical_date"))
    duration = getattr(ti, "duration", None)

    logger.info(
        "[SUCESSO] DAG=%s | Task=%s | DataLogica=%s | Duracao=%ss",
        dag_id,
        task_id,
        logical_date,
        duration,
    )


def callback_retry(context: dict[str, Any]) -> None:
    """Registra tentativa de retry com backoff exponencial."""
    ti = context.get("task_instance")
    dag_id = context["dag"].dag_id if context.get("dag") else "desconhecida"
    task_id = ti.task_id if ti else "desconhecida"
    try_number = getattr(ti, "try_number", "?")
    exception = context.get("exception")

    logger.warning(
        "[RETRY] DAG=%s | Task=%s | Tentativa=%s | Backoff exponencial ativo | Erro=%s",
        dag_id,
        task_id,
        try_number,
        exception,
    )


def callback_falha(context: dict[str, Any]) -> None:
    """Simula o envio de um alerta crítico (Slack/Webhook) em caso de falha definitiva."""
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")

    alerta = {
        "canal": "#alertas-dados-shopbrasil",
        "severidade": "CRITICAL",
        "evento": "TASK_FAILURE",
        "dag_id": context["dag"].dag_id if context.get("dag") else "desconhecida",
        "task_id": ti.task_id if ti else "desconhecida",
        "run_id": dag_run.run_id if dag_run else "desconhecido",
        "logical_date": str(context.get("logical_date")),
        "erro": str(context.get("exception")),
        "log_url": getattr(ti, "log_url", ""),
    }

    logger.error(
        "[ALERTA SIMULADO - FALHA CRITICA]\n%s",
        json.dumps(alerta, ensure_ascii=False, indent=2),
    )


def callback_sla_miss(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: list[Any],
    blocking_tis: list[Any],
) -> None:
    """Simula o envio de alerta quando o SLA do pipeline é violado."""
    alerta = {
        "canal": "#alertas-dados-shopbrasil",
        "severidade": "WARNING",
        "evento": "SLA_MISS",
        "dag_id": dag.dag_id if dag else "desconhecida",
        "tasks_atrasadas": task_list,
        "tasks_bloqueadoras": blocking_task_list,
    }
    logger.warning(
        "[ALERTA SIMULADO - SLA VIOLADO]\n%s",
        json.dumps(alerta, ensure_ascii=False, indent=2),
    )
