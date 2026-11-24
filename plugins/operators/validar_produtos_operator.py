from __future__ import annotations

import logging
from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

logger = logging.getLogger(__name__)


class ValidarProdutosOperator(BaseOperator):
    """Operador customizado que valida o schema e as regras de domínio dos produtos."""

    template_fields: Sequence[str] = ("produtos",)
    CAMPOS_OBRIGATORIOS = ("id", "title", "price", "category")

    def __init__(self, *, produtos: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context: Context) -> list[dict[str, Any]]:
        if not isinstance(self.produtos, list) or len(self.produtos) == 0:
            raise AirflowException(
                "Payload de produtos inválido: esperado uma lista não vazia."
            )

        erros: list[str] = []
        for idx, item in enumerate(self.produtos):
            if not isinstance(item, dict):
                erros.append(f"Índice {idx}: item não é um dicionário.")
                continue

            faltantes = [c for c in self.CAMPOS_OBRIGATORIOS if c not in item]
            if faltantes:
                erros.append(f"Índice {idx}: campos obrigatórios ausentes {faltantes}.")
                continue

            categoria = item.get("category")
            if not isinstance(categoria, str) or not categoria.strip():
                erros.append(
                    f"Índice {idx} (id={item.get('id')}): categoria inválida ({categoria!r})."
                )

            preco = item.get("price")
            if isinstance(preco, bool) or not isinstance(preco, (int, float)) or preco <= 0:
                erros.append(
                    f"Índice {idx} (id={item.get('id')}): preço inválido ({preco!r})."
                )

        if erros:
            resumo = "; ".join(erros[:10])
            logger.error("Falha na validação de produtos (%d erros): %s", len(erros), resumo)
            raise AirflowException(
                f"Validação de produtos falhou com {len(erros)} erro(s): {resumo}"
            )

        logger.info("Validação concluída com sucesso para %d produtos.", len(self.produtos))
        return self.produtos
