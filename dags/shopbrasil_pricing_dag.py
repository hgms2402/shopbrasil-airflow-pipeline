from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import timedelta
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from include.callbacks import (
    callback_falha,
    callback_retry,
    callback_sla_miss,
    callback_sucesso,
)
from operators.validar_produtos_operator import ValidarProdutosOperator

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://fakestoreapi.com/products"

UPSERT_SNAPSHOT_SQL = """
INSERT INTO analytics.metricas_categorias_snapshot (
    data_referencia,
    categoria,
    preco_medio,
    preco_minimo,
    preco_maximo,
    quantidade_produtos,
    atualizado_em
)
VALUES (%s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (data_referencia, categoria)
DO UPDATE SET
    preco_medio = EXCLUDED.preco_medio,
    preco_minimo = EXCLUDED.preco_minimo,
    preco_maximo = EXCLUDED.preco_maximo,
    quantidade_produtos = EXCLUDED.quantidade_produtos,
    atualizado_em = NOW();
"""

INSERT_HISTORICO_SQL = """
INSERT INTO analytics.metricas_categorias_historico (
    data_execucao,
    data_referencia,
    categoria,
    preco_medio,
    preco_minimo,
    preco_maximo,
    quantidade_produtos
)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""


def calcular_estatisticas_categoria(dados_categoria: dict[str, Any]) -> dict[str, Any]:
    """Função pura de cálculo de métricas (facilita testes unitários)."""
    categoria = dados_categoria["categoria"]
    precos = [float(p) for p in dados_categoria["precos"]]
    quantidade = len(precos)

    return {
        "categoria": categoria,
        "preco_medio": round(sum(precos) / quantidade, 2),
        "preco_minimo": round(min(precos), 2),
        "preco_maximo": round(max(precos), 2),
        "quantidade_produtos": quantidade,
    }


@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Pipeline diário de pricing e catálogo por categoria da ShopBrasil",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=callback_sla_miss,
    default_args={
        "owner": "shopbrasil_dados",
        "sla": timedelta(minutes=30),
    },
    tags=["shopbrasil", "pricing", "ecommerce"],
)
def shopbrasil_pricing_pipeline() -> None:
    with TaskGroup(group_id="ingestao") as tg_ingestao:

        @task(
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=10),
            on_failure_callback=callback_falha,
            on_retry_callback=callback_retry,
            on_success_callback=callback_sucesso,
        )
        def buscar_produtos() -> list[dict[str, Any]]:
            url = os.getenv("FAKESTORE_API_URL", DEFAULT_API_URL)
            logger.info("Consultando catálogo de produtos em: %s", url)

            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                brutos = response.json()
            except requests.RequestException as exc:
                logger.error("Erro ao consultar API (%s): %s", url, exc)
                raise

            # Projeção leve para evitar payloads pesados em XCom
            projetados = [
                {
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "category": item.get("category"),
                    "price": item.get("price"),
                }
                for item in brutos
                if isinstance(item, dict)
            ]
            logger.info("Coletados %d produtos (payload projetado).", len(projetados))
            return projetados

        @task
        def agrupar_por_categoria(
            produtos_validados: list[dict[str, Any]],
        ) -> list[dict[str, Any]]:
            grupos: dict[str, list[float]] = defaultdict(list)
            for produto in produtos_validados:
                grupos[produto["category"]].append(float(produto["price"]))

            resultado = [
                {"categoria": cat, "precos": precos}
                for cat, precos in sorted(grupos.items())
            ]
            logger.info("Categorias identificadas dinamicamente: %d", len(resultado))
            return resultado

        produtos_brutos = buscar_produtos()
        validar_produtos = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=produtos_brutos,
        )
        categorias_agrupadas = agrupar_por_categoria(validar_produtos.output)

    with TaskGroup(group_id="analise") as tg_analise:

        @task(pool="ecommerce_pool", sla=None)
        def calcular_metricas_categoria(
            dados_categoria: dict[str, Any],
        ) -> dict[str, Any]:
            metrica = calcular_estatisticas_categoria(dados_categoria)
            logger.info("Métricas calculadas: %s", metrica)
            return metrica

        @task
        def consolidar_metricas(
            metricas_mapeadas: list[dict[str, Any]],
        ) -> list[dict[str, Any]]:
            consolidadas = sorted(
                list(metricas_mapeadas),
                key=lambda item: item["categoria"],
            )
            logger.info("=== RESUMO EXECUTIVO SHOPBRASIL (%d categorias) ===", len(consolidadas))
            for m in consolidadas:
                logger.info(
                    "Categoria=%s | Qtd=%d | Medio=%.2f | Min=%.2f | Max=%.2f",
                    m["categoria"],
                    m["quantidade_produtos"],
                    m["preco_medio"],
                    m["preco_minimo"],
                    m["preco_maximo"],
                )
            return consolidadas

        metricas_mapeadas = calcular_metricas_categoria.expand(
            dados_categoria=categorias_agrupadas
        )
        metricas_consolidadas = consolidar_metricas(metricas_mapeadas)

    with TaskGroup(group_id="persistencia") as tg_persistencia:

        @task
        def salvar_metricas_postgres(
            metricas: list[dict[str, Any]],
        ) -> dict[str, Any]:
            ctx = get_current_context()
            data_referencia = ctx["ds"]
            data_execucao = pendulum.now("America/Sao_Paulo")

            hook = PostgresHook(postgres_conn_id="postgres_shopbrasil")
            conn = hook.get_conn()
            try:
                with conn.cursor() as cursor:
                    for item in metricas:
                        cursor.execute(
                            UPSERT_SNAPSHOT_SQL,
                            (
                                data_referencia,
                                item["categoria"],
                                item["preco_medio"],
                                item["preco_minimo"],
                                item["preco_maximo"],
                                item["quantidade_produtos"],
                            ),
                        )
                        cursor.execute(
                            INSERT_HISTORICO_SQL,
                            (
                                data_execucao,
                                data_referencia,
                                item["categoria"],
                                item["preco_medio"],
                                item["preco_minimo"],
                                item["preco_maximo"],
                                item["quantidade_produtos"],
                            ),
                        )
                conn.commit()
                logger.info(
                    "Persistência concluída: %d categorias gravadas (snapshot + histórico) para %s.",
                    len(metricas),
                    data_referencia,
                )
                return {
                    "data_referencia": data_referencia,
                    "categorias_persistidas": len(metricas),
                }
            except Exception:
                conn.rollback()
                logger.exception("Erro ao persistir métricas no PostgreSQL; rollback executado.")
                raise
            finally:
                conn.close()

        salvar_metricas_postgres(metricas_consolidadas)

    tg_ingestao >> tg_analise >> tg_persistencia


shopbrasil_pricing_pipeline()
