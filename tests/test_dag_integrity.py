from airflow.models import DagBag


def test_dag_carrega_sem_erros_de_importacao():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert dagbag.import_errors == {}, f"Erros de importação: {dagbag.import_errors}"
    assert "shopbrasil_pricing_pipeline" in dagbag.dags


def test_configuracao_agendamento_e_timezone():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    dag = dagbag.get_dag("shopbrasil_pricing_pipeline")

    assert dag.schedule_interval == "0 6 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert str(dag.timezone.name) == "America/Sao_Paulo"


def test_taskgroups_e_pool_ecommerce():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    dag = dagbag.get_dag("shopbrasil_pricing_pipeline")

    task_ids = set(dag.task_ids)
    esperadas = {
        "ingestao.buscar_produtos",
        "ingestao.validar_produtos",
        "ingestao.agrupar_por_categoria",
        "analise.calcular_metricas_categoria",
        "analise.consolidar_metricas",
        "persistencia.salvar_metricas_postgres",
    }
    assert esperadas.issubset(task_ids)

    task_calculo = dag.get_task("analise.calcular_metricas_categoria")
    assert task_calculo.pool == "ecommerce_pool"

    task_ingestao = dag.get_task("ingestao.buscar_produtos")
    assert task_ingestao.retries == 3
    assert task_ingestao.retry_exponential_backoff is True
