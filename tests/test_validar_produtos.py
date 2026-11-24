import pytest
from airflow.exceptions import AirflowException

from dags.shopbrasil_pricing_dag import calcular_estatisticas_categoria
from operators.validar_produtos_operator import ValidarProdutosOperator


def test_validar_produtos_sucesso():
    payload = [
        {"id": 1, "title": "Notebook", "price": 3500.0, "category": "electronics"},
        {"id": 2, "title": "Mouse", "price": 150.5, "category": "electronics"},
    ]
    op = ValidarProdutosOperator(task_id="test_ok", produtos=payload)
    assert op.execute({}) == payload


@pytest.mark.parametrize(
    "payload_invalido",
    [
        [],
        [{"id": 1, "title": "Sem preço", "category": "electronics"}],
        [{"id": 1, "title": "Preço negativo", "price": -5.0, "category": "electronics"}],
        [{"id": 1, "title": "Preço zero", "price": 0, "category": "electronics"}],
        [{"id": 1, "title": "Categoria vazia", "price": 10.0, "category": "   "}],
    ],
)
def test_validar_produtos_falha_em_dados_invalidos(payload_invalido):
    op = ValidarProdutosOperator(task_id="test_fail", produtos=payload_invalido)
    with pytest.raises(AirflowException):
        op.execute({})


def test_calcular_estatisticas_categoria():
    dados = {"categoria": "electronics", "precos": [100.0, 200.0, 300.0]}
    resultado = calcular_estatisticas_categoria(dados)

    assert resultado == {
        "categoria": "electronics",
        "preco_medio": 200.0,
        "preco_minimo": 100.0,
        "preco_maximo": 300.0,
        "quantidade_produtos": 3,
    }
