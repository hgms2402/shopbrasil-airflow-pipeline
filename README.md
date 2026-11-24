# ShopBrasil — Pipeline de Pricing e Catálogo (Apache Airflow)

Pipeline diário, resiliente e idempotente para coleta, validação, cálculo de métricas de preço por categoria e persistência analítica da **ShopBrasil**, substituindo o script legado em `cron`.

---

## 1. Arquitetura e Topologias

O pipeline é construído com a **TaskFlow API** (`@dag` / `@task`), organizado em **3 `TaskGroup`s** e evidencia explicitamente as topologias **Linear**, **Fan-Out** (*Dynamic Task Mapping*) e **Fan-In**:

```text
[TaskGroup: ingestao]  (Topologia Linear)
  buscar_produtos  ──►  validar_produtos  ──►  agrupar_por_categoria
  (retry + backoff      (ValidarProdutos        (extrai categorias
   + 3 callbacks)        Operator)               dinamicamente)
                                                       │
                                                       ▼  (Fan-Out: .expand())
[TaskGroup: analise]
                    ┌──► calcular_metricas_categoria[0] (pool: ecommerce_pool, 2 slots) ──┐
                    ├──► calcular_metricas_categoria[1] (pool: ecommerce_pool, 2 slots) ──┤
                    └──► calcular_metricas_categoria[N] (pool: ecommerce_pool, 2 slots) ──┤
                                                                                          ▼  (Fan-In)
                                                                                 consolidar_metricas
                                                                                          │
                                                                                          ▼
[TaskGroup: persistencia]
                                                                              salvar_metricas_postgres
                                                                          (Snapshot Idempotente + Histórico)
```

### Principais Características Técnicas

| Recurso | Implementação |
| :--- | :--- |
| **Fonte de Dados** | FakeStore API (`https://fakestoreapi.com/products`) com projeção leve de XCom (`id`, `title`, `category`, `price`). |
| **Agendamento** | Diário às **06:00** em `America/Sao_Paulo` (`pendulum`), `catchup=False`, `max_active_runs=1`. |
| **Resiliência** | `retries=3`, `retry_exponential_backoff=True`, `try/except + raise`, `on_success_callback`, `on_retry_callback`, `on_failure_callback` e SLA de 30 min. |
| **Escalabilidade e Concorrência** | *Dynamic Task Mapping* (`.expand()`) por categoria + Pool `ecommerce_pool` limitado a **2 slots**. |
| **Qualidade de Dados** | Operador customizado `ValidarProdutosOperator(BaseOperator)` validando schema e regras de negócio. |
| **Persistência** | `PostgresHook` (`postgres_shopbrasil`) com `UPSERT` idempotente em `analytics.metricas_categorias_snapshot` e carga `append` em `analytics.metricas_categorias_historico`. |

---

## 2. Estrutura do Repositório

```text
shopbrasil-airflow-pipeline/
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
├── dags/
│   └── shopbrasil_pricing_dag.py
├── plugins/
│   └── operators/
│       └── validar_produtos_operator.py
├── include/
│   ├── callbacks.py
│   ├── mock/
│   │   └── fakestore_server.py
│   └── sql/
│       └── init_analytics.sql
├── tests/
│   ├── test_dag_integrity.py
│   └── test_validar_produtos.py
└── README.md
```

---

## 3. Pré-requisitos

* **Docker** e **Docker Compose** (v2+)
* **Git**

---

## 4. Como Executar o Projeto

### 1. Clonar e configurar variáveis de ambiente

```bash
git clone https://github.com/hgms2402/shopbrasil-airflow-pipeline.git
cd shopbrasil-airflow-pipeline
cp .env.example .env
mkdir -p logs && chmod -R 777 logs
```

### 2. Subir a infraestrutura completa

```bash
docker compose up -d --build
```

O serviço `airflow-init` executa automaticamente:
* Migração do banco de metadados (`airflow db migrate`);
* Criação do usuário administrador (`admin` / `admin`);
* Registro da Airflow Connection `postgres_shopbrasil`;
* Criação do Pool `ecommerce_pool` com **2 slots**.

### 3. Acessar a Interface Web do Airflow

* **URL:** `http://localhost:8080`
* **Usuário / Senha:** `admin` / `admin`
* Ative a DAG **`shopbrasil_pricing_pipeline`** e dispare uma execução manual (ou aguarde o agendamento das 06:00 BRT).

> **Nota para ambientes em nuvem (AWS/GCP/Azure):** Caso o Cloudflare bloqueie o IP do datacenter contra `fakestoreapi.com` (HTTP 403), há um espelho local opcional em `include/mock/fakestore_server.py` que reproduz exatamente o contrato da API (e expõe `/error` para simular HTTP 500).

---

## 5. Testes Automatizados (`pytest`)

Execute a suíte de testes de integridade da DAG, operador customizado e cálculos:

```bash
docker compose exec airflow-scheduler pytest -v /opt/airflow/tests
```

---

## 6. Validação no PostgreSQL Analítico

Consulte as métricas gravadas nas tabelas de snapshot (idempotente) e histórico (*append*):

```bash
# Snapshot atual por categoria e data de referência (zero duplicidade em reprocessamentos)
docker compose exec postgres-analytics psql -U analytics -d shopbrasil -c "
SELECT data_referencia, categoria, preco_medio, preco_minimo, preco_maximo, quantidade_produtos
FROM analytics.metricas_categorias_snapshot
ORDER BY data_referencia DESC, categoria;
"

# Histórico de execuções (modo append)
docker compose exec postgres-analytics psql -U analytics -d shopbrasil -c "
SELECT id, data_execucao, data_referencia, categoria, preco_medio, quantidade_produtos
FROM analytics.metricas_categorias_historico
ORDER BY id DESC;
"
```
