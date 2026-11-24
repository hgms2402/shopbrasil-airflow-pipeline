-- Schema dedicado para os dados analíticos da ShopBrasil
CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. Tabela Snapshot Idempotente (Requisito Obrigatório)
-- A chave primária composta (data_referencia, categoria) permite UPSERT sem duplicidades
CREATE TABLE IF NOT EXISTS analytics.metricas_categorias_snapshot (
    data_referencia DATE NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    preco_medio NUMERIC(10, 2) NOT NULL,
    preco_minimo NUMERIC(10, 2) NOT NULL,
    preco_maximo NUMERIC(10, 2) NOT NULL,
    quantidade_produtos INTEGER NOT NULL,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_metricas_categorias_snapshot PRIMARY KEY (data_referencia, categoria)
);

-- 2. Tabela de Histórico em Modo Append (Requisito Opcional)
-- Registra cada execução para acompanhamento da evolução dos preços
CREATE TABLE IF NOT EXISTS analytics.metricas_categorias_historico (
    id SERIAL PRIMARY KEY,
    data_execucao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_referencia DATE NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    preco_medio NUMERIC(10, 2) NOT NULL,
    preco_minimo NUMERIC(10, 2) NOT NULL,
    preco_maximo NUMERIC(10, 2) NOT NULL,
    quantidade_produtos INTEGER NOT NULL
);

-- Índice para acelerar consultas temporais por categoria no histórico
CREATE INDEX IF NOT EXISTS idx_metricas_historico_ref_cat
    ON analytics.metricas_categorias_historico (data_referencia, categoria);
