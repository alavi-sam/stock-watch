CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS articles (
    id BIGINT PRIMARY KEY,
    ticker TEXT NOT NULL,
    category TEXT,
    datetime TIMESTAMPTZ,
    headline TEXT,
    image TEXT,
    source TEXT,
    summary TEXT,
    url TEXT,
    fetched BOOLEAN NOT NULL DEFAULT FALSE,
    num_try INT
);

CREATE TABLE IF NOT EXISTS article_embedding (
    id BIGINT PRIMARY KEY REFERENCES articles(id),
    embedding vector(2048),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
