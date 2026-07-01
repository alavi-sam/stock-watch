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
    fetched BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS full_article(
    
)