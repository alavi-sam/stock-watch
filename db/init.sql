CREATE TABLE IF NOT EXISTS articles (
    id BIGINT PRIMARY KEY,
    ticker TEXT NOT NULL,
    category TEXT,
    datetime TIMESTAMPTZ,
    headline TEXT,
    image TEXT,
    source TEXT,
    summary TEXT,
    url TEXT
);

