-- initdb/schema.sql
CREATE TABLE IF NOT EXISTS arxiv_papers (
    id TEXT PRIMARY KEY,
    title TEXT,
    summary TEXT,
    published TIMESTAMP,
    category TEXT,
    pdf_url TEXT,
    status TEXT DEFAULT 'pending',  -- 'pending' | 'downloading' | 'done' | 'error'
    error_message TEXT              -- если ошибка при скачивании
);
