CREATE TABLE IF NOT EXISTS arxiv_articles (
    identifier TEXT PRIMARY KEY,
    title TEXT,
    summary TEXT,
    published TIMESTAMP,
    authors TEXT[],
    updated_date TIMESTAMP,
    raw_category TEXT,
    pdf_url TEXT,
    status TEXT DEFAULT 'pending'
);
