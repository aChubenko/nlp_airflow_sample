CREATE TABLE IF NOT EXISTS translated_articles_analytics (
    identifier TEXT PRIMARY KEY,
    research_field TEXT,
    word_count INTEGER,
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

