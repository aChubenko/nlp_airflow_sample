CREATE TABLE IF NOT EXISTS arxiv_amount_of_pupications_by_year (
    year INTEGER PRIMARY KEY,
    complete_list_size INTEGER,
    resumption_token TEXT,
    cursor INTEGER,
    status TEXT DEFAULT 'pending',
    updated_at TIMESTAMP DEFAULT now()
);