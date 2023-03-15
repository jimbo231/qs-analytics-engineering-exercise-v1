-- This is where you will write the SQL to create the tables needed by the bar staff to assist on restocking decisions
CREATE TABLE IF NOT EXISTS current_glass_stock (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP,
    drink VARCHAR(256),
    amount FLOAT,
    city VARCHAR(64),
    glass VARCHAR(256),
    category VARCHAR(256),
    stock FLOAT,
    glass_used FLOAT,
    current_stock FLOAT
);