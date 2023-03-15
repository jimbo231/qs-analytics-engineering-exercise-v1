
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP,
    drink VARCHAR(256),
    amount FLOAT,
    city VARCHAR(64),
    FOREIGN KEY(drink) REFERENCES drinks(drink)
);

CREATE TABLE IF NOT EXISTS stock (
    stock_id INTEGER PRIMARY KEY AUTOINCREMENT,
    glass VARCHAR(256),
    stock FLOAT,
    city VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS drinks (
    drink_id INTEGER PRIMARY KEY AUTOINCREMENT,
    drink VARCHAR(256),
    glass VARCHAR(256),
    category VARCHAR(256)
);
