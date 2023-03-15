# Use this script to write the python needed to complete this task
import pandas as pd
import requests
import sqlite3
from sqlalchemy import create_engine


#                                       Functions
#################################################################################################

def load_cities(file_path, sep_, header_, date_format, city_):
    df_ = pd.read_csv(file_path, sep=sep_, header=header_)
    del df_[df_.columns[0]]
    df_.columns = ['timestamp', 'drink', 'amount']
    df_.timestamp = pd.to_datetime(df_.timestamp, format=date_format)
    df_.drink = df_.drink.astype(str).str.lower()
    df_.amount = df_.amount.astype(float)
    df_['city'] = city_
    return df_


def get_data(content_):
    drink_ = content_.get('drinks')[0].get('strDrink').lower()
    glass_ = content_.get('drinks')[0].get('strGlass').lower()
    category_ = content_.get('drinks')[0].get('strCategory').lower()
    return {'drink': drink_,
            'glass': glass_,
            'category': category_}


#                                       Load Data
#################################################################################################

stock = pd.read_csv('data/bar_data.csv')
# highball glass stock is 34 glasses, we want just numbers for the stock
stock.stock = stock.stock.str.extract('(\d+)')
stock.columns = ['glass', 'stock', 'city']
stock.glass = stock.glass.str.lower()
stock.city = stock.city.str.lower().replace(" ", "_", regex=True)

budapest = load_cities('data/budapest.csv.gz', sep_=',', header_='infer', date_format="%Y-%m-%d %H:%M:%S",
                       city_="budapest")
new_york = load_cities('data/ny.csv.gz', sep_=',', header_='infer', date_format="%m-%d-%Y %H:%M", city_="new_york")
london = load_cities('data/london_transactions.csv.gz', sep_='\t', header_=None, date_format="%Y-%m-%d %H:%M:%S",
                     city_="london")
# combine them all together
transactions = pd.concat([budapest, new_york, london]).sort_values(by=['drink', 'city', 'timestamp'])
# get unique list of drinks being served
unique_drinks = transactions.drink.str.lower().replace(" ", "_", regex=True).unique()

#                                Get drink glass mapping from the API
#################################################################################################

glass_drink_match = []
base_url = "https://www.thecocktaildb.com/api/json/v1/1/search.php?s="
for drink in unique_drinks:
    response = requests.get(f'{base_url}{drink}')
    content = response.json()
    glass_drink_match.append(get_data(content))

# Make it into a dataframe
glass_drink_df = pd.DataFrame(glass_drink_match)

#                                 Create Tables & Load in data
#################################################################################################

# Use sqlite
conn = sqlite3.connect('bars_database.db')
c = conn.cursor()
# Create the database scheme from the script
with open('data_tables.SQL', 'r') as sql_file:
    conn.executescript(sql_file.read())

# Insert dataframes directly to the tables
engine = create_engine('sqlite:///bars_database.db', echo=False)
stock.to_sql('stock', con=engine, if_exists='append', index=False)
transactions.to_sql('transactions', con=engine, if_exists='append', index=False)
glass_drink_df.to_sql('drinks', con=engine, if_exists='append', index=False)

# Create POC table for easier querying on current stock levels
stock_level = transactions.merge(glass_drink_df, on="drink", how='left'). \
    merge(stock, on=['glass', 'city'], how='left'). \
    sort_values(by=['city', 'glass', 'timestamp'])
stock_level['glass_used'] = stock_level.groupby(['city', 'glass']).cumcount()
stock_level['current_stock'] = stock_level['stock'].astype(int) - stock_level['glass_used']

with open('poc_tables.SQL', 'r') as sql_file:
    conn.executescript(sql_file.read())
stock_level.to_sql('stock_level', con=engine, if_exists='append', index=False)

# Example way to query and see the data
print([a for a in c.execute("SELECT * FROM stock LIMIT 10;")])
print([a for a in c.execute("SELECT * FROM transactions LIMIT 10;")])
print([a for a in c.execute("SELECT * FROM drinks LIMIT 10;")])
print([a for a in c.execute("SELECT * FROM stock_level LIMIT 10;")])

conn.close()
