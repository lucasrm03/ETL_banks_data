# Code for ETL operations on Bank data

# Importing the required libraries
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = './Largest_banks_data.csv'
csv_currency = './exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    now = datetime.now()
    time_stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    with open (log_file,'a') as f:
        f.write(time_stamp+' : '+message+'\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    data_list = [] 
    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            data_dict = {'Name': col[1].text.strip(), 'MC_USD_Billion': col[2].contents[0].replace('\n', '')}
            data_list.append(data_dict)
    
    df =pd.DataFrame(data_list)
    df.columns = table_attribs

    return df

def transform(df, csv_currency):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    currency = pd.read_csv(csv_currency)
    currency.set_index('Currency', inplace=True)
    
    # Using single quotes for strings and putting 'Rate' inside quotes
    df['MC_GBP_Billion'] = (df['MC_USD_Billion'].astype(float) * currency.loc['GBP', 'Rate'].astype(float)).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'].astype(float) * currency.loc['EUR', 'Rate'].astype(float)).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'].astype(float) * currency.loc['INR', 'Rate'].astype(float)).round(2)

    return df

df = extract(url, table_attribs)
df_transformed = transform(df, csv_currency)


def load_to_csv(df_transformed, csv_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df_transformed.to_csv(csv_path,index = False)

def load_to_db(df_transformed, conn, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df_transformed.to_sql(table_name, conn, if_exists='replace', index=False)


    
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_currency)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f'SELECT * FROM Largest_banks'
run_query(query_statement, sql_connection)

query_statement = f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement, sql_connection)

query_statement = f'SELECT Name from Largest_banks LIMIT 5'
run_query(query_statement, sql_connection)

sql_connection.close()

log_progress('Process Complete.')

print('Process Complete.')
