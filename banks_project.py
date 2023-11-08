# 2023-11-07, KN, Code for Final Project

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3 
import numpy as np 
from datetime import datetime


# Initialize all known entities
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion'] 
db_name = 'Banks.db'
table_name = 'Largest_banks'
exchange_rate_csv_path = './exchange_rate.csv'
output_csv_path = './largest_banks_data.csv' 


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ':' + message + '\n') 


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    html_page = requests.get(url).text  # Extract the webpage as text
    data = BeautifulSoup(html_page, 'html.parser') # Parse the text into an HTML object
    df = pd.DataFrame(columns=table_attribs) # Create a pandas dataframe with table attributes as columns

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows: 
        columns = row.find_all('td')

        if len(columns) != 0:
            #name = str(columns[1].contents[0])  # Extract the second column
            #mc_usd_billion = columns[2].contents[0]  # Extract the third column
            name = columns[1].get_text().strip()
            mc_usd_billion = columns[2].get_text().strip()
            try:
                mc_usd_billion_float = float(mc_usd_billion)
                data_dict = {"Name": name, "MC_USD_Billion": mc_usd_billion_float}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
            except ValueError:
                # Handle the case where mc_usd_billion cannot be converted to a float
                print(f"Skipping row with invalid MC_USD_Billion: {mc_usd_billion}")
    return df



def transform(df, exchange_rate_csv_path ):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rate_df = pd.read_csv(exchange_rate_csv_path) 
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    # Add the 'MC_GBP_Billion' column
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]

    # Add the 'MC_EUR_Billion' column
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]

    # Add the 'MC_INR_Billion' column
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_csv_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path.'''

    df.to_csv(output_csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. '''

"""
#initiate sqlite3 ocnnection
log_progress('SQL Connection initiated')
#call load to db
log_progress('Data loaded to Database as a table, Executing queries')
#call run query 
log_progress('Process Complete')
#close the connection
log_progress('Server Connection closed')

    
"""


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url,table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, exchange_rate_csv_path)
log_progress('Data transformation complete. Initiating Loading process')
     
load_to_csv(df, output_csv_path)
log_progress('Data saved to CSV file')

conn =  sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')

load_to_db(df, conn, table_name)
log_progress('Data loaded to Database as table.')

