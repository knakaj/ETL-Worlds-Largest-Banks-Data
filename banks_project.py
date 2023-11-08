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
csv_path = './largest_banks_data.csv' 


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
 
    # Find the heading "By market capitalization"
    #heading = data.find('span', {'id': 'By_market_capitalization'})
    # Find the table following the heading
    #table = heading.find_next('table')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    df = pd.DataFrame(columns=table_attribs)

    for row in rows: 
        columns = row.find_all('td')
        row_data = [column.text.strip() for column in columns]
        # Remove the last character ('\n') from the Market Cap column and typecast to float
        row_data[2] = float(row_data[2][:-1].replace(',', ''))
        table_data.append(row_data)
    
    # Create a Pandas DataFrame
    df = pd.DataFrame(table_data, columns=table_attribs)
    
    return df



def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path.'''


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name.'''


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. '''

"""

log_progress('Preliminaries complete. Initiating ETL process')

#call extract
log_progress('Data extraction complete. Initiating Transformation process')
#call transform
log_progress('Data transformation complete. Initiating Loading process')
#call load to csv
log_progress('Data saved to CSV file')
#initiate sqlite3 ocnnection
log_progress('SQL Connection initiated')
#call load to db
log_progress('Data loaded to Database as a table, Executing queries')
#call run query 
log_progress('Process Complete')
#close the connection
log_progress('Server Connection closed')

    
"""

# Function call to extract the table
df = extract(url,table_attribs)
# Print the resulting DataFrame
print(df)
