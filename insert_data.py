# Get data from Rolling sales Database
import pandas as pd
import requests
import urllib3
import sqlite3
import os.path
import re
import logging
import numpy as np

from sqlite3 import Error
from os import path
from datetime import datetime

DB_FILE = "database/housing_data.db"

def create_connection(db_file: str):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
        if conn is not None:
            conn.close()
            
def execute_query(query: str, db_file: str):
    # Generic execute query function
    conn = create_connection(db_file)
    conn.execute(query)
    conn.commit()
    conn.close()
    
def insert_df(df, db_file: str, table_name: str):
    # Insert DataFrame into sqllite database
    conn = create_connection(db_file)
    conn.execute(f"create table if not exists {table_name}" + columns_to_sql(df))
    df.columns = [col.replace(' ', '_') for col in df.columns]
    df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
    conn.close()
    print('SQL insert process finished')
    
# Create a primary key of the house data
def convert_row_to_p_key(x):
    return "_".join([re.sub(' +', ' ', str(i).strip()) for i in x[sorted(x.index)].values])

def columns_to_sql(df):
    primary_key = df['p_key']
    insert_query_values = '('
    
    for col in df.columns:
        date_type = df[col].dtype
        insert_query_values += col.replace(' ', '_')
        if (np.issubdtype(date_type, np.integer)) or (np.issubdtype(df['NEIGHBORHOOD'].dtype, np.bool_)):
            insert_query_values += " INTEGER,\n"
        elif np.issubdtype(date_type, np.float64):
            insert_query_values += " REAL,\n"
        else: # Else object or datetime
            insert_query_values += " TEXT,\n"
    insert_query_values += 'PRIMARY KEY (p_key) );'
    return insert_query_values

if __name__ == '__main__':
    if not path.exists(DB_FILE):
        create_connection(DB_FILE)
        print("Database created")
    else:
        print("Database already created")

    curr_year = datetime.now().year
    site = "https://www1.nyc.gov"
    nyc_sale_data_past = "https://www1.nyc.gov/site/finance/taxes/property-annualized-sales-update.page"
    nyc_sale_data_curr = "https://www1.nyc.gov/site/finance/taxes/property-rolling-sales-data.page"
    resp = requests.get(nyc_sale_data_past)
    resp_curr = requests.get(nyc_sale_data_curr)
    if not resp:
        raise ValueError("Error connecting: 404 Error")

    for date in range(2007, curr_year - 1): # Get data from 2017 to last year
        for borough in ['Queens', 'Brooklyn', 'Manhattan', 'Bronx']: # For all boroughs in NYC
            # Regex the excel url
            excel_url = site + re.findall(f"href=\"(.*{date}.*{borough}.*.xls.*)\"", resp.content.decode('utf-8'), re.IGNORECASE)[0]
            
            # Get the excel
            raw_excel = requests.get(excel_url).content
            df = pd.read_excel(raw_excel, skiprows = range(0,3))
            
            # Calculate a simple primary key from excel
            if 'p_key' not in df.columns:
                df['p_key'] = df.apply(convert_row_to_p_key, axis=1)
            
            insert_df(df=df, db_file=DB_FILE, table_name='house_data')

    # Upload current year as well
    for borough in ['Queens', 'Brooklyn', 'Manhattan', 'Bronx']: # For all 4 boroughs in NYC
        # Regex the excel url
        excel_url = site + re.findall(f"href=\"(.*{borough}.*.xls.*)\"", resp_curr.content.decode('utf-8'), re.IGNORECASE)[0]
        # Get the excel
        raw_excel = requests.get(excel_url).content
        df = pd.read_excel(raw_excel, skiprows = range(0,3))

        # Calculate a simple primary key from excel
        if 'p_key' not in df.columns:
            df['p_key'] = df.apply(convert_row_to_p_key, axis=1)

        insert_df(df=df, db_file=DB_FILE, table_name='house_data')  