#!/usr/bin/env python
# coding: utf-8

import mysql.connector
import pandas as pd
import smartsheet
import logging
import os
import sys 
import credentials_inputs 

# Create Log file / configure logging
logging.basicConfig(
    filename='log.log',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# Load in Parameters
# DB Cred
DB_HOST = credentials_inputs.DB_HOST
DB_USER = credentials_inputs.DB_USER
DB_PASSWORD = credentials_inputs.DB_PASSWORD
DB_NAME = credentials_inputs.DB_NAME

# Query
QUERY_1 = credentials_inputs.QUERY_1
QUERY_2 = credentials_inputs.QUERY_2

# Smartsheet
SMARTSHEET_API_TOKEN = credentials_inputs.SMARTSHEET_API_TOKEN
SMARTSHEET_SHEET_ID = credentials_inputs.SMARTSHEET_SHEET_ID

# Configure database credentials and establish connection
try:
    mydb = mysql.connector.connect(
      host=DB_HOST,
      user=DB_USER,
      password=DB_PASSWORD,
      database=DB_NAME
    )
except:
    logging.info("Can't connect to MySQL server")
    print("Can't connect to MySQL server")
    sys.exit()

# Create cursor object to excute SQL statments
mycursor = mydb.cursor()

# Execute SQL Query_1
try:
    mycursor.execute(QUERY_1)
    
except:
    logging.info("Unable to execute MySQL query_1")
    print("Unable to execute MySQL query_1")
    sys.exit()

# Stage query results
myresult = mycursor.fetchall()

# Load query results into a pandas dataframe
df_not_child = pd.DataFrame(myresult)

# Execute SQL Query_2
try:
    mycursor.execute(QUERY_2)
except:
    logging.info("Unable to execute MySQL query_2")
    print("Unable to execute MySQL query_2")
    sys.exit()

# Stage query results
myresult = mycursor.fetchall()

# Load query results into a pandas dataframe
df_child = pd.DataFrame(myresult)

# Join both dataframes on campaign_id and perform dataframe restructuring 
df_joined = pd.merge(df_not_child, df_child, on = 6 , how='left')
df_joined = df_joined.fillna(value = {'5_y':0})
df_joined["a"] = df_joined['5_x'] + df_joined["5_y"]
df_joined.a = df_joined.a.astype(int)
df_joined = df_joined[['0_x','1_x','2_x','3_x','4_x','a',6]]
df_joined = df_joined.copy()
df_joined = df_joined.rename(columns={'0_x':0,'1_x':1,'2_x':2,'3_x':3,'4_x':4,'a':5})

# Create base client object and set the access token
smartsheet_client = smartsheet.Smartsheet(SMARTSHEET_API_TOKEN)

# Make sure we don't miss any errors
smartsheet_client.errors_as_exceptions(True)

# Get sheet object and extract all the row ids within it
# Elements within sheet object are accessable via dot notation
try:
    sheet = smartsheet_client.Sheets.get_sheet(SMARTSHEET_SHEET_ID)
except:
    logging.info("Can't retrive Smartsheet sheet object")
    print("Can't retrive Smartsheet sheet object")
    sys.exit()

# Extract all the row-ids within sheet object
# Elements within sheet object are accessable via dot notation
try:
    sheet_rows =[]
    for row in sheet.rows:
        sheet_rows.append(row.id)
except:
    logging.info("Can't extract smartsheet rows")
    print("Can't extract smartsheet rows")
    sys.exit()

# Extract all the column-ids within sheet object
# Elements within sheet object are accessable via dot notation
try:
    sheet_columns =[]
    for column in sheet.columns:
        sheet_columns.append(column.id)
except:
    logging.info("Can't extract smartsheet columns")
    print("Can't extract smartsheet columns")
    sys.exit()

# If present, delete all rows in sheet
if sheet_rows:
    try:
        response = smartsheet_client.Sheets.delete_rows(SMARTSHEET_SHEET_ID, ids=sheet_rows, ignore_rows_not_found=False)
    except:
        logging.info("Can't delete existing smartsheet rows")
        print("Can't delete existing smartsheet rows")
        sys.exit()

# Iterate over dataframe rows, add rows to list
error_num = 1
for index, row in df_joined.iterrows():
    #Specify cell values for one row
    row_a = smartsheet.models.Row()
    #row_a.to_top = True
    row_a.cells.append({
      'column_id': sheet_columns[0], #campaign_id
      'value': row[6],
    })

    row_a.cells.append({
      'column_id':sheet_columns[1], #campaign_name
      'value': row[0],
    })

    row_a.cells.append({
      'column_id': sheet_columns[2], #IBM_Plan
      'value': row[1],
    })

    row_a.cells.append({
      'column_id': sheet_columns[3], #IBM_Program
      'value': row[2],
    })
    
    row_a.cells.append({
      'column_id': sheet_columns[4], #Tactic
      'value': row[3],
    })
    
    row_a.cells.append({
      'column_id': sheet_columns[5], #placement_start_date
      'value': str(row[4]),
    })
    
    row_a.cells.append({
      'column_id': sheet_columns[6], #num_placements
      'value': row[5],
    })


    # Add rows in list to sheet and catch errors
    try:
        response = smartsheet_client.Sheets.add_rows(
            SMARTSHEET_SHEET_ID,       # sheet_id
            [row_a])
    except:
        logging.info(f'Error #{error_num} REASON STATED ABOVE || Row Details: Query Row #{index} Campaign Id: {row[0]}')
        print(f'Error #{error_num} REASON STATED IN LOG || Row Details: Query Row #{index} Campaign Id: {row[0]}')
        error_num +=1


