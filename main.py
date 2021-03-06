#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import pandas as pd
import smartsheet
import logging
import os
import sys
import credentials_inputs
import datetime


# In[2]:


# For debugging: Displays entire dataframe
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', -1)


# In[3]:


# Create Log file / configure logging
logging.basicConfig(
    filename='log.log',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


# In[4]:


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

CAMPAIGN_KEY_FORMULA = credentials_inputs.CAMPAIGN_KEY_FORMULA
FINAL_PLAN_FORMULA = credentials_inputs.FINAL_PLAN_FORMULA
FINAL_PROGRAM_FORMULA = credentials_inputs.FINAL_PROGRAM_FORMULA


# In[5]:


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


# In[6]:


# Create cursor object to excute SQL statments
mycursor = mydb.cursor()


# In[7]:


# Execute SQL Query_1
try:
    mycursor.execute(QUERY_1)

except:
    logging.info("Unable to execute MySQL query_1")
    print("Unable to execute MySQL query_1")
    sys.exit()


# In[8]:


# Stage query results
myresult_1 = mycursor.fetchall()


# In[9]:


# Load query results into a pandas dataframe
df_not_child = pd.DataFrame(myresult_1)


# In[10]:


# Execute SQL Query_2
try:
    mycursor.execute(QUERY_2)
except:
    logging.info("Unable to execute MySQL query_2")
    print("Unable to execute MySQL query_2")
    sys.exit()


# In[11]:


# Stage query results
myresult_2 = mycursor.fetchall()


# In[12]:


# Load query results into a pandas dataframe
df_child = pd.DataFrame(myresult_2)


# In[13]:


# Create id columns
df_not_child[8] = df_not_child[1] +'_'+ df_not_child[2] +'_'+ df_not_child[3] +'_'+ df_not_child[4].astype(str)
df_child[8] = df_child[1] +'_'+ df_child[2] +'_'+ df_child[3] +'_'+ df_child[4].astype(str)


# In[14]:


# Join both dataframes on campaign_id and perform dataframe restructuring
df_joined = pd.merge(df_not_child, df_child, on = 8 , how='left')
df_joined = df_joined.fillna(value = {'7_y':0})
df_joined["a"] = df_joined['7_x'] + df_joined["7_y"]
df_joined.a = df_joined.a.astype(int)
df_joined = df_joined[['0_x','1_x','2_x','3_x','4_x','5_x','6_x','a',8]]
df_joined = df_joined.copy()
df_joined = df_joined.rename(columns={'0_x':0,'1_x':1,'2_x':2,'3_x':3,'4_x':4,'5_x':5,'6_x':6,'a':7})


# In[15]:


# Apply date buckets
for i in df_joined.index:
    if df_joined.at[i, 4].day < 10:
        df_joined.at[i, 4] = df_joined.at[i, 4].replace(day= 1)
    elif df_joined.at[i, 4].day > 9:
        if df_joined.at[i, 4].day < 20:
            df_joined.at[i, 4] = df_joined.at[i, 4].replace(day= 10)
        elif df_joined.at[i, 4].day > 19:
            df_joined.at[i, 4] = df_joined.at[i, 4].replace(day= 20)
        else:
            print(f'DATE BUCKETS ERROR: Row index#{i} of dataframe || REASON: 2nd Condition Not Satisfied || DATE:{df_joined.at[i, 4]}')
            logging.info(f'DATE BUCKETS ERROR: Row index#{i} of dataframe || REASON: 2nd Condition Not Satisfied || DATE:{df_joined.at[i, 4]}')
    else:
        print(f'DATE BUCKETS ERROR: Row index#{i} of dataframe || REASON: No Condition Satisfied || DATE:{df_joined.at[i, 4]}')
        logging.info(f'DATE BUCKETS ERROR: Row index#{i} of dataframe || REASON: No Condition Satisfied || DATE:{df_joined.at[i, 4]}')


# In[ ]:


# Apply new date format
#for i in df_joined.index:
#    month = str(df_joined.at[i, 4].month)
#    day = str(df_joined.at[i, 4].day)
#    year = str(df_joined.at[i, 4].year)
#    if len(month) < 2:
#        month = '0'+month
#    
#    if len(day) < 2:
#        day = '0'+day
#    
#    if len(year) < 4:
#        year = '20'+year
#                   
#    df_joined.at[i, 4] = f'{month}/{day}/{year}'


# In[16]:


# reapply column id with newly formated date
df_joined[8] = df_joined[1] +'_'+ df_joined[2] +'_'+ df_joined[3] +'_'+ df_joined[4].astype(str)

# Truncate year from YYYY to YY
# for index, row in df_joined.iterrows():
#    df_joined.at[index,8] = row[8][:-2]


# In[17]:


#Remove Duplicates
df_joined = df_joined.groupby([0,1,2,3,4,5,6,8])[7].sum().reset_index()


# In[18]:


# Create base client object and set the access token
smartsheet_client = smartsheet.Smartsheet(SMARTSHEET_API_TOKEN)


# In[19]:


# Make sure we don't miss any errors
smartsheet_client.errors_as_exceptions(True)


# In[20]:


# Get sheet object
# Elements within sheet object are accessable via dot notation
try:
    sheet = smartsheet_client.Sheets.get_sheet(SMARTSHEET_SHEET_ID)
except:
    logging.info("Can't retrive Smartsheet sheet object")
    print("Can't retrive Smartsheet sheet object")
    sys.exit()


# In[21]:


# Extract all the column-ids within sheet object
# Elements within sheet object are accessable via dot notation
try:
    sheet_columns ={}
    for column in sheet.columns:
        sheet_columns[column.title] = column.id
except:
    logging.info("Can't extract smartsheet columns")
    print("Can't extract smartsheet columns")
    sys.exit()


# In[22]:


# If present, delete all rows in sheet
# Elements within sheet object are accessable via dot notation
try:
    sheet_rows=[]
    for row in sheet.rows:
        sheet_rows.append(row.id)

        #Delete rows to sheet by chunks of 200
        if len(sheet_rows) > 199:
            smartsheet_client.Sheets.delete_rows(SMARTSHEET_SHEET_ID,sheet_rows)
            sheet_rows = []

    # Delete remaining rows
    if len(sheet_rows)>0:
        smartsheet_client.Sheets.delete_rows(SMARTSHEET_SHEET_ID,sheet_rows)
except:
    logging.info("Can't extract smartsheet columns")
    print("Can't extract smartsheet columns")
    sys.exit() 


# In[23]:


# Iterate over dataframe rows, add rows to list
error_num = 1
for index, row in df_joined.iterrows():
    #Specify cell values for one row
    row_a = smartsheet.models.Row()
    #row_a.to_top = True
    #row_a.cells.append({
      #'column_id': sheet_columns['Campaign Key'], #campaign_id
      #'value': row[8],
    #})

    row_a.cells.append({
      'column_id':sheet_columns['Campaign Name'], #campaign_name
      'value': row[0],
    })

    row_a.cells.append({
      'column_id': sheet_columns['IBM Plan'], #IBM_Plan
      'value': row[1],
    })

    row_a.cells.append({
      'column_id': sheet_columns['IBM Program'], #IBM_Program
      'value': row[2],
    })

    row_a.cells.append({
      'column_id': sheet_columns['Tactic'], #Tactic
      'value': row[3],
    })

    row_a.cells.append({
      'column_id': sheet_columns['Placement Start Date'], #placement_start_date
      'value': str(row[4]),
    })

    row_a.cells.append({
      'column_id': sheet_columns['Year'], #Year
      'value': row[5],
    })

    row_a.cells.append({
      'column_id': sheet_columns['Quarter'], #Quarter
      'value': row[6],
    })

    row_a.cells.append({
      'column_id': sheet_columns['# of Placements'], #num_placements
      'value': row[7],
    })
    
    #Smartsheet formulas
    row_a.cells.append({
      'column_id': sheet_columns['Campaign Key'], #Column values generated by smartsheet
      'formula': CAMPAIGN_KEY_FORMULA,
    })
 
    row_a.cells.append({
      'column_id': sheet_columns['Final Plan'], #Column values generated by smartsheet
      'formula': FINAL_PLAN_FORMULA,
    })
    
    row_a.cells.append({
      'column_id': sheet_columns['Final Program'], #Column values generated by smartsheet
      'formula': FINAL_PROGRAM_FORMULA,
    })
    
    # Add rows in list to sheet and catch errors
    try:
        response = smartsheet_client.Sheets.add_rows(
            SMARTSHEET_SHEET_ID,       # sheet_id
            [row_a])
    except:
        logging.info(f'Error #{error_num} REASON STATED ABOVE || Row Details: Query Row #{index} Campaign Name: {row[0]}')
        print(f'Error #{error_num} REASON STATED IN LOG || Row Details: Query Row #{index} Campaign Name: {row[0]}')
        error_num +=1


# In[ ]:




