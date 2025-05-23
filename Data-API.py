#!/usr/bin/env python
# coding: utf-8

# In[34]:


import cbsodata
import pandas as pd
from IPython.display import display
from dotenv import load_dotenv
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# In[36]:


data = pd.DataFrame(cbsodata.get_data('84709ENG'))

display(data.info())


# In[44]:


# Removing unwanted dara
filtered_data = data[(data['Sex'] != 'Total male and female') & 
(data['PersonalCharacteristics'].isin(['Age: 6 to 11 years', 'Age: 12 to 17 years','Age: 18 to 24 years','Age: 25 to 34 years','Age: 35 to 49 years','Age: 50 to 64 years','Age: 65 to 74 years','Age: 75 years or older'])) &
(data['ModesOfTravel'] != 'Total') &
(data['Margins'] != 'Value') &
(data['RegionCharacteristics'].isin(['Noord-Nederland (LD)','Oost-Nederland (LD)','West-Nederland (LD)','Zuid-Nederland (LD)','Groningen (PV)','Frysl\u00e2n (PV)','Drenthe (PV)','Overijssel (PV)','Flevoland (PV)','Gelderland (PV)','Utrecht (PV)','Noord-Holland (PV)','Zuid-Holland (PV)','Zeeland (PV)','Noord-Brabant (PV)','Limburg (PV)']))]

filtered_data = filtered_data[['ID','Sex','PersonalCharacteristics','ModesOfTravel','RegionCharacteristics','Periods','Trips_4','DistanceTravelled_5','TimeTravelled_6']]

filtered_data = filtered_data.rename(columns={
        'ID': 'id',
        'Sex': 'sex',
        'PersonalCharacteristics': 'agegroup',
        'RegionCharacteristics': 'region',
        'ModesOfTravel': 'mode',
        'Trips_4': 'tripsavg',
        'DistanceTravelled_5': 'distanceavg',
        'TimeTravelled_6': 'timeavg'
    })

non_null = filtered_data[filtered_data['tripsavg'] > 0]
display(filtered_data.info())


# In[46]:


print("User:", os.getenv("SNOWFLAKE_USER"))
print("Password:", os.getenv("SNOWFLAKE_PASSWORD"))
print("Account:", os.getenv("SNOWFLAKE_ACCOUNT"))
print("Warehouse:", os.getenv("SNOWFLAKE_WAREHOUSE"))
print("Database:", os.getenv("SNOWFLAKE_DATABASE"))
print("Schema:", os.getenv("SNOWFLAKE_SCHEMA"))
print("Role:", os.getenv("SNOWFLAKE_ROLE"))

print(filtered_data.columns.tolist())


# In[50]:


# Load environment variables from .env file
load_dotenv()

# Access Snowflake credentials from environment variables
conn_params = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE'),
}

# Connect and upload
conn = snowflake.connector.connect(**conn_params)
success, nchunks, nrows, _ = write_pandas(conn, filtered_data, table_name='TRIPS', database='DBT_TRAINING', schema='NL_TRANSPORT', quote_identifiers=False)

if success:
    print(f'Successfully uploaded {nrows} rows')
else:
    print('Upload failed')

conn.close()

