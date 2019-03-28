#!/usr/bin/env python
# coding: utf-8

# # 1.Importing required modules

# In[ ]:


import os
import pandas as pd
from pymongo import MongoClient
os.chdir("E:\\sample proj")
import glob
import shutil
from datetime import datetime
from time import sleep
import sys
import logging
logging.basicConfig(filename="logfile.log", level=logging.INFO)


# # 2.Searching for files

# In[ ]:


while True:
    path,dirs,files=next(os.walk("E:\\sample proj"))
    file_count=len(files)
    logging.info('searching for files')
    if file_count > 6:
        files_cy  = glob.glob('*cy*.csv')
        files_ytd = glob.glob('*ytd*.csv')
        logging.info('files found moving to next process')
        logging.info('spliting files')
        break
        
    else:
        logging.info('waiting for files')
        sleep(20)


# # 3.Connect to server & database

# In[ ]:


try: 
    conn = MongoClient() 
    print("Connected successfully!!!") 
    logging.info('connecting to database')
except:   
    print("Could not connect to MongoDB")
    logging.info('error while coonecting to database')
    


# In[ ]:


db = conn.empdb


# # 4.Separating the files and insert into mongo colletions

# In[ ]:


for csv in files_cy:
    
    if 'cy_2017' in csv:
        # reading the csv file
        
        df = pd.read_csv(csv)
        
        #connecting to table
        collection = db.emp_cy_2017
        
        #removing previous data
        collection.remove()
        
        #updating recent data
        collection.insert(df.to_dict('records'))
        
    elif 'cy_2018' in csv:
        df = pd.read_csv(csv)
        collection = db.emp_cy_2018
        collection.remove()
        collection.insert(df.to_dict('records'))
        
    elif 'cy_2019' in csv:
        df = pd.read_csv(csv)
        collection = db.emp_cy_2019
        collection.remove()
        collection.insert(df.to_dict('records'))
        
logging.info('inserted CY data into database')        


# In[ ]:


for csv in files_ytd:
    
    if 'ytd_2017' in csv:
        df = pd.read_csv(csv)
        collection = db.emp_ytd_2017
        collection.remove()
        collection.insert(df.to_dict('records'))
        
    elif 'ytd_2018' in csv:
        df = pd.read_csv(csv)
        collection = db.emp_ytd_2018
        collection.remove()
        collection.insert(df.to_dict('records'))
        
    elif 'ytd_2019' in csv:
        df = pd.read_csv(csv)
        collection = db.emp_ytd_2019
        collection.remove()
        collection.insert(df.to_dict('records'))

logging.info('inserted YTD data into database') 


# # 5.backup the files

# In[ ]:


today = datetime.now()
name = today.strftime('%Y%m%d')
path = os.getcwd()
shutil.make_archive(name,'zip',path)
logging.info('creating backup for files')

