#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
from pymongo import MongoClient
os.chdir("E:\\sample proj")
import glob


# In[2]:


files_cy  = glob.glob('*cy*.csv')
files_ytd = glob.glob('*ytd*.csv')


# In[3]:


try: 
    conn = MongoClient() 
    print("Connected successfully!!!") 
except:   
    print("Could not connect to MongoDB")


# In[4]:


db = conn.empdb


# In[5]:


for csv in files_cy:
    
    if 'cy_2017' in csv:
        df = pd.read_csv(csv)
        collection = db.emp_cy_2017
        collection.remove()
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
        
    
        


# In[6]:


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

