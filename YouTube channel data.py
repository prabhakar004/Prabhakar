#!/usr/bin/env python
# coding: utf-8

# In[7]:


import json


# In[40]:


import requests
import pandas as pd
import pandasql as ps
import os


# In[73]:


import pymysql


# In[76]:


URL="https://www.googleapis.com/youtube/v3/channels?key=AIzaSyDt5RCukN1plquKk0N8bw7d4MuV0_-3_Wo&id=UCHfLNbW2oe7gjcmtoueqRig&part=snippet,contentDetails,statistics,status"


# In[77]:


page=requests.get(URL)


# In[78]:


page


# In[79]:


json_struc=page.json()


# In[80]:


df = pd.json_normalize(json_struc['items'])


# In[81]:


df


# In[82]:


df1= df[df.columns[df.columns.isin(['id','snippet.title','snippet.description','snippet.publishedAt','contentDetails.relatedPlaylists.uploads','statistics.viewCount','statistics.subscriberCount','statistics.hiddenSubscriberCount','statistics.videoCount'])]]
df1


# In[83]:


df1.columns=['ID','TITLE', 'DESCRIPTION','PUBLISHEDAT','RELATEDPLAYLIST','VIEWCOUNT','SUBSCRIBERCOUNT','HIDDENSUBSCRIBERCOUNT','VIDEOCOUNT']
df1


# In[84]:


df1.to_csv('/Users/prabhakarbalaram/Downloads/youtubechannel.csv')


# In[85]:


os.makedirs('/Users/prabhakarbalaram/Downloads',exist_ok=True)
df1.to_csv('/Users/prabhakarbalaram/Downloads/youtubechannel.csv',index=False)


# In[86]:


rav=pd.read_csv('/Users/prabhakarbalaram/Downloads/youtubechannel.csv')
rav.columns=['ID','TITLE', 'DESCRIPTION','PUBLISHEDAT','RELATEDPLAYLIST','VIEWCOUNT','SUBSCRIBERCOUNT','HIDDENSUBSCRIBERCOUNT','VIDEOCOUNT']


# In[87]:


rav


# In[88]:


connection= pymysql.connect(host='localhost',
user ='root',
password='prabhakar',
db='db2')

cursor=connection.cursor()


# In[90]:


cols = "`,`".join([str(i) for i in rav.columns.tolist()])



# Insert DataFrame recrds one by one.
for i,row in rav.iterrows():
    sql = "INSERT INTO `YTUBE_CHANNELS` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))



# the connection is not autocommitted by default, so we must commit to save our changes
connection.commit()


# In[ ]:




