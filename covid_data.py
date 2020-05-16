#!/usr/bin/env python
# coding: utf-8

# In[5]:


#Load all relevant packages
import psycopg2
from sqlalchemy import create_engine
import json,urllib.request
import io
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
# You need to put your DB credentials here
engine = create_engine('postgresql+psycopg2://sibcxdnhdulocz:d3a54d00394cbe70f7a4188114f06d25589b9b05b682c590ec30ef1076aafae8@ec2-52-200-119-0.compute-1.amazonaws.com:5432/d5rufljbg16502',use_batch_mode=True)


# In[6]:


#Get countries using code snippet from RapidAPI
url = "https://covid-19-data.p.rapidapi.com/help/countries"

querystring = {"format":"json"}

headers = {
    'x-rapidapi-host': "covid-19-data.p.rapidapi.com",
    'x-rapidapi-key': "8eb50e7026msh3b2fec8b017e037p1be492jsn2ea6becf9583"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

#Convert response into dataframe
output_countries = json.loads(response.text)
df_countries = json_normalize(output_countries)

#Send to database
#df_countries.to_sql('countries', engine, if_exists='replace', index=False, method = 'multi')


# In[ ]:


#Get Daily Cumulative Totals
from datetime import date, timedelta

start_date = date(2020, 1,2)
end_date = date(2020, 4, 22)
delta = timedelta(days=1)

while start_date <= end_date:
    url = "https://covid-19-data.p.rapidapi.com/report/totals"
    querystring = {"date-format":"YYYY-MM-DD","format":"json","date":start_date}

    headers = {
        'x-rapidapi-host': "covid-19-data.p.rapidapi.com",
        'x-rapidapi-key': "8eb50e7026msh3b2fec8b017e037p1be492jsn2ea6becf9583"
        }

    response3 = requests.request("GET", url, headers=headers, params=querystring)
    output3 = json.loads(response3.text)
    df3 = json_normalize(output3)
    df3['date'] = start_date
    df3.to_sql('daily_cumul_total', engine, if_exists='append', index=False, method = 'multi')
    print(df3)
    start_date += delta


# In[82]:





# In[8]:


from datetime import date, timedelta
df_countries_trunc = df_countries[(df_countries['latitude'] > 36) & (df_countries['longitude'] <= 13)]
df_countries_trunc
countries_list = df_countries_trunc['name'].to_list()
url = "https://covid-19-data.p.rapidapi.com/report/country/name"

for x in countries_list:
    start_date = date(2020, 2,15)
    end_date = date(2020, 4, 20)
    delta = timedelta(days=1)
    while start_date <= end_date:
        querystring = {"date-format":"YYYY-MM-DD","format":"json","date":start_date,"name":x}

        headers = {
            'x-rapidapi-host': "covid-19-data.p.rapidapi.com",
            'x-rapidapi-key': "8eb50e7026msh3b2fec8b017e037p1be492jsn2ea6becf9583"
            }

        response = requests.request("GET", url, headers=headers, params=querystring)
        output = json.loads(response.text)
        df = json_normalize(output[0])
        list = df['provinces'][0]
        df2 = pd.DataFrame(list)
        country = str(df['country'][0])
        df2['country_name'] = country
        df2['date'] = start_date
        if len(df2.columns) == 7:
            df2.to_sql('daily_cumul_province', engine, if_exists='append', index=False, method = 'multi')
        start_date += delta


# In[ ]:
