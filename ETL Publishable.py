#!/usr/bin/env python
# coding: utf-8

# In[1]:
#Libraries
import requests
import pandas as pd
import json
from pandas import json_normalize
from datetime import datetime, timedelta
import random as rand
import logging
from sqlalchemy import create_engine


# In[2]:


#Database Connection

connection_uri = "mysql+mysqlconnector://*******************************"
engine = create_engine(connection_uri)

# In[3]:


# Authorization Function

def Authorization(ti):
    url = "https://secure-wms.com/AuthServer/api/Token"

    payload = json.dumps({
      "grant_type": "**************",
      "user_login_id": "****************"
    })
    headers = {
      'Host': 'secure-wms.com',
      'Connection': 'keep-alive',
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'Authorization': 'Basic ********************************',
      'Accept-Encoding': 'gzip,deflate,sdch',
      'Accept-Language': 'en-US,en;q=0.8'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    access_token = response.json().get("access_token")
    
    ti.xcom_push(key='access_token', value=access_token)


# In[4]:


# EXTRACT function

def Extract(ti, StartDate, EndDate):

    access_token = ti.xcom_pull(task_ids='Auth_task',key='access_token')

    URL = "https://secure-wms.com/inventory/receivers?detail=All"
    Authorization = "Bearer " + access_token
    
    headers  = {"Accept": "application/json",
                "Host":"secure-wms.com",
                "Content-Type":"application/hal+json; charset=utf-8",
                "Accept":"application/json","Accept-Language":"en-US,en;q=0.8",
                "Authorization":Authorization} 
    
    params={"rql": f"ArrivalDate=ge={StartDate};ArrivalDate=lt={EndDate};ReadOnly.Status==1", "detail":"All", "itemDetail":"All"}
    
    #Sending the request
    resp = requests.get(URL , headers = headers,params=params)
    
    #Cathing Connection Error
    if resp.status_code != 200:
        print('3pl connection error: ' + str(resp.status_code))
    else:
        print('3pl connection Success')
    
    #Responded data
    Raw_res = resp.json()

    #ti.xcom_push(key='Raw', value=Raw_res)
    ti.xcom_push(key='Raw', value=json.dumps(Raw_res))
    #return(Raw_res)


# In[5]:


#Transforming Function
def transform(ti):
    #Raw_res = ti.xcom_pull(task_ids='Extract_task',key='Raw')
    Raw_res = json.loads(ti.xcom_pull(task_ids='Extract_task', key='Raw'))


    Flatted_df = pd.json_normalize(Raw_res["ResourceList"])


    Rownumber = Flatted_df.shape[0]

    #create new column
    Flatted_df['total Qty'] = 0
    Flatted_df['total WeightImperial'] = 0
    Flatted_df['Lines Count'] = 0
    Flatted_df['PalletCount'] = 0

        
    #Qty        
    for i in range(Rownumber):
        try:
            total_Qty = pd.json_normalize(Flatted_df['ReceiveItems'][i])["Qty"].sum()
            Flatted_df.loc[i,'total Qty']  = total_Qty
        
        except:
            print(f'Qty error row number: {i}')
            
    #Weight        
    for i in range(Rownumber):
        try:
            total_WeightImperial = pd.json_normalize(Flatted_df['ReceiveItems'][i])["WeightImperial"].sum()
            Flatted_df.loc[i,'total WeightImperial']  = total_WeightImperial
        
        except:
            print(f'Weight not available row number: {i}')
        
    #Lines        
    for i in range(Rownumber):
        try:
            lines_count = pd.json_normalize(Flatted_df['ReceiveItems'][i])["ItemIdentifier.Sku"].nunique()
            Flatted_df.loc[i,'Lines Count'] = lines_count
        
        except:
            print(f'Lines error row number: {i}')
        
    #Pallets        
    for i in range(Rownumber):
        try:
            Pallet_count= pd.json_normalize(Flatted_df['ReceiveItems'][i])["PalletInfo.Label"].nunique()
            Flatted_df.loc[i,'PalletCount'] = Pallet_count
        
        except:
            print(f'Pallet error row number: {i}')
            
            
            
     #Missing Values
    Flatted_df.fillna(value={"Carrier":"UNKNOWN"},inplace=True)
    
    #Data type
    Flatted_df['ArrivalDate'] = pd.to_datetime(Flatted_df['ArrivalDate'])
    Flatted_df['ArrivalDate'] = Flatted_df['ArrivalDate'].dt.floor('1s')##because my database can't support fractional seconds
    Flatted_df['ArrivalDate'] = Flatted_df['ArrivalDate'].astype(str)## airflow jasonizing problem       
            
     # Organize the output
    Output=Flatted_df.loc[:,['ReferenceNum','ArrivalDate','ReadOnly.ReceiverId','ReadOnly.CustomerIdentifier.Name','ReadOnly.CustomerIdentifier.Id',
                  'ReadOnly.Status','total Qty','total WeightImperial','Lines Count','PalletCount','Carrier']]
    Output.rename(columns={'ReadOnly.ReceiverId':'TransactionId','ReadOnly.CustomerIdentifier.Name':'Customer_name','ReadOnly.CustomerIdentifier.Id':'Customer_Id',
                      'ReadOnly.Status':'status'},inplace=True)
    Filtered_Receipt_Output = Output.loc[:,['ArrivalDate','TransactionId','Carrier','total Qty','total WeightImperial','Lines Count','PalletCount','Customer_Id']]
    Filtered_Receipt_Output.rename(columns={'TransactionId':'Transaction_Id','total Qty':"Qty",'total WeightImperial':'Weight','Lines Count':'Line','PalletCount':'Pallet'},inplace=True)
    
    output_data_list = Filtered_Receipt_Output.to_dict(orient='records')
    return(output_data_list)
    

# In[6]:


#Load Function -- into csv file, also sql db
def loadToxlsx(ti):
    output_data_list = ti.xcom_pull(task_ids='transform_task')
    Filtered_Receipt_Output = pd.DataFrame(output_data_list)

    date = (datetime.now()- pd.offsets.BusinessDay(n=1)).strftime('%Y-%m-%d')
    Random = rand.randint(1111,9999)
    
    #writing output
    Filtered_Receipt_Output.to_excel(f'/Users/Arianayazdi/Downloads/Filtered_Receipt_Output{Random}_{date}.xlsx')
    #Flatted_df.to_excel(f'Output Record/Receipts/FullDetail_Receipt_Output{Random}_{today}.xlsx')


# In[7]:


def LoadToSQL(df):
    try:
        Filtered_Receipt_Output.to_sql(
            name='Inbound',
            con=engine,
            if_exists='append',
            index=False)
        print("Data written to MySQL successfuly.")
    
    except Exception as e:
        print("Error:",e)




    


