print('started')
# Import the package
# The requests library is a popular Python library used for making HTTP requests.
# It simplifies sending HTTP requests and handling responses
import requests

import pandas as pd
pd.set_option('display.max_columns', None)
import csv
github_api_url = "https://api.github.com/repos/squareshift/stock_analysis/contents/"

# from requests module using get function
# This line sends an HTTP GET request to the URL stored in github_api_url.
response = requests.get(github_api_url) # returns response object
# print(response)--> <Response [200]>
# print(type(response))  --> <class 'requests.models.Response'>
# The response variable in your example is an instance of the Response class from the requests.models module.
# This means it is an object, even though its class is specifically Response
# print(response.status_code) --> This prints the HTTP status code of the response (e.g., 200 for success, 404 for not found).
# print(response.content) --> data in raw form
# print(type(response.content)) --> bytes
if response.status_code ==200:
    files = response.json() # This converts the JSON content of the response to a Python dictionary or list.
else:
    print('404 for not found')
    
# print(type(response.json())) --> print(type(response.content))

# print(files)
download_url_list = [file['download_url'] for file in files if file['name'].endswith('.csv')]


removed_url = download_url_list.pop() 
print(1)
print(removed_url)
print(2)
# print(removed_url) # removing symbol_metadata.csv so that we can combine to data frame later
d = pd.read_csv(removed_url) # read removed url and convert to dataframe

# print(d)
# print(d.columns)

dataframes=[]
file_names=[]
for url in download_url_list:

    file_name = url.split("/")[-1].replace(".csv", "")
    df = pd.read_csv(url)
    df['Symbol'] = file_name
    dataframes.append(df)
    file_names.append(file_name)

# print(df)
# print(dataframes)
# print(file_names)

combined_df = pd.concat(dataframes, ignore_index=True) # ignore_index=True argument ensures that the resulting DataFrame has a new continuous index.

# print(combined_df.columns)
o_df = pd.merge(combined_df,d,on='Symbol',how='left')
# print(o_df)
# print(o_df.columns)
# print(o_df[2:5])
result = o_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
# print(result)
# print(o_df["timestamp"])
# print(o_df["timestamp"].dtype)
o_df["timestamp"] = pd.to_datetime(o_df["timestamp"]) # converting data type of column timestamp from object to datetime
# print(o_df.dtypes)
# print(o_df["timestamp"])
filtered_df = o_df[(o_df['timestamp'] >= "2021-01-01") & (o_df['timestamp'] <= "2021-05-26")]# filtering the data frame based on timeperiod
# print(filtered_df)
result_df = filtered_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
# print(result_df)
list_sector = ["TECHNOLOGY","FINANCE"]
result_df = result_df[result_df["Sector"].isin(list_sector)].reset_index(drop=True)
print(result_df)
path="/home/mukunthan/Documents/Personal/Learning/dataengineering/big-data/stock-poc/output_{}.csv"
file_list = result_df["Sector"].to_list()
for i in file_list:
    print(i)
    filtered_df = result_df[result_df["Sector"]==i]
    path =path.format(i)
    filtered_df.to_csv(path)
# path=r"/home/mukunthan/Documents/Personal/Learning/BigData/stock-poc/output.csv"
# result_df.to_csv(path,header=True)
# print("data has been written successfully")
print('nothing')
print('success')
