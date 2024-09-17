import pandas as pd
import json
import logging
import sys
import requests
import datetime
from datetime import timedelta
import os
import pytz
import time
import fnmatch
import time
import subprocess
import csv
import os
import argparse

#logging.captureWarnings(True)
# parser = argparse.ArgumentParser(description="Proactive Bad Tags Monitoring")
# parser.add_argument('--server_token', type=str, required=True, help='Put where server should the tokens be fetched')
# parser.add_argument('--server_fetch', type=str, required=True, help='Put what server to fetch the raw values')
# parser.add_argument('--username', type=str, required=True, help='Put what server to fetch the raw values')
# parser.add_argument('--password', type=str, required=True, help='Put what server to fetch the raw values')

# args = parser.parse_args()
# print(args.server_token, args.server_fetch, args.username, args.password)

# server_fetch = args.server_fetch
# server_token = args.server_token
# username_input = args.username
# password_input = args.password

# token_url = f"https://{server_token}.na.pg.com/uaa/oauth/token" 


# token_url = "https://AMI-MESOPSHFHC.na.pg.com/uaa/oauth/token" 
# token_url = "https://AMI-MESOPSHQFHC.na.pg.com/uaa/oauth/token" 

def get_new_token():
    logging.captureWarnings(True)
    client_id = 'historian_public_rest_api'
    client_secret = 'publicapisecret'
    #account credentials
    username = 'golpe.rv' 
    password = 'Oragonsince1999!'
    data = {'grant_type': 'password','username': username, 'password': password}
    token_url = f"https://CAB-MESDC3BC.na.pg.com/uaa/oauth/token"   #LDAP auth server 
    access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
    tokens = json.loads(access_token_response.text)
    return tokens['access_token']

def timezone():
    CAB_timezone = pytz.timezone("CET") 
    now = datetime.datetime.now(CAB_timezone)
    return now

# def start_time():
#     start_time = (timezone() - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#     start_time = start_time[:-4]+"Z"
#     return start_time

# def end_time():
#     end_time = (timezone()- timedelta(minutes=16)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#     end_time = end_time[:-4]+"Z"
#     return end_time



def getCV():
    #print("Fetching data from excel")
    path = f"E:/Program Files/Splunk/etc/apps/PG_MFG_PROD_DB_Inputs/bin/CAB-EVENT-TAGS__filtered.csv"
    #path = f"BC-EVENT-TAGS__filtered.csv"
    # path = f"BC-PROFQ-TAGS_filtered.csv"
    row = pd.read_csv(path)
    
    # Load the progress file to keep track of extracted rows
    try:
        progress_df = pd.read_csv('progress.csv')
        extracted_rows = progress_df['Extracted Rows'].tolist()
    except FileNotFoundError:
        extracted_rows = []

    # Check if there are remaining rows to extract
    if len(row) > len(extracted_rows):
        # Calculate the index range to extract
        start_index = len(extracted_rows)
        end_index = min(start_index + 20, len(row))

        # Extract the tag names
        tagName = row.loc[start_index:end_index-1, 'VALUE'].tolist()
        #print (f"Start Index = {start_index}, End Index = {end_index}")
        #print (tagName)
        tagName = ';'.join([str(item) for item in tagName])
        #print (tagName)

        # Update the progress file with the extracted rows
        extracted_rows.extend(range(start_index, end_index))
        progress_df = pd.DataFrame({'Extracted Rows': extracted_rows})
        progress_df.to_csv('progress.csv', index=False)
        output_df = pd.read_csv('progress.csv')
        output_df.to_csv('progress.csv', index=False)
        
        # Tag Extraction
        try:
            #Server = "BC-MESHSTQA"
            Server = "CAB-MESDC3BC"
            start_timer = time.time()  
            token = get_new_token()
            end_timer = time.time()
            elapsed_time = end_timer - start_timer
            #print(f"elapsed time for token:{elapsed_time} ")  
            df = pd.DataFrame()
            headers = {"Authorization": f"Bearer {token}"}
           #end_time is 15 minutes before the current time
            current_time = timezone()
            end_time = (current_time- timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            end_time = end_time[:-4]+"Z"
            #start_time is the current time
            start_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            start_time = start_time[:-4]+"Z"
            get_link = f"https://{Server}.na.pg.com/historian-rest-api/v1/datapoints/raw?tagNames={tagName}&start={start_time}&end={end_time}&count=1&direction=1"
            #print (get_link)
            data = requests.get(get_link, headers=headers).json()
            data_json = json.dumps(data)
            #print(data_json)
            df = df.append(pd.DataFrame.from_dict(data['Data'][:]),ignore_index=True) 
            # pd.set_option('display.max_colwidth', None)
            # df2 = pd.DataFrame(iter(df["Samples"].apply(lambda ls: ls[0])))
            df2 = pd.DataFrame(iter(df["Samples"].apply(lambda ls: ls[0] if len(ls) > 0 else None).dropna()))
            df3 = pd.concat([df, df2], axis=1)
            df4 = df3[['TagName', 'TimeStamp', 'Value', 'Quality']]
            df4['TimeStamp'] = pd.to_datetime(df4['TimeStamp']) + timedelta(hours=2)
            #Convert dataframe df4 to key-value pair
            for index, row in df4.iterrows():
                timestamp = row['TimeStamp']
                tagname = row['TagName']
                value = row['Value']
                quality = row['Quality']
                output = f"TimeStamp={timestamp}, TagName={tagname}, Value={value}, Quality={quality}"
                print(output)
            # # df4.to_csv(f"C:\\Users\\golpe.rv\\Procter and Gamble\\GBUSS Mfg IT Operations - General\\Intelligent Operations\\Operations Engineering Initiatives\\Early Detection of Bad Tags using Historian REST API\\Phase 2.5 - Alert System\\Current_values_latest_file.csv", index=False)
            # # df4.to_csv(f"Current_values_latest_file.csv", index=False)
            
            # # output_file = 'Current_Test.csv'
            
            # # if os.stat(output_file).st_size == 0:
            # #     headers = True
            # # else:
            # #     headers = False  
            # # headers=False
            # # df4.to_csv(output_file, mode='a', index=False, header=headers)
            # # print(headers)
            # # print('File Saved')
            # # # df4.to_csv('Current_values_latest_file.csv', mode='a', index=False, header=True)
            
            # # #else
            # # print("Append", len(row), " ", len(progress_df))
            
            # # print(df4)
        except requests.exceptions.RequestException as e:
            print("Error occurred during tag extraction:", e)
    else:
        print('No more rows to extract from the Excel file. Resetting progress')
        progress_df = pd.DataFrame({'Extracted Rows': []})
        progress_df.to_csv('progress.csv', index=False)
        print("No rows", len(row), " ", len(progress_df))
        # with open("Current_Test.csv", "w") as file:
        #     file.truncate(0)
        #     print('Data Erased')
            # data = ['TagName', 'TimeStamp', 'Value', 'Quality']
            # writer = csv.writer(file)
            # writer.writerow(data)
            
        # df_truncated = pd.read_csv('Current_values_latest_file.csv')







# timezone()
# start_time()
# end_time()
# Start the timer

start_time = time.time()        
getCV()
# End the timer
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the elapsed time
#print(f"Elapsed time: {elapsed_time} seconds")

# result = subprocess.run(['python', 'FINAL_TEST.py'])




