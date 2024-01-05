#Create shortcuts in a target folder based on a list of report GUIDs.

import requests
import json
import csv
from IPython.display import display
import pandas as pd
from pandas import option_context, DataFrame


serverName = '.cloud.microstrategy.com'
userName=''
pwd=''
proj_name = ''
targetFolderGUID = ''
base_url ='https://' + serverName + '/MicroStrategyLibrary'
api_url = base_url + '/api'
#csv_file = 'c:/tmp/reports.csv'
xlsx_file = 'c:/tmp/reports.xlsx'
#https://community.microstrategy.com/s/article/KB16048-List-of-all-object-types-and-object-descriptions-in?language=en_US
objectTypeId = 3

def login(baseURL,username,password):
    header = {'username': username,
              'password': password,
              'loginMode': 1}
    r = requests.post(baseURL + '/auth/login', data=header)
    if r.ok:
        authToken = r.headers["x-mstr-authtoken"]
        cookies = dict(r.cookies)
        print("Token: " + authToken)
        print("Session ID: {}".format(cookies))
        headers_svr = {'X-MSTR-AuthToken': authToken,
                       'Content-Type': 'application/json',
                       'Accept': 'application/json'}
        return authToken, cookies, headers_svr
    else:
        print("HTTP {} - {}, Message {}".format(r.status_code, r.reason, r.text))
        return []


def objectLocation(ancestors_dict):

    count = 0
    locationStr = ""
    #print(ancestors_dict)
    for record in ancestors_dict:
        if "name" in record:
            count +=1
    #print(count)

    fldr = 1

    while fldr < count :
        folderName = ancestors_dict[fldr]["name"]
        locationStr =  locationStr + '/' + folderName
        fldr += 1

    return locationStr

def MSTRProject_api(api_url, headers_svr, cookies, proj_name):
    projectProps = requests.get(api_url + '/projects/' + proj_name, headers=headers_svr, cookies=cookies)
    project_dict = dict(projectProps.json())
    projectGUID = project_dict["id"]
    projectName = project_dict["name"]
    print(projectGUID, projectName)

    headers_prj = {'X-MSTR-AuthToken': authToken,
                   'Content-Type': 'application/json',
                   'Accept': 'application/json',
                   'X-MSTR-ProjectID': projectGUID}

    return projectGUID, headers_prj
    
authToken, cookies, headers_svr = login(api_url,userName,pwd)
projectGUID, headers_prj = MSTRProject_api(api_url, headers_svr, cookies, proj_name)    

df = pd.read_excel(xlsx_file)
display(df)

#Adjust for Excel input
objectGUIDList = df['Object GUID']
display (objectGUIDList)

payload = {'folderId': targetFolderGUID}

for ind in objectGUIDList.index:
    print(df['Object GUID'][ind])
    objectGUID = df['Object GUID'][ind]
    shortcut = requests.post(api_url + '/objects/' + objectGUID + '/type/' + str(objectTypeId) + '/shortcuts', headers=headers_prj, cookies=cookies, data=json.dumps(payload) )
    print(shortcut.status_code)