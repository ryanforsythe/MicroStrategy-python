#Update a metric using a Platform Analytics report as a data set. Uses the MicroStrategy Rest API.
#This script specifically changes the Remove related elements from true to false.

import requests
import json
#from pandas.io.json import json_normalize
#import pandas as pd
#import numpy as np
import csv
import jmespath as jq
#from jsonpath_ng import jsonpath, parse as jp

serverName = '.cloud.microstrategy.com'
userName=''
pwd=''
projectName = ''
base_url ='https://' + serverName + '/MicroStrategyLibrary'
api_url = base_url + '/api'

plaURL = api_url
plauserName = userName
plapwd = pwd
plaProjectName ='Platform Analytics'
plaReportGUID = ''

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
        
#Get Platform Analytics Data
authToken, cookies, headers_svr = login(plaURL,plauserName,plapwd)
projectGUID, headers_prj = MSTRProject_api(plaURL, headers_svr, cookies, plaProjectName)    

            
reportInstance = requests.post(plaURL + '/reports/' + plaReportGUID + '/instances', headers=headers_prj, cookies=cookies)
print("Report Instance:" + str(reportInstance.status_code))
report_json = json.loads(reportInstance.text)
report_num_rows = jq.search('result.data.paging.total', report_json)
print(report_num_rows)


resultArray =[]  

report_row_iter = 0

#May need to adjust based on report definition.
metricInclude = jq.search('result.data.root.children[' + str(report_row_iter) + '].children[0].children[0].children[0].element.formValues.ID',report_json)
print(metricInclude)
metricGUID = jq.search('result.data.root.children[' + str(report_row_iter) + '].element.formValues.ID',report_json)

while report_row_iter < report_num_rows:
    metricInclude = jq.search('result.data.root.children[' + str(report_row_iter) + '].children[0].children[0].children[0].element.formValues.ID',report_json)
    #print(metricInclude)
    if metricInclude == 'x':
        metricGUID =  jq.search('result.data.root.children[' + str(report_row_iter) + '].element.formValues.ID',report_json)
        resultArray.append(metricGUID)
        #print(metricGUID)

    report_row_iter +=1

print(resultArray)

authToken, cookies, headers_svr = login(api_url,userName,pwd)
projectGUID, headers_prj = MSTRProject_api(api_url, headers_svr, cookies, ProjectName)    
                         
changesetDef = requests.post(api_url + '/model/changesets?schemaEdit=false', headers=headers_prj, cookies=cookies)
changesetDef_json = json.loads(changesetDef.text)

#print(changesetDef.text)
changesetId = jq.search('id',changesetDef_json)
headers_chgset = {'X-MSTR-AuthToken': authToken,
              'Content-Type': 'application/json',
              'Accept': 'application/json',
              'X-MSTR-MS-Changeset': changesetId}       

for metricGUID in resultArray:
    metricDef = requests.get(api_url + '/model/metrics/' + metricGUID + '?showExpressionAs=tree&showFilterTokens=true&showAdvancedProperties=true', headers=headers_prj, cookies=cookies)
    metric_json = json.loads(metricDef.text)
    #update the remove related elements to false
    metric_json['conditionality']['removeElements']="FALSE"
    metricUpdate = requests.put(api_url + '/model/metrics/' + metricGUID + '?showExpressionAs=tree&showFilterTokens=true&showAdvancedProperties=true', headers=headers_chgset, data=json.dumps(metric_json), cookies=cookies)
    print(str(metricUpdate.status_code) + ':' + metricGUID)
    #print(metricUpdate.text)              
    
changesetPost = requests.post(api_url + '/model/changesets/' + changesetId + '/commit', headers=headers_prj, cookies=cookies)
print(changesetPost.text)    