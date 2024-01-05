#Uses the Rest API to retrieve the metric definition based on a search result object and output to a CSV file.

import requests
import json
#from pandas.io.json import json_normalize
import pandas as pd
import csv
import jmespath as jq
#from jsonpath_ng import jsonpath, parse as jp

###Parameters
serverName = '.cloud.microstrategy.com'
userName=''
pwd=''
projectName=''
searchGUID = ''
csv_file = 'c:/tmp/metrics.csv'
searchResultsLimit = '-1' #-1 unlimited, default=50 max=200
base_url ='https://' + serverName + '/MicroStrategyLibrary'
api_url = base_url + '/api'

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

def metricDefinition(metricGUID):        
    #print(metric_GUID)
    metricDef = requests.get(api_url + '/model/metrics/' + metricGUID + '?showExpressionAs=tree&showFilterTokens=true&showAdvancedProperties=true', headers=headers_prj, cookies=cookies)
    
    metric_json = json.loads(metricDef.text)
    #print(metricDef.text)
    
    metric_modTS = jq.search('information.dateModified',metric_json)
    metric_expression = jq.search('expression.text',metric_json)        
    
    metric_type_test = jq.search("contains(keys(expression.tree),'children')",metric_json)   
    
    if metric_type_test == True:
        compoundMetricInd = 1
    else:
        compoundMetricInd = 0
    
    metric_condition = jq.search('conditionality.filter.name',metric_json)
    metric_conditionGUID = jq.search('conditionality.filter.id',metric_json)
    metric_condition_removeelements = jq.search('conditionality.removeElements',metric_json)

    return metric_modTS, metric_expression, compoundMetricInd, metric_condition, metric_condition_removeelements
    
##### Body    
    

authToken, cookies, headers_svr = login(api_url,userName,pwd)
projectGUID, headers_prj = MSTRProject_api(api_url, headers_svr, cookies, proj_name)    
              
offsetVal=0
searchResults = requests.get(api_url + '/searchObjects/' + searchGUID + '/results?offset=' + str(offsetVal) + '&limit=' + searchResultsLimit + '&includeAncestors=true&includeAcl=false', headers=headers_prj, cookies=cookies)
search_dict = dict(searchResults.json())
print(search_dict)

totalItems = int(search_dict["totalItems"])

if searchResultsLimit == '-1':
    searchResultsLimitInt = 100000
else:
    searchResultsLimitInt = int(searchResultsLimit)

if totalItems > searchResultsLimitInt:
    metric_count = searchResultsLimitInt
else:
    metric_count = totalItems
    
print(metric_count)

csv.register_dialect('csvDialect', delimiter=';', quoting=csv.QUOTE_NONNUMERIC)

with open(csv_file, 'w', newline='') as file:
    writer = csv.writer(file, dialect='csvDialect')
    writer.writerow(["MetricGUID","metric_name","location","metric_modTS","compoundMetricInd", "metric_expression", "metric_condition","metric_conditionGUID", "metric_condition_removeelements"])

    i=0

    while i < metric_count:

        metricGUID = search_dict["result"][i]["id"]
        metric_name = search_dict["result"][i]["name"]        
        print(metric_name + ":" + metricGUID )
        ancestors_dict = search_dict["result"][i]["ancestors"]
        #print(objectLocation(ancestors_dict)
        location = objectLocation(ancestors_dict)
        metric_modTS, metric_expression, compoundMetricInd, metric_condition,metric_conditionGUID, metric_condition_removeelements = metricDefinition(metricGUID)
        #print(metric_GUID)      
       
        writer.writerow([metricGUID,metric_name,location,metric_modTS, compoundMetricInd, metric_expression, metric_condition, metric_conditionGUID, metric_condition_removeelements])

        i += 1
        
"""
#Get metric definition
metricGUID = '007738C74801580F543E48A40D34A591'
#print(metric_GUID)

metricDef = requests.get(api_url + '/model/metrics/' + metricGUID + '?showExpressionAs=tree&showFilterTokens=true&showAdvancedProperties=true', headers=headers_prj, cookies=cookies)
metric_dict = dict(metricDef.json())
print(metric_dict)
"""        
        
"""
#Test function MetricDefinition
metricGUID = '007738C74801580F543E48A40D34A591'
metric_modTS, metric_expression, compoundMetricInd, metric_condition, metric_condition_removeelements = metricDefinition(metricGUID)
#print(metric_GUID)
print(metric_modTS, metric_expression, compoundMetricInd, metric_condition, metric_condition_removeelements)   
"""        