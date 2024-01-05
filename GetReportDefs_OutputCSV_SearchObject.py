#Uses the MicroStrategy Rest API. Retrieve report defenitions based on the results of a search object and outpus to a CSV.

import requests
import json
#from pandas.io.json import json_normalize
#import pandas as pd
#import numpy as np
import csv
import jmespath as jq
#from jsonpath_ng import jsonpath, parse as jp
from operator import length_hint 
import array as arr


serverName = '.cloud.microstrategy.com'
userName=''
pwd=''
proj_name = ''
searchGUID = ''
searchResultsLimit = '-1' #-1 unlimited, default=50 max=200
#userName=input("User Name:")
#pwd=input("Password:")

base_url ='https://' + serverName + '/MicroStrategyLibrary'
api_url = base_url + '/api'
csv_file = 'c:/tmp/reports.csv'


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
    

def reportDefinition(reportGUID):
    #reportGUID = 'D312F8654DC4A5A220BF91A6D1A81301'
    
    reportDef = requests.get(api_url + '/model/reports/' + reportGUID + '?showExpressionAs=tree&showFilterTokens=true&showAdvancedProperties=false', headers=headers_prj, cookies=cookies)
    report_json = json.loads(reportDef.text)
    report_dict = dict(reportDef.json())
    #print(reportDef.text)
    
    report_name = jq.search('information.name',report_json)
    report_modTS = jq.search('information.dateModified',report_json)
    report_sourcetype = jq.search('sourceType',report_json)
    #print(report_modTS)
    
   
    if report_sourcetype == 'normal':
        #print(reportDef.text)
        templateAttributes = jq.search('dataSource.dataTemplate.units[?type ==`attribute`].[id, type, name]',report_json)
        #print(templateAttributesRes)
        if templateAttributes is None:
            templateAttributes = [["-","-","-"]]
            
        templateMetrics = jq.search('dataSource.dataTemplate.units[?type ==`metrics`].elements[].[id, subType, name, isEmbedded]',report_json)
        if templateMetrics is None:
            templateMetrics = [["-","-","-","-"]]
        #print(templateMetricsRes)
        #templateAttributes.append(templateAttributesRes)
        #templateMetrics.append(templateMetricsRes)
    else:
        templateAttributes = [["-","-","-"]]
        templateMetrics = [["-","-","-","-"]]   

    #print(templateAttributes)
    #print(templateMetrics)

    return templateAttributes, templateMetrics , report_sourcetype


authToken, cookies, headers_svr = login(api_url,userName,pwd)
projectGUID, headers_prj = MSTRProject_api(api_url, headers_svr, cookies, proj_name)

"""
reportGUID ='44DB1D7711D617516000828DC030CF97'
reportDef = requests.get(api_url + '/model/reports/' + reportGUID + '?showExpressionAs=tree&showFilterTokens=true&showAdvancedProperties=false', headers=headers_prj, cookies=cookies)
report_json = json.loads(reportDef.text)
report_dict = dict(reportDef.json())
print(reportDef.text)
"""

offsetVal=0
searchResults = requests.get(api_url + '/searchObjects/' + searchGUID + '/results?offset=' + str(offsetVal) + '&limit=' + searchResultsLimit + '&includeAncestors=true&includeAcl=false', headers=headers_prj, cookies=cookies)
search_dict = dict(searchResults.json())
#print(search_dict)

totalItems = int(search_dict["totalItems"])

if searchResultsLimit == '-1':
    searchResultsLimitInt = 100000
else:
    searchResultsLimitInt = int(searchResultsLimit)

if totalItems > searchResultsLimitInt:
    object_count = searchResultsLimitInt
else:
    object_count = totalItems
    
print(object_count)

csv.register_dialect('csvDialect', delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
with open(csv_file, 'w', newline='') as file:
    writer = csv.writer(file, dialect='csvDialect')
    writer.writerow(["ReportGUID","ReportName","location","ReportSourceType","report_modTS","objectGUID", "objectType", "objectName","isEmbedded"])
     i=0

    while i < object_count:
    
        reportGUID = search_dict["result"][i]["id"]
        report_name = search_dict["result"][i]["name"]   
        report_modTS = search_dict["result"][i]["dateModified"]            
        print(str(i) + ":" + report_name + ":" + reportGUID )
        ancestors_dict = search_dict["result"][i]["ancestors"]
        location = objectLocation(ancestors_dict)
        
        templateObjects, templateAttributes, templateMetrics, report_sourcetype = reportDefinition(reportGUID)
        
        #print(templateAttributes)
        """        
        for obj in templateObjects:
            #print(report_name, attr[0], attr[1],attr[2])
            #print(length_hint(obj))
            objectGUID = obj[0]
            objectType = obj[1]
            objectName = obj[2]
            if length_hint(obj) > 3:
                isEmbedded = obj[3]
            else:
                isEmbedded = ""
                    
            writer.writerow([reportGUID, report_name,location,report_sourcetype,report_modTS, objectGUID, objectType,objectName,isEmbedded])
        """
        for attr in templateAttributes:
            #print(report_name, attr[0], attr[1],attr[2])
            #print(length_hint(obj))
            objectGUID = attr[0]
            objectType = attr[1]
            objectName = attr[2]
            isEmbedded = ""
            
            writer.writerow([reportGUID, report_name,location,report_sourcetype,report_modTS, objectGUID, objectType,objectName,isEmbedded])
        
        for metr in templateMetrics:
            #print(report_name, attr[0], attr[1],attr[2])
            #print(length_hint(obj))
            objectGUID = metr[0]
            objectType = metr[1]
            objectName = metr[2]
            isEmbedded = metr[3]
            writer.writerow([reportGUID, report_name,location,report_sourcetype,report_modTS, objectGUID, objectType,objectName,isEmbedded])
        
        i +=1