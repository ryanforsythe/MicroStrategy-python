#MicroStrategy Rest API
#Addto_ContentGroup_RestAPI_ReportData_JSON
#Created due to the fact that the Workstation Content Group functionality doesn't do a great job of providing folder or GUID context.

import requests
import json
#from pandas.io.json import json_normalize
import pandas as pd

serverName = '.cloud.microstrategy.com'
userName=''
pwd=''
projectName = ''
contentGroupID = '2A5B949544E2BE6511CCC898867F42A5'
#userName=input("User Name:")
#pwd=input("Password:")

api_url = 'https://' + serverName + '/MicroStrategyLibrary/api'

plaURL = api_url
plauserName = userName
plapwd = pwd
plaProjectName ='Platform Analytics'

"""
dossierList = ["3E63CD1649B024748831059FA28E7FB6"
,"9FF25F8C44E4B54A596BC49A9588D818"
]
"""

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
report_dict = dict(reportInstance.json())
#print(reportInstance)     

num_rows = report_dict["result"]["data"]["paging"]["total"]   

resultArray =[]  

i = 0
while i < num_rows:
       #Adjust based on report_dict results
      resultArray.append(report_dict["result"]["data"]["root"]["children"][0]["children"][i]["element"]["formValues"]["GUID"])
      i += 1

"""
gridObjects = {
        'requestedObjects': {
        'attributes': [
            {'name': 'Object',
            'id': '4169F1B14C72AB2B061B93A6629B84E9',
            'type': 'Attribute',
            'forms': [
                #{'id': 'CCFBE2A5EADB4F50941FB879CCF1721C',
                #'name': 'Folder Name',
                #'dataType': 'Char',
                #'baseFormCategory': 'DESC',
                #'baseFormType': 'Text'},
               {'id': 'F8B7953640503B097C6333A0C5CFA011',
                'name': 'GUID',
                'dataType': 'Char',
                'baseFormCategory': 'Object None',
                'baseFormType': 'Text'}]}
                ]
        }
     }
"""

#gridData = requests.get(plaURL + '/reports/' + plaReportGUID, headers=headers_rpt, data=gridObjects, cookies=cookies)

#grid_obj = json.loads(gridData)

#Add Dossier to Content Group

authToken, cookies, headers_svr = login(api_url,userName,pwd)
              

contentGroup = requests.get(api_url + '/contentGroups/' + contentGroupID, headers=headers_svr, cookies=cookies)
print("Content Group:" + str(contentGroup.status_code))

dossierList = resultArray #comment out if using manual list.

for d in dossierList:

    payload = {
      "operationList": [
        {
          "op": "add",
          "path": '/' + projectGUID,
          "value": [
            {"id": d,"type": 55}
          ],
          "id": 1
        }
      ]
    }

    response = requests.patch(api_url + '/contentGroups/' + contentGroupID + '/contents', headers=headers_svr, data=json.dumps(payload),cookies=cookies)
    print("Content Group Add: " + d + " " + str(response.status_code))
    #print(response.reason)
    #print(response.text)
    
    
responseCG = requests.get(api_url + '/contentGroups/' + contentGroupID + '/contents?projectId=' + projectID , headers=headers_svr,cookies=cookies)
    