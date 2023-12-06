#MicroStrategy Rest API
#Addto_ContentGroup_RestAPI_ReportData_JSON

import requests
import json
#from pandas.io.json import json_normalize
import pandas as pd
import numpy as np

projectID = '92F117EF491CF3D633AB7982C831B065'
#userName=input("User Name:")
#pwd=input("Password:")
userName=''
pwd=''
api_url = "https://altus-bpi-stage.agdata.net/MIApplications/api"
mstrAuth = {"username": userName, "password": pwd,"loginMode": 1, "maxSearch": 3}

plaURL = "https://altus.agdata.net/AltusApplications/api"
plaProjectID = 'DE3A4B4C4000D196AAD863A96EE97732'
plaReportGUID = 'D388AA794807B1887AE9F1A63A8722D8'

"""
dossierList = ["3E63CD1649B024748831059FA28E7FB6"
,"9FF25F8C44E4B54A596BC49A9588D818"
,"8BED26434D504E485EABC3A7312A5912"
,"1CB931A3408EC86BE3DBB6BA15604630"
,"1554FB0E4ECF74733516C8A1136C6E33"
,"479C678C4E92C0EA3CE54FB0E8E0AE82"
,"A341263A40F2E1304AD359BB5D3389F9"
,"00DF51664E035E9D427F37903EC8ADB8"
,"C536CB954300E48F77A627B504E36A1B"
,"D4C84A074A9CD4982D06CA9968306A22"
,"F00F120747034F7BE03F919E78C5F3EF"
,"4EB61E7A4E0CDFA3FD46E2B3E4C704DC"
,"95A0D79140AF48C1929D19A015A86BC7"
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
        return authToken, cookies
    else:
        print("HTTP {} - {}, Message {}".format(r.status_code, r.reason, r.text))
        return []

#Get Platform Analytics Data
authToken, cookies = login(plaURL,userName,pwd)



headers_rpt = {'X-MSTR-AuthToken': authToken,
              'Content-Type': 'application/json',
              'Accept': 'application/json',
              'X-MSTR-ProjectID': plaProjectID}

reportInstance = requests.post(plaURL + '/reports/' + plaReportGUID + '/instances', headers=headers_rpt, cookies=cookies)
print("Report Instance:" + str(reportInstance.status_code))
report_dict = dict(reportInstance.json())
#print(reportInstance)     

num_rows = report_dict["result"]["data"]["paging"]["total"]   

resultArray =[]  

i = 0
while i < num_rows:
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

#Use resultArray to populate Content Group
authToken, cookies = login(api_url,userName,pwd)
              
headers_cg = {'X-MSTR-AuthToken': authToken,
              'Content-Type': 'application/json',
              'Accept': 'application/json'}

contentGroupID = '2A5B949544E2BE6511CCC898867F42A5'
contentGroup = requests.get(api_url + '/contentGroups/' + contentGroupID, headers=headers_cg, cookies=cookies)
print("Content Group:" + str(contentGroup.status_code))

dossierList = resultArray

for d in dossierList:

    payload = {
      "operationList": [
        {
          "op": "add",
          "path": "/92F117EF491CF3D633AB7982C831B065",
          "value": [
            {"id": d,"type": 55}
          ],
          "id": 1
        }
      ]
    }

    response = requests.patch(api_url + '/contentGroups/' + contentGroupID + '/contents', headers=headers_cg, data=json.dumps(payload),cookies=cookies)
    print("Content Group Add: " + d + " " + str(response.status_code))
    #print(response.reason)
    #print(response.text)
    
responseCG = requests.get(api_url + '/contentGroups/' + contentGroupID + '/contents?projectId=' + projectID , headers=headers_cg,cookies=cookies)
    