from mstrio.access_and_security.privilege import Privilege
from mstrio.connection import Connection
from mstrio.object_management.migration import *
from mstrio.types import ObjectTypes
from mstrio.users_and_groups.user import User
import requests
import json
#from pandas.io.json import json_normalize
import pandas as pd
import numpy as np
from IPython.display import display

userName='ryan.forsythe@agdata.com'
pwd='Jarvis!8'
auth_url = "https://altus-bpi-stage.agdata.net/MIApplications"
api_url = auth_url + "/api"
mstrAuth = {"username": userName, "password": pwd,"loginMode": 1, "maxSearch": 3}
PROJECT_NAME = "Crop Canada"

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
        
authToken, cookies = login(api_url,userName,pwd)

headers_srvr = {'X-MSTR-AuthToken': authToken,
              'Content-Type': 'application/json',
              'Accept': 'application/json'}
              
packages = requests.get(api_url + '/migrations', headers=headers_srvr, cookies=cookies)
packages_dict = json.loads(packages.text)
print(packages.text)
package_cnt = packages_dict["total"]
print(package_cnt)              


packagesdf = pd.json_normalize(packages_dict,record_path=['data'],sep="_")
#packagesdf.loc[:,'packageInfo.id']

colsOutput = packagesdf[['packageInfo_id','packageInfo_name','packageInfo_lastUpdatedDate']]
#df.columns = ['packageGUID', 'packageName']
display(colsOutput)