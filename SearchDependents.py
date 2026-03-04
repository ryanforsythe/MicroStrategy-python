from mstrio.object_management import (
    Folder,
    full_search,
    get_my_personal_objects_contents,
    get_predefined_folder_contents,
    get_search_results,
    get_search_suggestions,
    list_folders,
    list_objects,
    Object,
    PredefinedFolders,
    quick_search,
    quick_search_from_object,
    SearchObject,
    SearchPattern,
    SearchResultsFormat,
    start_full_search
)
from mstrio.project_objects import *
from mstrio.types import ObjectSubTypes, ObjectTypes
from mstrio.helpers import Rights

from mstrio.connection import get_connection, Connection

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
from IPython.display import display
import pandas as pd

projectID = '01FD8F93A34CF4471AE9E69993D4C752'
projectGUID = '01FD8F93A34CF4471AE9E69993D4C752' #Source Dossier Project
searchGUID = ''
searchResultsLimit = '-1' #-1 unlimited, default=50 max=200
#userName=input("User Name:")
#pwd=input("Password:")
userName=''
pwd=''
base_url ='https://.cloud.microstrategy.com/MicroStrategyLibrarySTD'
api_url = base_url + '/api'
mstrAuth = {"username": userName, "password": pwd,"loginMode": 1, "maxSearch": 3}
csv_file = 'c:/tmp/reports.csv'

conn = Connection(base_url, userName, pwd, project_id=projectGUID)

objectGUID = '44DAFFFE11D617516000828DC030CF97'

dependentsList = full_search(
    conn,
    projectGUID,
    uses_object_id=objectGUID,
    uses_object_type=ObjectTypes.TRANSFORMATIONS,
    uses_recursive='True'
)

#display(dependentsList)
df = pd.DataFrame.from_dict(dependentsList)

display(df)