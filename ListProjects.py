from mstrio.connection import Connection, get_connection
from mstrio.server import compare_project_settings, Environment, Project
import pandas as pd
from pandas import option_context, DataFrame
from mstrio.connection import get_connection, Connection

#BASE_URL = 'https://altus-uat.agdata.net/AltusApplications/api'  # Insert URL for your env here
#MSTR_USERNAME = 'Administrator'  # Insert your env username here
#MSTR_PASSWORD = ''  # insert your mstr password here
#PROJECT_ID = 'DE3A4B4C4000D196AAD863A96EE97732' #Platform Analytics  # Insert you project ID here
#conn = Connection(BASE_URL, MSTR_USERNAME, MSTR_PASSWORD, project_id=PROJECT_ID)

conn = get_connection(workstationData)

env = Environment(connection=conn)
loaded_projects_as_dict = env.list_loaded_projects(to_dictionary=True)
#loaded_projects = env.list_loaded_projects()
df = pd.DataFrame.from_dict(loaded_projects_as_dict)
print(df[["id","name","status"]])

#df.to_csv('c:/temp/project_data.csv', index=False)