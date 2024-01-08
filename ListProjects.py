from mstrio.server import compare_project_settings, Environment, Project
from mstrio.connection import get_connection, Connection
import pandas as pd
from pandas import option_context, DataFrame


conn = get_connection(workstationData)

env = Environment(connection=conn)
loaded_projects_as_dict = env.list_loaded_projects(to_dictionary=True)
#loaded_projects = env.list_loaded_projects()
df = pd.DataFrame.from_dict(loaded_projects_as_dict)
print(df[["id","name","status"]])

#df.to_csv('c:/temp/project_data.csv', index=False)