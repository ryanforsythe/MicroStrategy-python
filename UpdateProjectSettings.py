from mstrio.connection import get_connection
from mstrio.server import compare_project_settings, Environment, Project

conn = get_connection(workstationData)
env = Environment(connection=conn)

loaded_projects = env.list_loaded_projects()

PROJECT_NAME = 'Platform Analytics'
project = Project(connection=conn, name=PROJECT_NAME)

#project_settings_df = project.settings.to_dataframe()
#print(project_settings_df)

for project in env.list_loaded_projects():
    print(project.name)
    project.settings.enableStatisticsMobileClientLocation=1
    project.settings.collectStatistics=1
#    project.settings.update()
    #print(project.name, project.settings.enableStatisticsMobileClientLocation, project.settings.collectStatistics)
    #print(project.settings.collectStatistics)
