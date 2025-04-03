# all utility functions into a single file called utils.py in the same directory as erosion.py, etc...
# and then import the functions from utils.py into erosion.py, etc..
# This will make the code more modular and easier to maintain. 

# import necessary libraries
# base packages
import os
#import sys
from datetime import datetime
import pathlib
#data packages
#from functools import reduce
#import sys??
#import sqlalchemy as sa
import pandas as pd
import numpy as np
#mapping packages
import arcpy
from arcgis import GIS
from arcgis.features import FeatureLayer
#import requests
from arcgis.geometry import SpatialReference
from functools import reduce
# external connection packages
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

gis = GIS()
wk_memory = "memory" + "\\"

# network path to connection files
filePath = "F:\\GIS\\DB_CONNECT"
#path to SEZ.gdb's to update and master data
master_path = "F:\GIS\PROJECTS\ResearchAnalysis\SEZ\Data\SEZ_Data.gdb"

### SDE Collection New data collected is put into SDE.Survey under the indicator name
### SDE Vector is where the data will go into staging tables 
sdeVector    = os.path.join(filePath, "Vector.sde")
sdeCollect = os.path.join(filePath, "Collection.sde")
# local variables sdata is starting data and f data is finishing datatables
sdemonitoring= os.path.join(sdeVector, "sde.SDE.Monitoring")
sdata = os.path.join(sdeCollect, "sde.SDE.Survey")
#Path to staging tables?

##Tables we get the data from in Collect #! 2010-2022 globalids don'tmatch so must get data from gdb in F drive
sezsurveytable = os.path.join(sdata, "sde.SDE.sez_survey")
erosiondata = os.path.join(sdata, "sde.SDE.Stream_Erosion")
incisiondata = os.path.join(sdata, "sde.SDE.sez_channel_incision")
invasivedata = os.path.join(sdata, "sde.SDE.sez_invasive_plant")
headcutdata = os.path.join(sdata, "sde.SDE.sez_stream_headcut")

#make this a spatial df
streamdata = os.path.join(sdemonitoring, "sde.SDE.Stream")

#Staging Tables in sde.Vector
# to post to staging tables in SEZ_Data.GDB replace master_path with sdeBase
stage_bank_stability = os.path.join(sdeVector, "sez_scores_bank_stability") 
#stage_All_SEZ_Scores = os.path.join(sdeVector, "All_SEZ_Scores")
stage_biotic_integrity = os.path.join(sdeVector, "sez_scores_biotic")
stage_headcuts = os.path.join(sdeVector, "sez_scores_headcut")
stage_incision = os.path.join(sdeVector, "sez_scores_incision")
stage_invasives = os.path.join(sdeVector, "sez_scores_invasive")
stage_vegetation = os.path.join(sdeVector, "sez_scores_vegetation_vigor")
stage_conifer = os.path.join(sdeVector, "sez_scoresconifer_encroachment")
stage_aquatic = os.path.join(sdeVector, "sez_scores_aquatic_organism")
stage_ditches = os.path.join(sdeVector, "sez_scores_ditch")
stage_habitat = os.path.join(sdeVector, "sez_scores_habitat_fragmentation")

#REST servive links to staging tables
bank_stability_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/4"
biotic_integrity_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/5"
conifer_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/6"
ditches_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/7"
invasives_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/11"
Hab_Frag_url = 'https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/8'
vegetation_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/12"
incision_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/10"
headcuts_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/9"
AOP_url= "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/3"
SEZ_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/0"


#Staging Tables in SEZ.gdb as a qa tool
stage_bank_stabilitygdb = os.path.join(master_path, "bank_stability") 
#stage_All_SEZ_Scores = os.path.join(sdeVector, "All_SEZ_Scores")
stage_biotic_integritygdb = os.path.join(master_path, "biotic_integrity")
stage_headcutsgdb = os.path.join(master_path, "headcuts_table")
stage_incisiongdb = os.path.join(master_path, "incision")
stage_invasivesgdb = os.path.join(master_path, "invasives")
stage_vegetationgdb = os.path.join(master_path, "vegetation_vigor")
stage_conifergdb = os.path.join(master_path, "conifer_encroachment")
stage_aquaticgdb = os.path.join(master_path, "aquatic_organism_passage_table")
stage_ditchesgdb = os.path.join(master_path, "ditches")
stage_habitatgdb = os.path.join(master_path, "habitat_fragmentation")

#location of gdb's data 2023 and earlier-Raw data from field
gdbworking_folder = "F:\GIS\GIS_DATA\Monitoring"
headcutgdbfolder = os.path.join(gdbworking_folder, "Stream_Headcut", "StreamHeadcut_Survey")
invasivegdbfolder = os.path.join(gdbworking_folder, "Invasive_Species", "Invasive_Species_Survey")
sezgdbfolder= os.path.join(gdbworking_folder, "SEZ", "SEZ_Survey")
## GDB with Raw Data straight from S123 not in the original folder (that one is not edited)
headcut23gdb = os.path.join(headcutgdbfolder, "Stream_Headcut_Survey_2023.gdb")
headcut22gdb = os.path.join(headcutgdbfolder, "Stream_Headcut_Survey_2022.gdb")
headcut20gdb = os.path.join(headcutgdbfolder, "Stream_Headcut_Survey_2020.gdb")
headcut19gdb = os.path.join(headcutgdbfolder, "Stream_Headcut_Survey_2019.gdb")
sez_surveygdb = os.path.join(sezgdbfolder, "SEZ_Survey_2023.gdb")

#channelincision23gdb = os.path.join(working_folder,"Channel_Incision","Channel_Incision_Survey","Channel_Incision_Survey_2023.gdb")
#channelincision22gdb = os.path.join(working_folder, "Channel_Incision","Channel_Incision_Survey","Channel_Incision_Survey_2022.gdb")
#channelincision20gdb = os.path.join(working_folder, "Channel_Incision","Channel_Incision_Survey","Channel_Incision_Survey_2020.gdb")
invasiveplant23gdb= os.path.join(invasivegdbfolder,"Invasive_Species_Survey_2023.gdb")
invasiveplant22gdb= os.path.join(invasivegdbfolder,"Invasive_Species_Survey_2022.gdb")
invasiveplant20gdb= os.path.join(invasivegdbfolder,"Invasive_Species_Survey_2020.gdb")
invasiveplant19gdb= os.path.join(invasivegdbfolder,"Invasive_Species_Survey_2019.gdb")
#current working directory
local_path = pathlib.Path().absolute()
# set data path as a subfolder of the current working directory TravelDemandModel\2022\
data_dir = local_path.parents[0] / 'data/raw_data'
# folder to save processed data
out_dir  = local_path.parents[0] / 'data/processed_data'
# workspace gdb for stuff that doesnt work in memory
gdb = os.path.join(local_path,'Workspace.gdb')

# set environement workspace to in memory 
arcpy.env.workspace = 'memory'
# # clear memory workspace
# arcpy.management.Delete('memory')

# overwrite true
arcpy.env.overwriteOutput = True
# Set spatial reference to NAD 1983 UTM Zone 10N
sr = arcpy.SpatialReference(26910)

# function to get sql connection for tabular TRPA data Collection.sde?
# db options are 'tabular' or 'collect'??
def get_conn(db):
    # Get database user and password from environment variables on machine running script
    db_user             = os.environ.get('DB_USER')
    db_password         = os.environ.get('DB_PASSWORD')

    # driver is the ODBC driver for SQL Server
    driver              = 'ODBC Driver 17 for SQL Server'
    # server names are
    sql_12              = 'sql12'
    sql_14              = 'sql14'
    # make it case insensitive
    db = db.lower()
    # make sql database connection with pyodbc
    # if db   == 'sde_tabular':
    #     connection_string = f"DRIVER={driver};SERVER={sql_12};DATABASE={db};UID={db_user};PWD={db_password}"
    #     connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    #     engine = create_engine(connection_url)
    if db == 'sde_collection':
        connection_string = f"DRIVER={driver};SERVER={sql_12};DATABASE={db};UID={db_user};PWD={db_password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
    elif db == 'sde':
        connection_string = f"DRIVER={driver};SERVER={sql_12};DATABASE={db};UID={db_user};PWD={db_password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
    # else return None
    else:
        engine = None
    # connection file to use in pd.read_sql
    return engine

#Used for setting up database connection to write scores to staging tables in vector sde.
#--------------------------------------------------------------------------------------------------------#
# # Function to create database connection
# def create_db_connection(version_name_full):
#     """
#     Creates a database connection to the specified version in the SQL Server.
#     """
#     arcpy.CreateDatabaseConnection_management(
#         out_folder_path='db_connections/',
#         out_name="ConnectionFile.sde",
#         database_platform="SQL_SERVER",  # Replace with your DBMS type (e.g., ORACLE, SQL_SERVER, POSTGRESQL)
#         instance="sql12",
#         database="sde",
#         account_authentication="DATABASE_AUTH",  # Use "OPERATING_SYSTEM" for OS authentication
#         username="DB_USER",
#         password="DB_PASSWORD", # use environment variable
#         version_type='TRANSACTIONAL',
#         version=version_name_full
#     )
#     #--------------------------------------------------------------------------------------------------------#
# # Function to insert data into a feature class using arcpy.da.InsertCursor
# def insert_data_to_cursor(data, field_names, target_fc):
#     """
#     Inserts data into a feature class using an InsertCursor.
    
#     Parameters:
#     - data: List of dictionaries containing the data to insert.
#     - field_names: List of field names in the target feature class.
#     - target_fc: The feature class where the data will be inserted.
#     """
#     with arcpy.da.InsertCursor(target_fc, field_names) as cursor:
#         for row in data:
#             cursor.insertRow([row[field] for field in field_names])

#--------------------------------------------------------------------------------------------------------#
# # Function to load CSV, prepare data, and insert into the feature class within an edit session
# def process_and_insert_csv(csv_file_path, version_name_full, target_fc):
#     """
#     Loads a CSV file into a pandas DataFrame, converts it to a list of dictionaries,
#     and inserts it into a feature class using arcpy.da.InsertCursor inside an edit session.
    
#     Parameters:
#     - csv_file_path: Path to the CSV file to load.
#     - version_name_full: The version name used to create the DB connection.
#     - target_fc: The feature class where the data will be inserted.
#     """
#     # Load CSV into pandas DataFrame
#     df = pd.read_csv(csv_file_path)

#     # Create a database connection
#     create_db_connection(version_name_full)

#     # Get field names from the DataFrame columns
#     field_names = list(df.columns)

#     # Convert the DataFrame to a list of dictionaries
#     data = df.to_dict(orient='records')

#     # Start editing session
#     db_connection = 'db_connections/ConnectionFile.sde'
#     edit = arcpy.da.Editor(db_connection)
#     edit.startEditing(False, True)  # Start the edit session (False = don't use undo/redo, True = allow editing)

#     try:
#         # Start an edit operation
#         edit.startOperation()

#         # Insert the data into the feature class
#         insert_data_to_cursor(data, field_names, target_fc)
#         print(f"Data successfully inserted into {target_fc}.")

#         # Stop the edit operation
#         edit.stopOperation()

#         # Stop the editing session and save the changes
#         edit.stopEditing(True)  # True = save edits

#     except Exception as e:
#         print(f"Error during editing session: {e}")
#         edit.stopOperation()  # If error occurs, discard the operation
#         edit.stopEditing(False)  # False = discard changes

# # Example of how to call the function:
# #csv_file_path = r"C:/path/to/your/file.csv"  # Update with your actual CSV file path
# #version_name_full = "SDE.YourVersionName"   # Replace with the correct version name
# #process_and_insert_csv(csv_file_path, version_name_full, TARGET_FC)

#Used for USFS rest service
# return query_result
def get_fs_data_spatial_query(service_url, query_params=None):
        # Initialize the feature layer from the given URL
        feature_layer = FeatureLayer(service_url)
        # Perform the query and retrieve the results as a Spatially Enabled DataFrame (sdf)
        query_result = feature_layer.query(query_params).sdf  # Pass the query directly
        return query_result

# Gets data with query from the TRPA server
def get_fs_data_query(service_url, query_params):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query(query_params)
    # Convert the query result to a list of dictionaries
    feature_list = query_result.features
    # Create a pandas DataFrame from the list of dictionaries
    all_data = pd.DataFrame([feature.attributes for feature in feature_list])
    # return data frame
    return all_data

# Gets spatially enabled dataframe from TRPA server
def get_fs_data_spatial(service_url):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query().sdf
    return query_result

# Gets data from the TRPA server
def get_fs_data(service_url):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query()
    # Convert the query result to a list of dictionaries
    feature_list = query_result.features
    # Create a pandas DataFrame from the list of dictionaries
    all_data = pd.DataFrame([feature.attributes for feature in feature_list])
    # return data frame
    return all_data

# Function to read feature class into DataFrame
def feature_class_to_dataframe(feature_class, fields):
    data = [row for row in arcpy.da.SearchCursor(feature_class, fields)]
    return pd.DataFrame(data, columns=fields)



#SPECIAL Definition Functions#
#Used in invasives? need to pinpoint
# Define a function to add missing columns and keep only required columns
def add_and_keep_columns(df, required_columns):
    for col in required_columns:
        if col not in df.columns:
            df[col] = np.nan
    return df[required_columns]

#location of Invasives Priority Lookup
#file_path = "F:\GIS\PROJECTS\ResearchAnalysis\SEZ\Invasives Priority lookup.csv"

#def create_invasive_dictionary(file_path):
    """
    Reads a CSV file and creates a lookup dictionary for invasive plant priorities.

    Parameters:
        file_path (str): Path to the CSV file containing invasive plant data.

    Returns:
        dict: A lookup dictionary with plant types as keys and their scientific names and priorities as values.
        function: A custom function to map plant types to their priorities.
    """
    # # Read the CSV file into a DataFrame
    # csv_data = pd.read_csv(file_path)

    # # Define the lookup dictionary
    # key = 'Common'
    # values = ['Scientific', 'Priority']
    # invasives_lookup = csv_data.set_index(key)[values].to_dict(orient='index')

    # # Define a custom function to map plant types to priorities
    # def map_priority(plant_type):
    #     if pd.isnull(plant_type):
    #         return 'None'  # Return 'None' for NaN values
    #     else:
    #         # Extract the priority from the dictionary, or return 'Unknown' if not found
    #         plant_info = invasives_lookup.get(plant_type)
    #         if plant_info:
    #             return plant_info['Priority']
    #         else:
    #             return 'Unknown'

    # return invasives_lookup, map_priority

# Grading for each parameter 
#Defining Grade for Bank Stability based on Erosiondf[percent_unstable]
def categorize_erosion(Percent_Unstable):
    if pd.isna(Percent_Unstable):
        return np.nan
    elif 0 <= Percent_Unstable < 5:
        return 'A'
    elif 5 <= Percent_Unstable < 20:
        return 'B'
    elif 20 <= Percent_Unstable < 50:
        return 'C'
    else:
        return 'D'
    
#Scoring based off of grades
def score_indicator(Rating):
    if pd.isna(Rating):
        return np.nan
    elif  Rating == 'A':
        return '12'
    elif Rating == 'B':
        return '9'
    elif Rating == 'C':
        return '6'
    else:
        return '3'

#Define Grade for Incision based off of incisino ratio

def categorize_incision(bankfull_ratio):
    if pd.isna(bankfull_ratio):
        return 'A'
    elif 0 <= bankfull_ratio < 1.2:
        return 'A'
    elif 1.2 <= bankfull_ratio < 1.6:
        return 'B'
    elif 1.6 <= bankfull_ratio < 2.1:
        return 'C'
    else:
        return 'D'

#Define Grade for Bioassessment Score
def categorize_csci(biotic_integrity):
     if pd.isna(biotic_integrity):
        return np.nan
     elif   biotic_integrity > 0.92:
        return 'A'
     elif 0.79 < biotic_integrity <= 0.92:
        return 'B'
     elif 0.62 < biotic_integrity <= 0.79:
        return 'C'
     else:
        return 'D'

#Define Priority List Level of Invasive Plant Species lookup list will change
# Process Data
def rate_invasive(row):
    # Sum of Level 3 and Level 4
    level_3_4_sum = row['Level 3'] + row['Level 4']
    # Sum of Level 1 and Level 2
    level_1_2_sum = row['Level 1'] + row['Level 2']
    
    if level_3_4_sum == 1 and row['Level 1'] == 0 and row['Level 2'] == 0:
        return 'B'  # Assign score B if the sum of Level 3 and Level 4 is 1
    elif level_3_4_sum == 2 or row['Level 1'] == 1 or row['Level 2'] == 1:
        return 'C'  # Assign score C if the sum of Level 3 and Level 4 is 2, or any Level 1 or Level 2 is 1
    elif level_3_4_sum >= 3 or row['Level 1'] >= 2 or row['Level 2'] >= 2 or level_1_2_sum >= 2:
        return 'D'  # Assign score D if Level 3 or Level 4 is 3 or more, or Level 1 or 2 is 2 or more, or the sum of Level 1 and Level 2 is 2 or more
    else:
        return 'A'  # Assign score A if no significant invasives are present
     
#Define Size for Headcut based off of headcut size
##A = 0 headcut, B 1+small headcut
def categorize_headcut(headcutdepth):
    if pd.isnull(headcutdepth) or headcutdepth == 0:
        return 'None'
    elif 0.1 <= headcutdepth < 0.5:
        return 'small'
    elif 0.5 <= headcutdepth < 1:
        return 'medium'
    else:
        return 'large'

#define rating for headcut health per sez
def rate_headcut(row):
    # Check if the SEZ has at least one large headcut
    if row['large'] >= 1:
        return 'D'  # Assign score D
    elif row['medium'] >= 1:
        return 'C'  # Assign score C
    elif row['small'] >= 1:
       return 'B'  # Assign score B
    else:
        return 'A'  # Assign score A (no headcuts)


#define rating SEZ Rating
def rate_SEZ(percent):
    if 0 <= percent < .70:
        return 'D'
    elif .7 <= percent < .80:
        return 'C'
    elif .80 <= percent < .90:
        return 'B'
    else:
        return 'A'
    
    #Define Grade for IPI Score - Used only for Stream HAbitat Condition
def categorize_phab(IPI):
     if   IPI >= 0.94:
        return 'A'
     elif 0.83 < IPI < 0.94:
        return 'B'
     elif 0.7 < IPI <= 0.83:
        return 'C'
     else:
        return 'D'
#-----------------------------#
#Create LookUp Dictionaries 
#-----------------------------#
#Large Polygons or only polygon shapes lookup dictionary for Assessment Units with lerger values of acreage

# Step 1: Read the Excel file into a DataFrame
excel_data = pd.read_csv(r"C:\Users\snewsome\Documents\GitHub\Monitoring\SEZ_scripts\Large_Polygon_Lookup.csv")  

#Define Empty look up dataframe
lookup_dict = {}

for index, row in excel_data.iterrows():
    lookup_dict[row['Assessment_Unit_Name']] = row['SEZ_ID']

# See dictionary where keys are Assessment Unit Names and values are SEZ IDs
print(lookup_dict)

#Small Polygon if there are two acres for an SEZ
# Step 1: Read the Excel file into a DataFrame
excel_data = pd.read_csv(r"C:\Users\snewsome\Documents\GitHub\Monitoring\SEZ_scripts\Small_Polygon_Lookup.csv")  

#Define Empty look up dataframe
lookup_riverine = {}

for index, row in excel_data.iterrows():
    lookup_riverine[row['Assessment_Unit_Name']] = row['SEZ_ID']

# See dictionary where keys are Assessment Unit Names and values are SEZ IDs
print(lookup_riverine)

#All Polygons
# Step 1: Read the Excel file into a DataFrame
excel_data = pd.read_csv(r"C:\Users\snewsome\Documents\GitHub\Monitoring\SEZ_scripts\All_SEZID_Lookup.csv")  

#Define Empty look up dataframe
lookup_all = {}

for index, row in excel_data.iterrows():
    lookup_all[row['SEZ_ID']] = {'SEZ_Type': row['SEZ_Type']}


