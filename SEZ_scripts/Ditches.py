#Get old data from staging table instead of sql table. 
from utils import *
#Get most recent data from Staging Table
def get_stageditch_data_sql():
    """
    Connects to the SQL database, retrieves SEZ data, and returns it as a DataFrame.
    """
    # Connect to the database using the custom connection function
    engine = get_conn('sde')
    
    # Define the SQL query
    query = """
    SELECT 
        Year,
        Ditches_Data_Source,
        Ditches_Length,                    
        Ditches_Meadow_Length,
        Ditches_Percent,
        Ditches_Rating,
        Ditches_Score,
        SEZ_ID,
        Assessment_Unit_Name
    FROM 
        sde.SDE.stage_ditches_evw
    """
    
    # Execute the query and load the result into a DataFrame
    with engine.begin() as conn:
        Ditch_df = pd.read_sql(query, conn)
    
    return Ditch_df

#Get Data from SEZ ASsessment Unit Table
def get_sez_data_sql():
    """
    Connects to the SQL database, retrieves SEZ data, and returns it as a DataFrame.
    """
    # Connect to the database using the custom connection function
    engine = get_conn('sde')
    
    # Define the SQL query
    query = """
    SELECT 
        Ditches_Data_Source,
        Ditches_Length,
        Ditches_Meadow_Length,
        Ditches_Percent,
        Ditches_Rating,
        Ditches_Score,
        SEZ_ID, 
        Assessment_Unit_Name
    FROM 
        sde.SDE.SEZ_Assessment_Unit_evw
    """
    
    # Execute the query and load the result into a DataFrame
    with engine.begin() as conn:
        Ditch_df = pd.read_sql(query, conn)
    
    return Ditch_df

def get_ditches_datagdb():
    # Path to the geodatabase
    master_path = r"F:\GIS\PROJECTS\ResearchAnalysis\SEZ\Data\SEZ_Data.gdb"

    # Table name in the geodatabase
    table_name = "AssessmentUnits_Master"

    # Function to convert geodatabase table to DataFrame
    def table_to_dataframe(table):
        fields = [field.name for field in arcpy.ListFields(table)]
        data = [row for row in arcpy.da.SearchCursor(table, fields)]
        return pd.DataFrame(data, columns=fields)

    try:
        # Load the data from the geodatabase table into a DataFrame
        # Load the data from the geodatabase table into a DataFrame
        table_path = os.path.join(master_path, table_name)
        SEZ_Master = table_to_dataframe(os.path.join(table_path))

        # Remove leading and trailing whitespace from column names
        SEZ_Master.columns = SEZ_Master.columns.str.strip()

        # Columns to select from the table
        selected_columns = ['Ditches_Data_Source',
                        'Ditches_Length',
                        'Ditches_Meadow_Length',
                        'Ditches_Percent',
                        'Ditches_Rating',
                        'Ditches_Score',
                        'SEZ_ID', 
                        'Assessment_Unit_Name']

        # Create a new DataFrame with selected columns
        df = SEZ_Master[selected_columns].copy()
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
def process_ditches(df):
    #Prep Data
    df['Year'] = 2020

    #Field Mapping
    field_mapping = {
                'Assessment_Unit_Name': 'Assessment_Unit_Name',
                'Year': 'Year',
                'Ditches_Data_Source': 'Ditches_Data_Source',
                'Ditches_Length': 'Ditches_Length',                    
                'Ditches_Meadow_Length': 'Ditches_Meadow_Length',
                'Ditches_Percent': 'Ditches_Percent',
                'SEZ_ID': 'SEZ_ID',
                'Ditches_Rating': 'Ditches_Rating',
                'Ditches_Score': 'Ditches_Score'
}

    # Rename fields based on field mappings
    readydf = df.rename(columns=field_mapping).drop(columns=[col for col in df.columns if col not in field_mapping])