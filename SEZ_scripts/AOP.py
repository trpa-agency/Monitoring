from utils import *
#-Should I just take raw data from aquatic organism passage feature class in vector? 
#SDE. VECTOR SDE.Fisheries sde.AquaticOrganismPassage_USFS

# Future Us
# Get data from USFS table so that when it is udated it is incorporated Need to engineer this
def get_USFSAOP_data():
    # make sql database connection with pyodbc
    engine = get_conn('sde')
    # get BMP Status data as dataframe from BMP SQL Database
    with engine.begin() as conn:
        # create dataframe from sql query
        df  = pd.read_sql('SELECT Total_Mile, Number_Bar, Structures FROM sde.SDE.AquaticOrganismPassage_USFS_evw', conn)
    return df

# get sez table data

def get_aopsez_data_sql():
    #Connects to the SQL database, retrieves SEZ data, and returns it as a DataFrame.
    # Connect to the database using the custom connection function
    engine = get_conn('sde')
    
    # Define the SQL query
    query = """
    SELECT 
        AquaticOrganismPassage_Barriers, 
        AquaticOrganismPassage_DataSour, 
        AquaticOrganismPassage_NumberOf, 
        AquaticOrganismPassage_Rating, 
        AquaticOrganismPassage_Score, 
        AquaticOrganismPassage_StreamMi,
        SEZ_ID,
        Threshold_Year,
        Assessment_Unit_Name
    FROM 
        sde.SDE.SEZ_Assessment_Unit_evw
      WHERE 
        Threshold_Year = (SELECT MAX(Threshold_Year) FROM sde.SDE.SEZ_Assessment_Unit_evw)
    """
    
    try:
        # Execute query and load into DataFrame
        with engine.begin() as conn:
            AOP_df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error querying database: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
    
    return AOP_df    

def Process_aop(AOP_df): 
       # Field Mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'Threshold_Year': 'Year',
        'AquaticOrganismPassage_Barriers': 'AOP_BarriersPerMile',
        'AquaticOrganismPassage_DataSour': 'AOP_DataSour',                    
        'AquaticOrganismPassage_NumberOf': 'AOP_NumberofBarriers',
        'AquaticOrganismPassage_Score': 'AOP_Score',
        'SEZ_ID': 'SEZ_ID',
        'AquaticOrganismPassage_StreamMi': 'AOP_StreamMiles',
        'AquaticOrganismPassage_Rating': 'AOP_Rating'
    }

    # Rename fields based on field mappings
    readydf = AOP_df.rename(columns=field_mapping)

    return readydf


def post_AOP_data(readydf, draft= False):
    if draft == True:
        readydf.to_csv(r"C:\Users\snewsome\Documents\SEZ\processedaopdata.csv", index=False)
        # or post to SEZ.gdb?? staging table?
        # Convert DataFrame to a list of dictionaries
        #staging_table= stage_aquaticgdb
        #data = readydf.to_dict(orient='records')

        # Append data to staging table directly
        #field_names = list(readydf.columns)
        #with arcpy.da.InsertCursor(staging_table, field_names) as cursor:
         #   for row in data:
          #      cursor.insertRow([row[field] for field in field_names])

        #print(f"Draft data appended to {staging_table} successfully.")

   # elif draft == False:
    
    #    data = readydf.to_dict(orient='records')

        # Get the field names from the field mapping
     #   field_names = list(readydf.columns)

    # Append data to existing table uncomment when ready
      #  with arcpy.da.InsertCursor(stage_aquatic, field_names) as cursor:
       #     for row in data:
        #        cursor.insertRow([row[field] for field in field_names])


# #Grab old data from GDB 2020 Threshold --in case we need to rebuild it
def get_AOP_data_gdb():    
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
        table_path = os.path.join(master_path, table_name)
        SEZ_Master = table_to_dataframe(table_path)

        # Remove leading and trailing whitespace from column names
        SEZ_Master.columns = SEZ_Master.columns.str.strip()

        # Columns to select from the table
        selected_columns = [
            'AquaticOrganismPassage_Barriers',
            'AquaticOrganismPassage_DataSour',                    
            'AquaticOrganismPassage_NumberOf',
            'AquaticOrganismPassage_Rating',
            'SEZ_ID',
            'Assessment_Unit_Name',
            'AquaticOrganismPassage_Score',
            'AquaticOrganismPassage_StreamMi',
            'Year'
        ]

        # Create a new DataFrame with selected columns
        AOP_df = SEZ_Master[selected_columns].copy()
    
        # Add Year column
        AOP_df['Year'] = 2020

        # Field Mapping
        field_mapping = {
            'Assessment_Unit_Name': 'Assessment_Unit_Name',
            'Year': 'Year',
            'AquaticOrganismPassage_Barriers': 'AOP_BarriersPerMile',
            'AquaticOrganismPassage_DataSour': 'AOP_DataSource',                    
            'AquaticOrganismPassage_NumberOf': 'AOP_NumberofBarriers',
            'AquaticOrganismPassage_Score': 'AOP_Score',
            'SEZ_ID': 'SEZ_ID',
            'AquaticOrganismPassage_StreamMi': 'AOP_StreamMiles',
            'AquaticOrganismPassage_Rating': 'AOP_Rating'
        }

        # Rename fields based on field mappings
        readydf = AOP_df.rename(columns=field_mapping)

        return readydf

    except Exception as e:
        print(f"An error occurred: {e}")
        return None