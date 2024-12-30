from utils import *
#Options to clean up
# #Get data from Staging Tables
#Get data that is not new from SEZ Assessment Unit Table sql that is not new and reuse old data
#May not even need this .py
def get_sez_data_sql():
    """
    Connects to the SQL database, retrieves SEZ data, and returns it as a DataFrame.
    """
    # Connect to the database using the custom connection function
    engine = get_conn('sde')
    
    # Define the SQL query
    query = """
    SELECT 
        #Year,
        Habitat_Frag_Data_Source,
        Habitat_Frag_Percent_Impervious',                    
        Habitat_Frag_Score',
        Habitat_Frag_Impervious_Acres',
        Habitat_Frag_Rating,
        SEZ_ID,
        Assessment_Unit_Name
    FROM 
        sde.SDE.SEZ_Assessment_Unit_evw
    """
    
    # Execute the query and load the result into a DataFrame
    with engine.begin() as conn:
        df = pd.read_sql(query, conn)
    
    return df

#Used for the 2023 Threshold Evaluation
def get_habitat_data_gdb():
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
        #    Load the data from the geodatabase table into a DataFrame
        SEZ_Master = table_to_dataframe(os.path.join(master_path, table_name))

        # Remove leading and trailing whitespace from column names
        SEZ_Master.columns = SEZ_Master.columns.str.strip()

        # Columns to select from the table
        selected_columns = ['Habitat_Fragmentation_Data_Sour',
                        'Habitat_Fragmentation_Imperviou',
                        'Habitat_Fragmentation_Percent_I',
                        'Habitat_Fragmentation_Rating',
                        'Habitat_Fragmentation_Score',
                        'SEZ_ID', 
                        'Assessment_Unit_Name']

        # Create a new DataFrame with selected columns
        HabFrag_df = SEZ_Master[selected_columns].copy()
        HabFrag_df['Year']= 2020
        #Field Mapping
        field_mapping = {
                'Assessment_Unit_Name': 'Assessment_Unit_Name',
                'Year': 'Year',
                'Habitat_Fragmentation_Data_Sour': 'Habitat_Frag_Data_Source',
                'Habitat_Fragmentation_Percent_I': 'HAbitat_Frag_Percent_Impervious',                    
                'Habitat_Fragmentation_Score': 'Habitat_Frag_Score',
                'Habitat_Fragmentation_Imperviou': 'Habitat_Frag_Impervious_Acres',
                'SEZ_ID': 'SEZ_ID',
                'Habitat_Fragmentation_Rating': 'Habitat_Frag_Rating'
        }

        # Rename fields based on field mappings
        readydf = HabFrag_df.rename(columns=field_mapping).drop(columns=[col for col in HabFrag_df.columns if col not in field_mapping])
        return readydf

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def post_AOP_data(readydf, draft= False):
    if draft == True:
        readydf.to_csv(r"C:\Users\snewsome\Documents\SEZ\processedHabFragdata.csv", index=False)
        # or post to SEZ.gdb?? staging table?
        # Convert DataFrame to a list of dictionaries
        #staging_table= stage_habitatgdb
        #data = readydf.to_dict(orient='records')

        # Append data to staging table directly
        #field_names = list(readydf.columns)
        #with arcpy.da.InsertCursor(staging_table, field_names) as cursor:
         #   for row in data:
          #      cursor.insertRow([row[field] for field in field_names])

        #print(f"Draft data appended to {staging_table} successfully.")

    elif draft == False:
    
        # Convert DataFrame to a list of dictionaries
        data = readydf.to_dict(orient='records')

        # Get the field names from the field mapping
        field_names = list(readydf.columns)

        # Append data to existing table uncomment when ready
        with arcpy.da.InsertCursor(stage_habitat, field_names) as cursor:
            for row in data:
                cursor.insertRow([row[field] for field in field_names])