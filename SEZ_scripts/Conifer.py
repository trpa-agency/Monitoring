from utils import *
def get_conifersez_data_sql():
    """
    Connects to the SQL database, retrieves SEZ dataused in last threshold, and returns it as a DataFrame.
    """
    # Connect to the database using the custom connection function
    engine = get_conn('sde')
    
    # Define the SQL query
    query = """
    SELECT 
        Conifer_Encroachment_Data_Sourc,
        Conifer_Encroachment_Rating,                    
        Conifer_Encroachment_Percent_En,
        Conifer_Encroachment_Score,
        SEZ_ID,
        Assessment_Unit_Name,
        ConiferEncroachment_Comments
    FROM 
        sde.SDE.SEZ_Assessment_Unit_evw
    """
    
    # Execute the query and load the result into a DataFrame
    with engine.begin() as conn:
        conifer_df = pd.read_sql(query, conn)
    
    return conifer_df
    
def get_oldconifer_data():
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
            'Conifer_Encroachment_Data_Sourc',
            'Conifer_Encroachment_Rating',                    
            'Conifer_Encroachment_Percent_En',
            'Conifer_Encroachment_Score',
            'SEZ_ID',
            'Assessment_Unit_Name',
            'ConiferEncroachment_Comments'
        ]

        # Create a new DataFrame with selected columns
        conifer_df = SEZ_Master[selected_columns].copy()
        #Prep Data
        conifer_df['Year'] = 2020

        #Field Mapping
        field_mapping = {
                'Assessment_Unit_Name': 'Assessment_Unit_Name',
                'Year': 'Year',
                'Conifer_Encroachment_Data_Sourc': 'Conifer_Encroachment_Data_Source',
                'Conifer_Encroachment_Rating': 'Conifer_Encroachment_Rating',                    
                'Conifer_Encroachment_Percent_En': 'Conifer_Percent_Encroached',
                'Conifer_Encroachment_Score': 'Conifer_Encroachment_Score',
                'SEZ_ID': 'SEZ_ID',
                'ConiferEncroachment_Comments': 'ConiferEncroachment_Comments'
    }

        # Rename fields based on field mappings
        readydf = conifer_df.rename(columns=field_mapping).drop(columns=[col for col in conifer_df.columns if col not in field_mapping])

        return readydf
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
def process_conifer(conifer_df):
     #Prep Data
    conifer_df['Year'] = 2020
    

    #Field Mapping
    field_mapping = {
            'Assessment_Unit_Name': 'Assessment_Unit_Name',
            'Year': 'Year',
            'Conifer_Encroachment_Data_Sourc': 'Conifer_Encroachment_Data_Source',
            'Conifer_Encroachment_Rating': 'Conifer_Encroachment_Rating',                    
            'Conifer_Encroachment_Percent_En': 'Conifer_Percent_Encroached',
            'Conifer_Encroachment_Score': 'Conifer_Encroachment_Score',
            'SEZ_ID': 'SEZ_ID',
            'ConiferEncroachment_Comments': 'ConiferEncroachment_Comments'
        }

    # Rename fields based on field mappings
    readydf = conifer_df.rename(columns=field_mapping).drop(columns=[col for col in conifer_df.columns if col not in field_mapping])
    readydf = readydf[[field_mapping[key] for key in field_mapping if key in conifer_df.columns]]
    return readydf

def post_conifer_data(readydf, draft= False):
    #----------------------------------------------------------------#
    #Prep and post ending dataframe to invasives table in SEZ_Data.GDB
    #----------------------------------------------------------------#
    if draft == True:
        readydf.to_csv(r"C:\Users\snewsome\Documents\SEZ\processedconiferdata.csv", index=False)
        # or post to SEZ.gdb?? staging table?
        # Convert DataFrame to a list of dictionaries
        #staging_table= stage_conifergdb
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
        with arcpy.da.InsertCursor(stage_conifer, field_names) as cursor:
            for row in data:
                cursor.insertRow([row[field] for field in field_names])