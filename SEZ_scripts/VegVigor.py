from utils import *
#THIS JUST TAKES DATA FROM SEZ_DATA.gdb and creats a dataframe for the staging table.
#def get_vegvig_data_staged():
#def get_vegvig_data_sql():

def get_oldvegvig_data():
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
        SEZ_Master = table_to_dataframe(os.path.join(master_path, table_name))

    # Remove leading and trailing whitespace from column names
        SEZ_Master.columns = SEZ_Master.columns.str.strip()

    # Columns to select from the table
        selected_columns = ['VegetationVigor_DataSource',
                        'NDVI_ID',
                        'VegetationVigor_Raw',
                        'VegetationVigor_Rating',
                        'VegetationVigor_Score',
                        'SEZ_ID', 
                        'Assessment_Unit_Name']

    # Create a new DataFrame with selected columns
        vegetation_df = SEZ_Master[selected_columns].copy()
        #Prep Data
        vegetation_df['Year'] = 2020

        #Field Mapping
        field_mapping = {
                'Assessment_Unit_Name': 'Assessment_Unit_Name',
                'Year': 'Year',
                'VegetationVigor_DataSource': 'VegetationVigor_DataSource',
                'NDVI_ID': 'NDVI_ID',                    
                'VegetationVigor_Raw': 'VegetationVigor_Raw',
                'VegetationVigor_Rating': 'VegetationVigor_Rating',
                'SEZ_ID': 'SEZ_ID',
                'VegetationVigor_Score': 'VegetationVigor_Score'
        }

    # Rename fields based on field mappings
        readydf = vegetation_df.rename(columns=field_mapping).drop(columns=[col for col in vegetation_df.columns if col not in field_mapping])

        return readydf

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def post_Veg_data(readydf, draft= False):
    if draft == True:
        readydf.to_csv(r"C:\Users\snewsome\Documents\SEZ\processedvegvigdata.csv", index=False)
        # or post to SEZ.gdb?? staging table?
        # Convert DataFrame to a list of dictionaries
        #staging_table= stage_vegetationgdb
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
        with arcpy.da.InsertCursor(stage_vegetation, field_names) as cursor:
            for row in data:
                cursor.insertRow([row[field] for field in field_names])