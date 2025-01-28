from utils import *
#global variable
#get path to save file
#outchart=local_path.parents{1}/ '2023/SEZ/Analysis blah blah

#Get Erosion Data from SDE Collect
def get_erosion_data():
    # make sql database connection with pyodbc
    engine = get_conn('sde_collection')
    # get  data as dataframe from SQL Database
    with engine.begin() as conn:
        # Create dataframe from SQL query that includes STLength() to calculate shape length in the query itself
        query = """
        SELECT 
            Assessment_Unit_Name, 
            SHAPE.STLength() AS Shape_Length, 
            Bank_Type, 
            Survey_Date 
        FROM sde_collection.SDE.Stream_Erosion_evw
        """
        df = pd.read_sql(query, conn)
    
    return df
        
#Clean Up Raw Erosion Data and process
def process_grade_erosion(df, year):
    # Standardize Assessment_Unit_Name and fill missing SEZ_IDs
    df['Assessment_Unit_Name'] = df['Assessment_Unit_Name'].replace({
        'Blackwood Creek - upper 2': 'Blackwood Creek - Upper 2',
        'Taylor Creek marsh - 1': 'Taylor Creek marsh'
    })
    df['SEZ_ID'] = df['Assessment_Unit_Name'].map(lookup_dict).fillna(0).astype(int)

    # Standardize Bank_Type
    df['Bank_Type'] = df['Bank_Type'].replace({
        'both_banks': 'Both Banks',
        'Both banks': 'Both Banks',
        'one_bank': 'One Bank',
        'One bank': 'One Bank',
        'no_bank': 'No Bank'
    })

    # Extract year and filter data
    df['Year'] = df['Survey_Date'].dt.year
    df = df[df['Year'] == year]

    # Add bank multiplier
    df['bank_multiplier'] = df['Bank_Type'].map({'Both Banks': 2, 'One Bank': 1, 'No Bank': 0})
    df['eroded_banks_per_row'] = df['Shape_Length'] * df['bank_multiplier']

    # Perform explicit groupby aggregation
    grouped = df.groupby(['Assessment_Unit_Name', 'Year']).agg(
        banks_assessed_per_unit=('Shape_Length', lambda x: x.sum() * 2),  # Total banks assessed
        SEZ_total_eroded=('eroded_banks_per_row', 'sum')  # Total eroded banks
    ).reset_index()

    # Calculate Bank Stability Percentage
    grouped['Bank_Stability_Percent_Unstable'] = (
        grouped['SEZ_total_eroded'] / grouped['banks_assessed_per_unit'] * 100
    )

    # Apply grading and scoring
    grouped['Bank_Stability_Rating'] = grouped['Bank_Stability_Percent_Unstable'].apply(categorize_erosion)
    grouped['Bank_Stability_Score'] = grouped['Bank_Stability_Rating'].apply(score_indicator)
    grouped['Bank_Stability_Data_Source'] = 'TRPA'

    # Map SEZ_ID back into the grouped DataFrame
    grouped['SEZ_ID'] = grouped['Assessment_Unit_Name'].map(lookup_dict).fillna(0).astype(int)

    # Final cleanup with field mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'Year': 'Year',
        'Bank_Stability_Data_Source': 'Bank_Stability_Data_Source',
        'Bank_Stability_Percent_Unstable': 'Bank_Stability_Percent_Unstable',
        'Bank_Stability_Rating': 'Bank_Stability_Rating',
        'Bank_Stability_Score': 'Bank_Stability_Score',
        'SEZ_ID': 'SEZ_ID'
    }

    readydf = grouped.rename(columns=field_mapping)
    return readydf



#DO QA Before you post data t0 table
#draft=false if you want to do QA and draft
def post_erosion(readydf, draft= False):
    if draft == True:
        readydf.to_csv(r"C:\Users\snewsome\Documents\SEZ\processederosiondata.csv", index=False)
        # or post to SEZ.gdb?? staging table?
        # Convert DataFrame to a list of dictionaries
        #staging_table= stage_bank_stabilitygdb
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
    
    # Append data to existing table
        with arcpy.da.InsertCursor(stage_bank_stability, field_names) as cursor:
            for row in data:
                cursor.insertRow([row[field] for field in field_names])
 

   