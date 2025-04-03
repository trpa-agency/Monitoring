from utils import *
#Bring in data sent from USFS to get most recent invasives data from usfs layers

def get_USFSinvasive_data():
    #External Data import and spatial join to our SEZ Units
    # Define the USFS REST endpoint
    usfsrest = "https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_InvasiveSpecies_01/MapServer/0"
    where    = "FS_UNIT_ID = '0519'"
    # create and Query the feature layer
    sdfUSFS = get_fs_data_spatial_query(usfsrest, where)#Can also narrow down the fields here
    ##SEZ Data Import
    SEZ_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/0"
    dfSEZ = get_fs_data_spatial(SEZ_url)
    # Create the spatially enabled DataFrame (sdf) for target feature SEZ assessment units
    #spatial reference stuff
    sdfUSFS.spatial.sr = dfSEZ.spatial.sr
    #perform spatial join
    usfsdata = dfSEZ.spatial.join(sdfUSFS, how="inner")
    usfsfields = ['Assessment_Unit_Name', 'COMMON_NAME', 'SCIENTIFIC_NAME', 'DATE_COLLECTED']
    usfsdf = usfsdata[usfsfields].copy()
    usfsdf.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'COMMON_NAME': 'plant_type'}, inplace=True)
    usfsdf['Year']=usfsdf['DATE_COLLECTED'].dt.year
    required_columns = ['Assessment_Unit_Name', 'plant_type', 'percent_cover', 'other', 'Year', 'Source']
    usfsdf = add_and_keep_columns(usfsdf, required_columns)
    #Remove null plant types for usfs data
    usfsdf = usfsdf.loc[~usfsdf['plant_type'].isna()]
    # Remove records where plant_type is 'Eurasian watermilfoil'
    usfsdf = usfsdf.loc[usfsdf['plant_type'] != 'Eurasian watermilfoil']
    usfsdf.loc[:,'Source'] = 'USFS'
    return usfsdf

#Get new data directly from sde collect
def get_combined_survey_and_invasive_data():
    # Connect to SDE Collect to grab raw data
    engine = get_conn('sde_collection')

    # Get the first dataset (sez_survey data)
    with engine.begin() as conn:
        dfsurvey = pd.read_sql('SELECT GlobalID, Assessment_Unit_Name, survey_date, invasives_number_of_species, invasives_percent_cover  FROM sde_collection.SDE.sez_survey_evw', conn)

    # Get the second dataset (sez_invasive data)
    with engine.begin() as conn:
        df = pd.read_sql('SELECT ParentGlobalID, invasives_percent_cover, invasives_plant_type, invasive_type_other, created_date FROM sde_collection.SDE.sez_invasive_plant_evw', conn)
    # Rename columns in the invasive data to align with survey data
    df.rename(columns={'invasives_plant_type': 'plant_type', 'invasive_type_other': 'other'}, inplace=True)

    # Join the two DataFrames on GlobalID and ParentGlobalID
    Idf = pd.merge(dfsurvey, df, how='left', left_on='GlobalID', right_on='ParentGlobalID')
    required_columns = ['Assessment_Unit_Name', 'plant_type', 'percent_cover', 'other', 'Year', 'Source']
    Idf['Year']=Idf['survey_date'].dt.year
    Idf= add_and_keep_columns(Idf, required_columns)
    Idf['Source']= 'TRPA'
    
    return Idf

#This combines external and internal data from USFS and reorganizes the data so we can process it
def merge_format_prioritize_invasive(Idf, usfsdf, year):
    # Join USFS data and TRPA collected Invasive Data
    df = pd.concat([Idf, usfsdf], ignore_index=True)
    
    #---------------------------#
    # Format Data
    #---------------------------#
    #Do I need to to assign SEZ ID if we aren't going to grab 2019 data
    # Define SEZ ID based on Assessment_Unit_Name for QA on SEZ Name
    df.loc[:, 'SEZ_ID'] = df['Assessment_Unit_Name'].map(lookup_dict)
    df.loc[df['SEZ_ID'].isna(), 'SEZ_ID'] = 0  # Fill NaN values with 0
    
    # Remove meadow from 2019 test period that are not actually meadows
    df = df[df['SEZ_ID'] != 0]

    # Set 'Year' column 
    df = df.loc[df['Year'] == year].copy()
    #df['Year']= year

    df.loc[df['Source'] == 'USFS', 'Year'] 
    df.loc[df['Source'] == 'TRPA', 'Year'] 

    # Reset index
    #df.reset_index(drop=True, inplace=True)
    
    ##### make it so plant type is split into individual rows instead of lumped##
    # Replace various representations of null values with 'none'
    null_representations = ['<null>', '<Null>', '', 'NA', 'N/A', 'nan', 'NaN', 'None', 'NULL', None]
    df['plant_type'] = df['plant_type'].replace(null_representations, 'none')

    # Split plant types by comma and create new rows
    df['plant_type'] = df['plant_type'].str.split(pat=',')
    df = df.explode('plant_type')

    # Capitalize the first word and make the second word lower case, replace underscores with spaces
    df['plant_type'] = df['plant_type'].apply(lambda x: ' '.join([word.capitalize() if i == 0 else word.lower() for i, word in enumerate(x.split('_'))]))

    #----------------------#
    # Plant Type Replacements/ replace slang/misspellings in raw data
    #----------------------#

    # Lookup dictionary for plant type fixes
    plant_type_lookup = {
        'Common mullein': 'Wooly mullein',
        'Nodding plumeless thistle': 'Musk thistle',
        'Field bindweed': 'Common bindweed',
        'Common st. johnswort': 'Klamathweed',
        'Broadleaf Pepperweed': 'Perennial pepperweed',
        'Broadleaved pepperweed': 'Perennial pepperweed',
        'Sulphur cinquefoil': 'Sulfur cinquefoil',
        'Sweetclover': 'White sweetclover',
        'Reed canary grass': 'Reed canarygrass',
        'Salt cedar': 'Tamarisk',
        'Butter and eggs': 'Yellow toadflax',
        'Canada cottonthistle': 'Canada thistle'
    }

    # Apply replacements using .loc[] and plant_type_lookup
    for old_plant, new_plant in plant_type_lookup.items():
        df.loc[df['plant_type'] == old_plant, 'plant_type'] = new_plant

    # Drop duplicates based on 'Assessment_Unit_Name', 'Year', and 'plant_type'
    df = df.drop_duplicates(subset=['Assessment_Unit_Name', 'Year', 'plant_type'], keep='first')
    
      #------------------------------------#
    #Assign Priorities to Plant Types
    #--------------------------------------#
    # #Create Plant Priority look up dictionary
    
    #  Read and process the CSV into a lookup dictionary
    csv_data = pd.read_csv(r"C:\Users\snewsome\Documents\GitHub\Monitoring\SEZ_scripts\Invasives Priority Lookup.csv", skipinitialspace=True)
    csv_data.columns=csv_data.columns.str.strip()
    Invasives_lookup = {}
    # Create the lookup dictionary directly
    Invasives_lookup = csv_data.set_index('Common')[['Scientific', 'Priority']].to_dict(orient='index')
    
    #Assign Priorities to Cleaned Data
    # Only map priority for rows where plant_type is NOT 'None'
    df.loc[df['plant_type'] != 'None', 'Priority'] = df['plant_type'].map(lambda x: Invasives_lookup.get(x, {}).get('Priority', 'None'))

    # # Map priorities directly with a lambda function
    # df['Priority'] = df['plant_type'].map(lambda x: Invasives_lookup.get(x, {}).get('Priority', 'None'))
    # Debug print to check the priorities
    print("Priorities:", df['Priority'].unique())
    #Dictionary to rename priority levels
    priority_dict = {1: 'Level 1', 2: 'Level 2', 3: 'Level 3', 4: 'Level 4', None: 'None'}

    df['Priority'] = df['Priority'].replace(priority_dict)

   
    df.reset_index(drop=True, inplace=True)
    
    return df

def process_grade_invasives(df):
   # Perform the groupby operation to include priority and reset the index to create a proper DataFrame
    invasive_summary = df.groupby(['Assessment_Unit_Name', 'Year', 'Priority'], dropna=False).size().reset_index(name='Count')
    # Pivot the summary directly to create priority-level columns 1.2.3.4 with count values so all info is in one row
    invasive_priority_summary = (
        invasive_summary.pivot(
            index=['Assessment_Unit_Name', 'Year'],
            columns='Priority',
            values='Count'
        )
        .fillna(0)
        .reset_index()  # Flatten the DataFrame for simplicity
    )
    priority_list=['Level 1', 'Level 2', 'Level 3', 'Level 4']
    # Ensure all levels exist in the DataFrame
    for level in priority_list:
        if level not in invasive_priority_summary.columns:
            invasive_priority_summary[level] = 0  # or np.nan if missing values should be NaN
    #Rate and Grade
    invasive_priority_summary['Invasives_Rating'] =  invasive_priority_summary[priority_list].apply(rate_invasive, axis=1)
    # Calculate the score for the SEZ
    invasive_priority_summary['Invasives_Score'] = invasive_priority_summary['Invasives_Rating'].apply(score_indicator)
    invasive_priority_summary['Number_of_Invasives'] = invasive_priority_summary[priority_list].sum(axis=1)
    return invasive_summary, invasive_priority_summary

def final_format_invasive (df, invasive_priority_summary):
    ##FORMAT FOR FINAL TABLE
    # Create a new column with the plant type and priority combined name
    df['Plant_Type_With_Priority'] = df['plant_type'] + ' (' + df['Priority'].astype(str) + ')'
    # If plant_type is none or blank, assign Plant_Type_With_Priority name as just None
    df.loc[df['plant_type'].isin(['None', '', None, np.nan]), 'Plant_Type_With_Priority'] = 'None'
    
    #df_filtered = df[df['Priority'] != 'None']

    #Joining plant types
    # Group by 'Assessment_Unit_Name' and 'Year' and combine plant types and data sources
    grouped_df = df.groupby(['Assessment_Unit_Name', 'Year']).agg({
        'Plant_Type_With_Priority': lambda x: ', '.join(x),         # Combine plant types
        'Source': lambda x: ', '.join(sorted(set(x)))       # Combine data sources
    }).reset_index()
    
    # Rename columns for clarity
    grouped_df.rename(columns={
        'Plant_Type_With_Priority': 'all_plant_types',
        'Source': 'Data_Sources'
    }, inplace=True)
   
    
    grouped_df['Number_of_Invasives'] = invasive_priority_summary['Number_of_Invasives']
    grouped_df['Invasives_Rating'] = invasive_priority_summary['Invasives_Rating']
    grouped_df['Invasives_Score'] = invasive_priority_summary['Invasives_Score']

    #Assign SEZ ID for large polygons only QA for SEZ Names
    #grouped_df['SEZ_ID'] = grouped_df['Assessment_Unit_Name'].map(lookup_dict)
    #grouped_df['SEZ_ID'] = grouped_df['SEZ_ID'].fillna(0)
  
    #Percent cover add to final 
    grouped_df['percent_cover'] = np.nan
    # Field Mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'Year': 'Year',
        'Data_Sources': 'Invasives_Data_Source',
        'Number_of_Invasives': 'Invasives_Number_of_Invasives',
        'Invasives_Rating': 'Invasives_Rating',
        'Invasives_Score': 'Invasives_Scores',
        #'SEZ_ID': 'SEZ_ID',
        'percent_cover': 'Invasives_Percent_Cover',
        'all_plant_types': 'Invasives_Plant_Types',
    }

    # Rename fields based on field mappings
    readydf = grouped_df.rename(columns=field_mapping).drop(columns=[col for col in grouped_df.columns if col not in field_mapping])


    #Create a CSV file for final QA and
    # then append this into the database. Vector-->sde.sez_scores_invasives
    year = df['Year'].iloc[0]  # Assuming all rows in the DataFrame have the same year
    file_name = f"processedinvasivedata_{year}.csv"
    file_path = r"C:\Users\snewsome\Documents\SEZ\2024 Data Processing"
    #file_path = r"C:\Users\snewsome\Documents\GitHub\Monitoring\SEZ_scripts"  # Update with your GitHub repo path
    full_path = os.path.join(file_path, file_name)
    readydf.to_csv(full_path, index=False)
    print(f"Draft data written to {full_path} successfully.")
    return readydf

   
# def post_invasive(readydf, draft=True):
#       #----------------------------------------------------------------#
#     #Post ending dataframe to invasives table in SEZ_Data.GDB or CSV for QA/ manual edits to 'other' designations
#     #----------------------------------------------------------------#
#     if draft == True:
        
#         # or post to SEZ.gdb?? staging table?
#         # Convert DataFrame to a list of dictionaries
#         #staging_table= stage_invasivesgdb
#         #data = readydf.to_dict(orient='records')

#         # Append data to staging table directly
#         #field_names = list(readydf.columns)
#         #with arcpy.da.InsertCursor(staging_table, field_names) as cursor:
#          #   for row in data:
#           #      cursor.insertRow([row[field] for field in field_names])

#         #print(f"Draft data appended to {staging_table} successfully.")

#     elif draft == False:
#         #field_names = list(readydf.columns)
#         # call the CSV you just QA'd function:
#         csv_file_path = r"C:\Users\snewsome\Documents\SEZ\processedinvasivedata.csv"  # Update with your actual CSV file path
#         process_and_insert_csv(csv_file_path, stage_invasives)
       
#--------------------------------------------#
#This is invasive data from the F drive. Need to use if getting data between 2019 and 2022
def get_invasive_data_gdb():
    # Paths to the feature classes 2019-2023 threshold, 
    # Future US: just pull new invasive info from SDE Collect
    invasive19fc = os.path.join(invasiveplant19gdb, "Invasive_Species_2019")
    invasive20fc = os.path.join(invasiveplant20gdb, "Invasive_Species_2020")
    invasive22fc = os.path.join(invasiveplant22gdb, "Invasive_Species_2022")
    invasive23fc = os.path.join(invasiveplant23gdb, "sez_invasive_plant")
    sez_surveyfc = os.path.join(sez_surveygdb, "sez_survey")

    #Use gdb because they have assessment unit name in them... 2022 i had to manually add in PRO
    invasive23fields = ['ParentGlobalID', 'invasives_percent_cover','invasives_plant_type', 'invasive_type_other']
    invasive22fields = ['Assessment_Unit_Name', 'Invasives_Plant_Type', 'Invasives_Percent_Cover', 'InvasiveType_Other', 'Survey_Date']
    invasive20fields = ['Assessment_Unit_Name', 'Invasives_Plant_Type', 'Invasives_Percent_Cover',  'Other', 'Survey_Date']
    invasive19fields = ['SITE_ID', 'SURVEY_DATE', 'INVASIVE_PLANT', 'PERCENT_COVER' ]
    #usfsfields = ['Assessment_Unit_Name', 'COMMON_NAME', 'SCIENTIFIC_NAME']
    sez_surveyfields = ['GlobalID', 'invasives_percent_cover', 'Assessment_Unit_Name', 'invasives_number_of_species', 'survey_date']

    # Read feature classes into DataFrames
    invasive19df = feature_class_to_dataframe(invasive19fc, invasive19fields)
    invasive20df = feature_class_to_dataframe(invasive20fc, invasive20fields)
    invasive22df = feature_class_to_dataframe(invasive22fc, invasive22fields)
    invasivemeasurements23df = feature_class_to_dataframe(invasive23fc, invasive23fields)
    sez_surveyinvasivedf = feature_class_to_dataframe(sez_surveyfc, sez_surveyfields)

    #sez_surveyinvasivedf.loc[(sez_surveyinvasivedf['invasives_number_of_species'] == 0) | (sez_surveyinvasivedf['invasives_number_of_species'].isna()), 'invasives_plant_type'] = np.nan
    sez_surveyinvasivedf['created_date']= sez_surveyinvasivedf['survey_date']

    # Perform the join
    invasive23df = invasivemeasurements23df.merge(sez_surveyinvasivedf, left_on='ParentGlobalID', right_on='GlobalID', how='right')

    invasive23df.drop('invasives_percent_cover_y', axis=1, inplace=True)

    invasive23df.rename(columns={'invasives_percent_cover_x': 'invasives_percent_cover'}, inplace=True)

    #invasive23df['invasives_percent_cover'].fillna(0, inplace=True)
    invasive23df['invasives_plant_type'].fillna('None', inplace=True)
    #usfsdf = usfsdata[usfsfields]
    #usfsdf = usfsdata.drop(columns='SHAPE')

    #print(usfsdf)
    #usfs23df = feature_class_to_dataframe(usfsdata, usfsfields)
    # Rename fields for consistency
    invasive19df.rename(columns={'SITE_ID': 'Assessment_Unit_Name', 'INVASIVE_PLANT': 'plant_type', 'PERCENT_COVER': 'percent_cover', 'SURVEY_DATE': 'created_date'}, inplace=True)
    invasive20df.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'Invasives_Plant_Type': 'plant_type', 'Other': 'other', 'Invasives_Percent_Cover': 'percent_cover', 'Survey_Date': 'created_date'}, inplace=True)
    invasive22df.rename(columns={'Assessment_Unit': 'Assessment_Unit_Name', 'Invasives_Plant_Type': 'plant_type', 'InvasiveType_Other': 'other', 'Invasives_Percent_Cover': 'percent_cover', 'Survey_Date': 'created_date'}, inplace=True)
    invasive23df.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'invasives_percent_cover': 'percent_cover', 'invasives_plant_type': 'plant_type', 'invasive_type_other': 'other', 'created_date': 'created_date'}, inplace=True)
    #usfsdf.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'COMMON_NAME': 'plant_type'}, inplace=True)

    required_columns = ['Assessment_Unit_Name', 'plant_type', 'percent_cover', 'other', 'created_date', 'Source']

    # Add missing columns to each dataframe
    invasive19df = add_and_keep_columns(invasive19df, required_columns)
    invasive20df = add_and_keep_columns(invasive20df, required_columns)
    invasive22df = add_and_keep_columns(invasive22df, required_columns)
    invasive23df = add_and_keep_columns(invasive23df, required_columns)

    # Concatenate DataFrames
    Idf = pd.concat([invasive19df, invasive20df, invasive22df, invasive23df], ignore_index=True)
    Idf['Source']= 'TRPA'
    return Idf
    