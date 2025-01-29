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
    csv_data = pd.read_csv(r"F:\\GIS\\PROJECTS\\ResearchAnalysis\\SEZ\\Invasives Priority lookup.csv", skipinitialspace=True)
    csv_data.columns=csv_data.columns.str.strip()
    Invasives_lookup = {}
    # Create the lookup dictionary directly
    Invasives_lookup = csv_data.set_index('Common')[['Scientific', 'Priority']].to_dict(orient='index')
    
    #Assign Priorities to Cleaned Data
    # Map priorities directly with a lambda function
    df['Priority'] = df['plant_type'].map(lambda x: Invasives_lookup.get(x, {}).get('Priority', 'None'))
    # Debug print to check the priorities
    print("Priorities:", df['Priority'].unique())
    
    #Move to final FORMATTING??
    # Create a new column with the plant type and priority
    df['Plant_Type_With_Priority'] = df['plant_type'] + ' (Level ' + df['Priority'].astype(str) + ')'
    # If plant_type is none i want to assign Plant_Type_With_priority name as just None
    df.loc[df['plant_type'] == 'None', 'Plant_Type_With_Priority'] = 'None'
    # Reset Index?
    df.reset_index(drop=True, inplace=True)
    
    return df

def process_grade_invasives(df):
    # Convert 'Priority' column to categorical type for efficient memory usage and better performance
    #df['Priority'] = df['Priority'].astype('category')
   
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
    
    # priority_columns= ['1','2','3','4']
    #  # Ensure that the priority columns exist in the DataFrame
    # for col in priority_columns:
    #     if col not in invasive_priority_summary.columns:
    #         invasive_priority_summary[col] = 0
    # # Debug print to check the priority columns
    # #print("Invasive Priority Summary (After Ensuring Priority Columns):")
    # #print(invasive_priority_summary[priority_columns].head())
    # # Drop the 'None' column if it exists
    # if 'None' in invasive_priority_summary.columns:
    #     invasive_priority_summary.drop(columns=['None'], inplace=True)
    # #Rate and Grade'None')
    # #invasive_priority_summary[['1', '2', '3', '4']] = invasive_priority_summary[['1', '2', '3', '4']].astype(int)
    # #Rate and Grade
    # invasive_priority_summary['Invasives_Rating'] =  invasive_priority_summary[['1', '2', '3', '4']].apply(rate_invasive, axis=1)
    # #Calculate the total number of invasive plants per Assessment unit
    # invasive_priority_summary['Number_of_Invasives'] = invasive_priority_summary[['1', '2', '3', '4']].sum(axis=1)
    # # Calculate the score for the SEZ
    # invasive_priority_summary['Invasives_Score'] = invasive_priority_summary['Invasives_Rating'].apply(score_indicator)
    
    return invasive_priority_summary, invasive_summary

#----------------------#
    # #Joining plant types
    # # Group by 'Assessment_Unit_Name' and 'Year' and combine plant types and data sources
    # grouped_df = df.groupby(['Assessment_Unit_Name', 'Year']).agg({
    #     'plant_type': lambda x: ', '.join(x),         # Combine plant types
    #     'Source': lambda x: ', '.join(sorted(set(x)))       # Combine data sources
    # }).reset_index()

    # # Rename columns for clarity
    # grouped_df.rename(columns={
    #     'plant_type': 'all_plant_types',
    #     'Source': 'Data_Sources'
    # }, inplace=True)

    # # Merge combined columns back to the original DataFrame, keeping only the Data_Sources column from grouped_df
    # df = pd.merge(df.drop(columns=['Source']), grouped_df, on=['Assessment_Unit_Name', 'Year'], how='left')
    # df.rename(columns={'Data_Sources_y': 'Data_Sources'}, inplace=True)
    

   
   # Summarize invasive plants by grouping and aggregating- creates a count column for each priority level per assessment unit per year
    #df['Count'] = df.groupby(['Assessment_Unit_Name', 'Year', 'Priority'], dropna=False).size()
        #.reset_index(name='Count')
    # # Field Mapping
    # field_mapping = {
    #     'Assessment_Unit_Name': 'Assessment_Unit_Name',
    #     'Year': 'Year',
    #     'Data_Sources_y': 'Invasives_Data_Source',
    #     'Number_of_Invasives': 'Invasives_Number_of_Invasives',
    #     'Invasives_Rating': 'Invasives_Rating',
    #     'Invasives_Score': 'Invasives_Scores',
    #     'SEZ_ID': 'SEZ_ID',
    #     'percent_cover': 'Invasives_Percent_Cover',
    #     'all_plants': 'Invasives_Plant_Types',
    # }

    # # Rename fields based on field mappings
    # readydf = invasive_priority_summary.rename(columns=field_mapping).drop(columns=[col for col in invasive_priority_summary.columns if col not in field_mapping])

    # # Final SEZ ID check
    # readydf['SEZ_ID'] = readydf['Assessment_Unit_Name'].map(lookup_dict)
    #return invasive_priority_summary, invasive_summary#, readydf 