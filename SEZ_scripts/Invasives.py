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
    Idf = pd.merge(dfsurvey, df, how='inner', left_on='GlobalID', right_on='ParentGlobalID')
    required_columns = ['Assessment_Unit_Name', 'plant_type', 'percent_cover', 'other', 'Year', 'Source']
    Idf['Year']=Idf['created_date'].dt.year
    Idf= add_and_keep_columns(Idf, required_columns)
    Idf['Source']= 'TRPA'
    
    return Idf
#This combines external and internal data from USFS and reorganizes the data so we can process it
def merge_format_invasive(Idf, usfsdf, year):
    # Join USFS data and TRPA collected Invasive Data
    df = pd.concat([Idf, usfsdf], ignore_index=True)
    
    #---------------------------#
    # Format Data
    #---------------------------#
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
    df.reset_index(drop=True, inplace=True)
    #---------------------------#
    # Prep Plant_type Data
    #---------------------------#
    ###Other Plants##
    # Make a DataFrame to capture 'other' plants in TRPA data and then add it to invasive df
    #other_plants_df = df[['Source', 'Year', 'SEZ_ID', 'Assessment_Unit_Name', 'other']].copy()

    # Remove Null values
    #other_plants_df = other_plants_df[~other_plants_df['other'].isna()]
    #other_plants_df.reset_index(drop=True, inplace=True)

    # Rename 'other' to plant_type
    #other_plants_df.rename(columns={'other': 'plant_type'}, inplace=True)

    # Concatenate other_plants_df with df JUST DO THIS MANUALLY not working 
    #invasivedf = pd.concat([invasivedf, other_plants_df], ignore_index=True)
    
    
    ##### make it so plant type is split into individual rows instead of lumped##
    # Replace various representations of null values with 'none'
    null_representations = ['<null>', '<Null>', '', 'NA', 'N/A', 'nan', 'NaN', 'None', 'NULL', None]
    df['plant_type'] = df['plant_type'].replace(null_representations, 'none')

    # Split plant types by comma and create new rows
    df['plant_type'] = df['plant_type'].str.split(pat=',')
    df = df.explode('plant_type')

    # Capitalize the first word and replace underscores with spaces
    df['plant_type'] = df['plant_type'].str.split('_').str[0].str.capitalize() + ' ' + df['plant_type'].str.split('_').str[1:].str.join(' ')

    # Remove spaces after the words and capitalize the first word while replacing underscores with spaces
    df['plant_type'] = df['plant_type'].astype(str)
    df['plant_type'] = df['plant_type'].str.strip()

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

    # Group by 'Assessment_Unit_Name' and 'Year' and combine plant types
    #Columns formatting for final table so that all invasive plant types are in one block under a column called a_plant_types
    # Group by 'Assessment_Unit_Name' and 'Year' and combine plant types
    # Ensure 'grouped_df' is grouped by 'Assessment_Unit_Name' and 'Year'
    grouped_df = df.groupby(['Assessment_Unit_Name', 'Year'])['plant_type']
    combined_plant_types = grouped_df.apply(lambda x: ', '.join(x)).reset_index(name='all_plant_types')
    # Merge combined plant types back to df
    df = pd.merge(df, combined_plant_types, on=['Assessment_Unit_Name', 'Year'], how='left')
    #Create Plant Priority look up dictionary 
    
    # Now df contains the 'all_plant_types' column with combined values
    return df

def process_grade_invasive(df):
    #------------------------------------#
    #Create Plant Priority look up dictionary 
    #--------------------------------------#
    
    # Read the csv file into a DataFrame
    csv_data = pd.read_csv(r"F:\GIS\PROJECTS\ResearchAnalysis\SEZ\Invasives Priority lookup.csv") 

    #Define Empty look up dataframe
    Invasives_lookup = {}

    key = 'Common'
    values = ['Scientific', 'Priority'] 

    Invasives_lookup= csv_data.set_index(key)[values].to_dict(orient='index')

    # Define a custom function to map plant types to priorities
    def map_priority(plant_type):
        if pd.isnull(plant_type):
            return 'None' # Return NaN for NaN values
        else:
            # Extract the priority from the dictionary, or return 'Unknown' if not found
            plant_info = Invasives_lookup.get(plant_type)
            if plant_info:
                return plant_info['Priority']
            else:
                return 'Unknown'


    # Create a new column 'Priority' based on the mapping from the dictionary
    df['Priority'] = df['plant_type'].map(map_priority)
    
    return df


def post_invasive(df, draft= True):
    #----------------------------------------------------------------#
    #Prep and post ending dataframe to invasives table in SEZ_Data.GDB
    #----------------------------------------------------------------#
    if draft == True:
        df.to_csv(r"C:\Users\snewsome\Documents\SEZ\processedinavsivedata.csv", index=False)
        # or post to SEZ.gdb?? staging table?
        # Convert DataFrame to a list of dictionaries
        #staging_table= stage_invasivesgdb
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
        with arcpy.da.InsertCursor(stage_invasives, field_names) as cursor:
            for row in data:
                cursor.insertRow([row[field] for field in field_names])



