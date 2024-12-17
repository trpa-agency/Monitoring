from SEZ_scripts.utils import *
def get_USFSinvasive_data():

    #External Data import and spatial join to our SEZ Units
    # Define the USFS REST endpoint
    usfsrest = "https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_InvasiveSpecies_01/MapServer/0"
    where    = "FS_UNIT_ID = '0519'"
    # Query the feature layer
    sdfUSFS = get_fs_data_spatial_query(usfsrest, where)
    #SEZ Data Import
    SEZ_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/0"
    dfSEZ = get_fs_data_spatial(SEZ_url)
    # Create the spatially enabled DataFrame (sdf) for target feature SEZ assessment units
    #spatial reference stuff
    sdfUSFS.spatial.sr = dfSEZ.spatial.sr

    #CAsdf.spatial.set_spatial_reference(SEZsdf.spatial.sr)
    #perform spatial join
    usfsdata = dfSEZ.spatial.join(sdfUSFS, how="inner")
    usfsdf = usfsdata[usfsfields]

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
    usfsfields = ['Assessment_Unit_Name', 'COMMON_NAME', 'SCIENTIFIC_NAME']
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

    invasive23df['invasives_percent_cover'].fillna(0, inplace=True)
    invasive23df['invasives_plant_type'].fillna('None', inplace=True)
    usfsdf = usfsdata[usfsfields]
    #usfsdf = usfsdata.drop(columns='SHAPE')

    #print(usfsdf)
    #usfs23df = feature_class_to_dataframe(usfsdata, usfsfields)
    # Rename fields for consistency
    invasive19df.rename(columns={'SITE_ID': 'Assessment_Unit_Name', 'INVASIVE_PLANT': 'plant_type', 'PERCENT_COVER': 'percent_cover', 'SURVEY_DATE': 'created_date'}, inplace=True)
    invasive20df.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'Invasives_Plant_Type': 'plant_type', 'Other': 'other', 'Invasives_Percent_Cover': 'percent_cover', 'Survey_Date': 'created_date'}, inplace=True)
    invasive22df.rename(columns={'Assessment_Unit': 'Assessment_Unit_Name', 'Invasives_Plant_Type': 'plant_type', 'InvasiveType_Other': 'other', 'Invasives_Percent_Cover': 'percent_cover', 'Survey_Date': 'created_date'}, inplace=True)
    invasive23df.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'invasives_percent_cover': 'percent_cover', 'invasives_plant_type': 'plant_type', 'invasive_type_other': 'other', 'created_date': 'created_date'}, inplace=True)
    usfsdf.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'COMMON_NAME': 'plant_type'}, inplace=True)

    required_columns = ['Assessment_Unit_Name', 'plant_type', 'percent_cover', 'other', 'created_date', 'Source']


    # Add missing columns to each dataframe
    invasive19df = add_and_keep_columns(invasive19df, required_columns)
    invasive20df = add_and_keep_columns(invasive20df, required_columns)
    invasive22df = add_and_keep_columns(invasive22df, required_columns)
    invasive23df = add_and_keep_columns(invasive23df, required_columns)
    usfsdf = add_and_keep_columns(usfsdf, required_columns)


    #Remove null plant types for usfs data
    usfsdf = usfsdf[~usfsdf['plant_type'].isna()]
    # Remove records where plant_type is 'Eurasian watermilfoil'
    usfsdf = usfsdf[usfsdf['plant_type'] != 'Eurasian watermilfoil']


    #Add Source
    invasive19df['Source'] = 'TRPA'
    invasive20df['Source'] = 'TRPA'
    invasive22df['Source'] = 'TRPA'
    invasive23df['Source'] = 'TRPA'
    usfsdf['Source'] = 'USFS'

    # Concatenate DataFrames
    df = pd.concat([usfsdf, invasive19df, invasive20df, invasive22df, invasive23df], ignore_index=True)

    return df

def clean_process_invasive(df, draft=False):
    #---------------------------#
    #Prep Plant_type Data
    #---------------------------#
    # make sure the invasive priority list is updated. 
    #It lives here:"F:\GIS\PROJECTS\ResearchAnalysis\SEZ\Invasives Priority lookup.csv"

    #location of Invasives Priority Lookup
    file_path = "F:\GIS\PROJECTS\ResearchAnalysis\SEZ\Invasives Priority lookup.csv"

    # Create the lookup dictionary and mapping function
    lookup_dict, priority_mapper = create_invasive_dictionary(file_path)
    
    #Define SEZ ID based on Assessment_Unit_Name for QA on SEZ Name 
    df['SEZ_ID'] = df['Assessment_Unit_Name'].map(lookup_dict)

    # Fill NaN values with a specific value, such as 0
    df['SEZ_ID'] = df['SEZ_ID'].fillna(0)

    #remove meadow from 2019 test period that are not actually meadows- 
    # first run code without this  in case new data has wrongly spelled assessmne tunit name
    df = df[df['SEZ_ID'] != 0]

   
    # Set 'Year' column based on data source
    df['Year'] = df['created_date'].dt.year


    df.loc[df['Source'] == 'USFS', 'Year'] = '2023'
    df.loc[df['Source'] == 'TRPA', 'Year'] = df['created_date'].dt.year

    #invasivedf.reset_index(drop=True, inplace=True)
    df.reset_index(drop=True, inplace=True)
    #Make a dataframe to capture 'other' plants in trpa data and then add it to invasive df
    other_plants_df = df[['Source', 'Year', 'SEZ_ID', 'Assessment_Unit_Name', 'other']].copy()

    #Get rid of Null values
    other_plants_df = other_plants_df[~other_plants_df['other'].isna()]

    #invasivedf.reset_index(drop=True, inplace=True)
    other_plants_df.reset_index(drop=True, inplace=True)


    #Rename 'other to plant_type
    other_plants_df.rename(columns={'other': 'plant_type'}, inplace=True)

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


    # Remove any spaces at the beginning or end of a word
    df['plant_type'] = df['plant_type'].str.strip()


    #---------------------#
    #These replacements are so the same weed with different common name doesn't get counted twice toward score MAY NEED TO ADD MORE DESCREPENCIES in coming years if more pop up or find more elopquent solution
    #---------------------#
    # Replace 'Common mullein' with 'Wooly mullein' in the 'plant_type' column 
    df['plant_type'] = df['plant_type'].replace('Common mullein', 'Wooly mullein')
    # Replace 'Nodding plumeless thistle' with 'Musk thistle' in the 'plant_type' column
    df['plant_type'] = df['plant_type'].replace('Nodding plumeless thistle', 'Musk thistle')
    # Replace 'Nodding plumeless thistle' with 'Musk thistle' in the 'plant_type' column
    df['plant_type'] = df['plant_type'].replace('Field bindweed', 'Common bindweed')
    # Replace 'Nodding plumeless thistle' with 'Musk thistle' in the 'plant_type' column
    df['plant_type'] = df['plant_type'].replace('Common st. johnswort', 'Klamathweed')
    # Replace 'Broadleaf and leaved' with 'Perennial' in the 'plant_type' column
    # Define the replacements dictionary
    replacements = {'Broadleaf Pepperweed': 'Perennial pepperweed', 'Broadleaved pepperweed': 'Perennial pepperweed'}

    # Replace values in the 'plant_type' column using the dictionary
    df['plant_type'] = df['plant_type'].replace(replacements)

    #Replace Sulphur Cinquefoil with sulfur
    df['plant_type'] = df['plant_type'].replace('Sulphur cinquefoil', 'Sulfur cinquefoil')
    df['plant_type'] = df['plant_type'].replace('Sweetclover', 'White sweetclover')
    df['plant_type'] = df['plant_type'].replace('Reed canary grass', 'Reed canarygrass')
    #Replace
    df['plant_type'] = df['plant_type'].replace('Salt cedar', 'Tamarisk')
    df['plant_type'] = df['plant_type'].replace('Butter and eggs', 'Yellow toadflax')
    df['plant_type'] = df['plant_type'].replace('Canada cottonthistle', 'Canada thistle')
    # Replace empty strings or other placeholders with NaN
    #invasivedf['plant_type'] = invasivedf['plant_type'].replace('', np.nan)

    # Drop duplicates based on 'Assessment_Unit_Name' and 'Year' in the remaining DataFrame
    df = df.drop_duplicates(subset=['Assessment_Unit_Name', 'Year', 'plant_type'], keep='first')


    grouped_df = df.groupby(['Assessment_Unit_Name', 'Year'])['plant_type']

    # Aggregate the plant types into one column separated by commas
    combined_plant_types = grouped_df.apply(lambda x: ', '.join(x)).reset_index(name='all_plant_types')

    # Apply the rating function to the summary DataFrame
    invasive_summary_priority['Invasives_Rating'] = invasive_summary_priority[[1, 2, 3, 4]].apply(rate_invasive, axis=1)

    #Calculate the score for the sez
    invasive_summary_priority['Invasives_Score']= invasive_summary_priority['Invasives_Rating'].apply(score_indicator) 

    #Calculate total number of invasives per sez per year
    invasive_summary_priority['Number_of_Invasives']= invasive_summary_priority[[1, 2, 3, 4]].sum(axis=1)

    #Define SEZ ID based on Assessment_Unit_Name for QA on SEZ Name 
    invasive_summary_priority['SEZ_ID'] = invasive_summary_priority['Assessment_Unit_Name'].map(lookup_dict)

    invasive_summary_priority['all_plants']= combined_plant_types['all_plant_types']

def post_invasive(table):
    #----------------------------------------------------------------#
    #Prep and post ending dataframe to invasives table in SEZ_Data.GDB
    #----------------------------------------------------------------#
    #Field Mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'Year': 'Year',
        'Source': 'Invasives_Data_Source',
        'Number_of_Invasives': 'Invasives_Number_of_Invasives',
        'Invasives_Rating': 'Invasives_Rating',
        'Invasives_Score': 'Invasives_Scores',
        'SEZ_ID': 'SEZ_ID',
        'percent_cover': 'Invasives_Percent_Cover',
        'all_plants': 'Invasives_Plant_Types',
    }

    # Rename fields based on field mappings
    readyinvasivedf = invasive_summary_priority.rename(columns=field_mapping).drop(columns=[col for col in invasive_summary_priority.columns if col not in field_mapping])

    readyinvasivedf['SEZ_ID'] = readyinvasivedf['Assessment_Unit_Name'].map(lookup_dict)

    # Convert DataFrame to a list of dictionaries
    data = readyinvasivedf.to_dict(orient='records')

    # Get the field names from the field mapping
    field_names = list(readyinvasivedf.columns)

    # Append data to existing table uncomment when ready
    with arcpy.da.InsertCursor(table, field_names) as cursor:
        for row in data:
            cursor.insertRow([row[field] for field in field_names])



