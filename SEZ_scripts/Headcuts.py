from utils import *

def get_allheadcut_data():
    #sez_stream_headcuts doesn't capture the sez_survey data when there is 0!
    # Paths to the feature classes in GIS/GIS/DATA/Monitoring
    headcut19fc = os.path.join(headcut19gdb, "Stream_Headcut_2019")
    headcut20fc = os.path.join(headcut20gdb, "Stream_Headcut_2020")
    headcut22fc = os.path.join(headcut22gdb, "Stream_Headcut_2022")
    headcut23fc = os.path.join(headcut23gdb, "sez_stream_headcut")
    sez_surveyfc = os.path.join(sez_surveygdb, "sez_survey")

    headcut23fields = ['ParentGlobalID', 'headcut_depth']
    headcut22fields = ['Assessment_Unit', 'Headcut_Depth', 'synced_date']
    headcut20fields = ['Assessment_Unit_Name', 'Headcut_Depth','Survey_Date']
    headcut19fields = ['SITE_NAME', 'HEADCUT_DEPTH', 'SURVEY_DATE' ]
    sez_surveyfields = ['GlobalID', 'Assessment_Unit_Name', 'headcuts_number_of_headcuts', 'survey_date']

    # Read feature classes into DataFrames

    headcut19df = feature_class_to_dataframe(headcut19fc, headcut19fields)
    headcut20df = feature_class_to_dataframe(headcut20fc, headcut20fields)
    headcut22df = feature_class_to_dataframe(headcut22fc, headcut22fields)
    headcut23df = feature_class_to_dataframe(headcut23fc, headcut23fields)
    sez_surveyheadcutdf = feature_class_to_dataframe(sez_surveyfc, sez_surveyfields)

    #Join sez_survey and headcut23
    # Perform the join
    joined2023_df = headcut23df.merge(sez_surveyheadcutdf, left_on='ParentGlobalID', right_on='GlobalID', how='right')

    # Rename fields
    headcut19df.rename(columns={'SITE_NAME': 'Assessment_Unit_Name', 'HEADCUT_DEPTH': 'headcut_depth', 'SURVEY_DATE': 'created_date'}, inplace=True)
    headcut20df.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'Headcut_Depth': 'headcut_depth', 'Survey_Date': 'created_date'}, inplace=True)
    headcut22df.rename(columns={'Assessment_Unit': 'Assessment_Unit_Name', 'Headcut_Depth': 'headcut_depth', 'synced_date': 'created_date'}, inplace=True)
    joined2023_df.rename(columns={'Assessment_Unit_Name': 'Assessment_Unit_Name', 'survey_date': 'created_date', 'headcuts_number_of_headcuts': 'Count', 'headcut_depth': 'headcut_depth'}, inplace=True)
    # Concatenate DataFrames
    headcutdf = pd.concat([headcut19df, headcut20df, headcut22df, joined2023_df], ignore_index=True)
    
    return headcutdf

#Do QA in ArcGIS Pro in sde collect

#Get general survey data and headcut specific survey from sde.collect and combine
def get_combined_survey_and_headcut_data():
    # Connect to SDE Collect to grab raw data
    engine = get_conn('sde_collection')

    # Get the first dataset (sez_survey data)
    with engine.begin() as conn:
        dfsurvey = pd.read_sql('SELECT GlobalID, Assessment_Unit_Name, headcuts_number_of_headcuts, survey_date FROM sde_collection.SDE.sez_survey_evw', conn)

    # Get the second dataset (sez_stream_headcut data)
    with engine.begin() as conn:
        dfheadcut = pd.read_sql('SELECT ParentGlobalID, headcut_depth FROM sde_collection.SDE.sez_stream_headcut_evw', conn)

    # Join the two DataFrames on GlobalID and ParentGlobalID
    headcutdf = pd.merge(dfsurvey, dfheadcut, how='left', left_on='GlobalID', right_on='ParentGlobalID')
    #calculate year column 
    headcutdf['Year'] = headcutdf['survey_date'].dt.year

    return headcutdf


def process_grade_headcut(headcutdf, year):
    #----------------------------------------------#
    # Process Data
    #----------------------------------------------#
    #filter for year
    headcutdf = headcutdf[headcutdf['Year'] == year]
    # assign small, medium, large to headcut
    headcutdf['Headcut_Size']=headcutdf['headcut_depth'].apply(categorize_headcut)


    # Group by 'SEZ_ID', 'Year', and 'Headcut_Size', and count the number of occurrences for each group
    headcut_summary = headcutdf.groupby(['Assessment_Unit_Name', 'Year', 'Headcut_Size']).size().reset_index(name='Count')

    headcut_summary_sml = headcut_summary.pivot_table(index=['Assessment_Unit_Name', 'Year'], columns='Headcut_Size', values='Count', fill_value=0)
    # Ensure 'large', 'medium', and 'small' exist even if they have no data
    for col in ['large', 'medium', 'small']:
        if col not in headcut_summary_sml.columns:
            headcut_summary_sml[col] = 0  # Default value
    # Reset the index to flatten the DataFrame
    headcut_summary_sml.reset_index(inplace=True)

    #----------------------------------------------------------------#
    #Grade, Score, # of Headcuts
    #----------------------------------------------------------------#

    # Apply the rating function to the summary DataFrame
    headcut_summary_sml['Headcuts_Rating'] = headcut_summary_sml.apply(rate_headcut, axis=1)

    #Calculate total number of headcuts per sez per year
    headcut_summary_sml['Number_of_Headcuts']= headcut_summary_sml[['large', 'medium', 'small']].sum(axis=1)

    #Calculate the score for the sez
    headcut_summary_sml['Headcuts_Score']= headcut_summary_sml['Headcuts_Rating'].apply(score_indicator)

    #Add Datasource
    headcut_summary_sml['Headcuts_Data_Source'] = 'TRPA' 
    
      #Field Mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'Year': 'Year',
        'Headcuts_Data_Source': 'Headcuts_Data_Source',
        'Number_of_Headcuts': 'Number_of_Headcuts',
        'Headcuts_Rating': 'Headcuts_Rating',
        'Headcuts_Score': 'Headcuts_Score',
        'small': 'small',
        'medium': 'medium',
        'large': 'large'
    }

    # Rename fields based on field mappings
    readydf = headcut_summary_sml.rename(columns=field_mapping).drop(columns=[col for col in headcut_summary_sml.columns if col not in field_mapping])
    #post data to CSV to be appended to sez_score_headcut table in sde.Vector
    file_name = f"processedheadcutdata_{year}.csv"
    file_path = r"F:\GIS\GIS_DATA\Monitoring\Stream_Headcut\Processed_Headcut_Data"  
    #file_path = r"C:\Users\snewsome\Documents\GitHub\Monitoring\SEZ_scripts"  # Update with your GitHub repo path
    full_path = os.path.join(file_path, file_name)
    readydf.to_csv(full_path, index=False)
    print(f"Draft data written to {full_path} successfully.")
    return readydf

#post the data to a CSV or gdb for QA and manually append in ARCGIS Pro