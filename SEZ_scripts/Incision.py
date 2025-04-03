from utils import *

#Get Erosion Data from SDE Collect
def get_incision_data():
    # make sql database connection with pyodbc
    engine = get_conn('sde_collection')
    # get BMP Status data as dataframe from BMP SQL Database
    with engine.begin() as conn:
        # create dataframe from sql query is incision ratio the correct value to get (maybe use raw scores instead)
        df  = pd.read_sql('SELECT Assessment_Unit_Name, incision_ratio, survey_date FROM sde_collection.SDE.sez_channel_incision_evw', conn)
    return df

def get_combined_survey_and_incision_data():
    # Connect to SDE Collect to grab raw data
    engine = get_conn('sde_collection')

    # Get the first dataset (sez_survey data)
    with engine.begin() as conn:
        dfsurvey = pd.read_sql('SELECT GlobalID, Assessment_Unit_Name, incision_ratio, survey_date FROM sde_collection.SDE.sez_survey_evw', conn)

    # Get the second dataset (sez_stream_headcut data)
    with engine.begin() as conn:
        df = pd.read_sql('SELECT ParentGlobalID, bankfull_depth, depth_top_bank, bankfull_ratio, created_date FROM sde_collection.SDE.sez_channel_incision_evw', conn)

    # Join the two DataFrames on GlobalID and ParentGlobalID
    df = pd.merge(dfsurvey, df, how='left', left_on='GlobalID', right_on='ParentGlobalID')
    #calculate year column 
    df['Year'] = df['survey_date'].dt.year
    return df

def process_grade_incision(df, year):
#----------------------------------------------------------------#
#Add correct info to dataframe
#----------------------------------------------------------------#
     #filter for year
    df = df[df['Year'] == year].copy()
    #use this until we fix the domain- did we fix it? 12/9/24
    df.loc[:,'Assessment_Unit_Name'] = df['Assessment_Unit_Name'].replace({'Blackwood Creek - upper 2': 'Blackwood Creek - Upper 2', 'Taylor Creek marsh - 1': 'Taylor Creek marsh'})
    # Create a new column 'SEZ ID'
    #This code is for the excel look up dictionary
    df.loc[:,'SEZ_ID'] = df['Assessment_Unit_Name'].map(lookup_dict)

    # Fill NaN values with a specific value, such as 0
    df.loc[:, 'SEZ_ID'] = df['SEZ_ID'].fillna(0).astype(int)

#Calcluate the Incision Ratio 
    # Step 1: Calculate the incision ratio for each measurement
    df['calculated_incision_ratio'] = df['depth_top_bank'] / df['bankfull_depth']

    # Step 2: Group by Assessment Unit and Year, and calculate the mean incision ratio
    grouped = df.groupby(['Assessment_Unit_Name', 'Year']).agg({
        'calculated_incision_ratio': 'mean'  # Calculate the mean of the incision ratios
    }).reset_index()

    # Step 3: Merge the mean incision ratio back into the original DataFrame
    df = df.merge(grouped, on=['Assessment_Unit_Name', 'Year'], how='left', suffixes=('', '_mean'))

    #for QA purpses lets take the raw data and calculate the incision ratio
    #I can also do this manually in Pro when doing the initial QA check
    # Group by 'Assessment_Unit_Name' and calculate the sum and count
    #If you are processing data from 2023 or earlier change the scoring to point to incision_ratio rather than calculated_incision_ratio_mean

    
    
    #----------------------------------------------------------------#
    #Grade, Score
    #----------------------------------------------------------------#
    df['Incision_Rating'] = df['calculated_incision_ratio_mean'].apply(categorize_incision)
    #df['Incision_Rating']=df['incision_ratio'].apply(categorize_incision)
    df['Incision_Score']= df['Incision_Rating'].apply(score_indicator)

    #df['Incision_Data_Source'] = 'TRPA' 
    #Field Mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'Year': 'Year',
        'Incision_Data_Sourc': 'Incision_Data_Source',
        'incision_ratio': 'Incision_Ratio',
        'calculated_incision_ratio_mean': 'Calculated_Ratio',
        'Incision_Rating': 'Incision_Rating',
        'Incision_Score': 'Incision_Score',
        #'SEZ_ID': 'SEZ_ID',
        }

    # Rename fields based on field mappings
    incisionfinaldf = df.rename(columns=field_mapping).drop(columns=[col for col in df.columns if col not in field_mapping])

    readydf = incisionfinaldf.groupby(['SEZ_ID', 'Year']).first().reset_index()
    return readydf
    #----------------------------------------------------------------#
    #post ending dataframe to incision staging table in sde
    #----------------------------------------------------------------#
def post_incision(readydf, draft = False):
    if draft == True:
        readydf.to_csv(r"C:\Users\snewsome\Documents\SEZ\processedincisiondata.csv", index=False)
        # or post to SEZ.gdb?? staging table?
        # Convert DataFrame to a list of dictionaries
        #staging_table= stage_incisiongdb
        #data = readydf.to_dict(orient='records')

        # Append data to staging table directly
        #field_names = list(readydf.columns)
        #with arcpy.da.InsertCursor(staging_table, field_names) as cursor:
         #   for row in data:
          #      cursor.insertRow([row[field] for field in field_names])

        #print(f"Draft data appended to {staging_table} successfully.")

    elif draft == False:
        #uncomment this if you are trying to get data pre 2023
        #drop calcluated incision Ratio
        #readydf = readydf.drop(columns=['Calculated_Ratio'])
        #readydf['Incision_Rating'] = readydf['Incision_Ratio'].apply(categorize_incision)
        #df['Incision_Rating']=df['incision_ratio'].apply(categorize_incision)
        #readydf['Incision_Score']= readydf['Incision_Rating'].apply(score_indicator)
        # Convert DataFrame to a list of dictionaries
        data = readydf.to_dict(orient='records')

        # Get the field names from the field mapping
        field_names = list(readydf.columns)

        # Append data to existing table
        with arcpy.da.InsertCursor(stage_incision, field_names) as cursor:
            for row in data:
                cursor.insertRow([row[field] for field in field_names])

