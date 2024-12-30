from utils import *
def get_bioassessment_data():
    #Get Stream Data/CSCI scores
    stream_url = "https://maps.trpa.org/server/rest/services/LTInfo_Monitoring/MapServer/8"
    streamsdf = get_fs_data_spatial(stream_url)
    ##SEZ Data Import
    SEZ_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/0"
    dfSEZ = get_fs_data_spatial(SEZ_url)
    #spatial reference stuff
    streamsdf.spatial.sr = dfSEZ.spatial.sr

    #perform spatial join of sde.stream and sez units
    thesdf = dfSEZ.spatial.join(streamsdf, how='inner')

def get_bioassessment_data_fc():
    arcpy.env.workspace = streamdata
    feature_class= "Stream"

    # Convert feature class to a pandas DataFrame
    fields = [field.name for field in arcpy.ListFields(feature_class)]

    # Create DataFrame
    streamsdf = pd.DataFrame.spatial.from_featureclass(feature_class, columns=fields)

    # Create the spatially enabled DataFrame (sdf) for target feature SEZ assessment units
    #SEZsdf = pd.DataFrame.spatial.from_featureclass(sdemonitoring)
    ##SEZ Data Import
    SEZ_url = "https://maps.trpa.org/server/rest/services/SEZ_Assessment_Unit/FeatureServer/0"
    dfSEZ = get_fs_data_spatial(SEZ_url)
    #spatial reference stuff
    streamsdf.spatial.sr = dfSEZ.spatial.sr

    #perform spatial join of sde.stream and sez units
    df = dfSEZ.spatial.join(streamsdf, how='inner')
    return df
def process_grade_bioassessment(df):
    #----------------------#
    #DATA PREP
    #----------------------#
    # List of columns to keep
    columns_to_keep = ['Assessment_Unit_Name', 'SEZ_Type', 'Feature_Type', 'SEZ_ID','SITE_NAME', 'COUNT_VALUE', 'YEAR_OF_COUNT', 'STATION_TYPE', 'LONGITUDE', 'LATITUDE', ]
    ##Try this instead
    # Select only the desired columns
    bioticdf = df.loc[:, columns_to_keep].copy()  

    #DATA PREP
    # Filter for years 2020 to 2023
    filtered_df = bioticdf.loc[(bioticdf['YEAR_OF_COUNT'] >= 2020) & (bioticdf['YEAR_OF_COUNT'] <= 2023)].copy()

    # Define SEZ ID based on Assessment_Unit_Name for QA on SEZ Name THIS METHOD USES LARGER POLYGONES 
    filtered_df.loc[:, 'SEZ_ID'] = filtered_df['Assessment_Unit_Name'].map(lookup_dict)

    # Fill NaN values with a specific value, such as 0
    filtered_df.loc[:, 'SEZ_ID'] = filtered_df['SEZ_ID'].fillna(0)

    # Convert SEZ ID column to integer
    filtered_df.loc[:, 'SEZ_ID'] = filtered_df['SEZ_ID'].astype(int)

    # Replace values in the 'Assessment_Unit_Name' column
    filtered_df.loc[:, 'Assessment_Unit_Name'] = filtered_df['Assessment_Unit_Name'].replace({'Blackwood Creek - upper 2': 'Blackwood Creek - Upper 2', 'Taylor Creek marsh - 1': 'Taylor Creek marsh', 'Sky Meadows meadow - 1': 'Sky meadows'})

    # Add data source information
    filtered_df['Source'] = 'TRPA, ' + filtered_df['SITE_NAME'].astype(str) + ', ' + filtered_df['YEAR_OF_COUNT'].astype(str)

    #Group by Year and Assessment Unit and Site NAME and remove duplicates

    filtered_df['SITE_NAME'] = filtered_df['SITE_NAME'].str.strip()
    filtered_df['YEAR_OF_COUNT'] = filtered_df['YEAR_OF_COUNT'].astype(str).str.strip().astype(int)
    filtered_df['YEAR_OF_COUNT'] = pd.to_numeric(filtered_df['YEAR_OF_COUNT'], errors='coerce')


    # Group by Assessment_Unit_Name, SITE_NAME, and YEAR_OF_COUNT and drop duplicates
    BIdf = filtered_df.groupby(['SEZ_ID', 'SITE_NAME', 'YEAR_OF_COUNT', 'COUNT_VALUE']).apply(lambda x: x.drop_duplicates()).reset_index(drop=True)

    #----------------------#
    #Grade and Score biotic integrity
    #----------------------#

    #Rate the score
    #ef categorize_csci(biotic_integrity):
    # Apply the rating function to the summary DataFrame
    BIdf['Biotic_Rating'] = BIdf['COUNT_VALUE'].apply(categorize_csci)

    #Calculate the score for the sez
    BIdf['Biotic_Score']= BIdf['Biotic_Rating'].apply(score_indicator) 

    #Field Mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'YEAR_OF_COUNT': 'Year',
        'Source': 'Biotic_Integrity_Data_Source',
        'COUNT_VALUE': 'Biotic_Integrity_CSCI',
        'Biotic_Rating': 'Biotic_Integrity_Rating',
        'Biotic_Score': 'Biotic_Integrity_Score',
        'SEZ_ID': 'SEZ_ID',
    }

    # Rename fields based on field mappings
    readydf = BIdf.rename(columns=field_mapping).drop(columns=[col for col in BIdf.columns if col not in field_mapping])

    readydf['SEZ_ID'] = readydf['Assessment_Unit_Name'].map(lookup_dict)
    return readydf
    #Post data frame for QA if draft is true and to final table if draft is false
def post_bioassessment(readydf, draft=True):
    if draft == True:
        readydf.to_csv(r"C:\Users\snewsome\Documents\SEZ\processedbioassessmentdata.csv", index=False)
        # or post to SEZ.gdb?? staging table?
        # Convert DataFrame to a list of dictionaries
        #staging_table= stage_biotic_integritygdb
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
        with arcpy.da.InsertCursor(stage_biotic_integrity, field_names) as cursor:
            for row in data:
                cursor.insertRow([row[field] for field in field_names])
