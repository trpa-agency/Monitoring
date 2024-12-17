from SEZ_scripts.utils import *

#Get Erosion Data from SDE Collect
def get_incision_data():
    # make sql database connection with pyodbc
    engine = get_conn('sde_collection')
    # get BMP Status data as dataframe from BMP SQL Database
    with engine.begin() as conn:
        # create dataframe from sql query is incision ratio the correct value to get (maybe use raw scores instead)
        df  = pd.read_sql('SELECT Assessment_Unit_Name, incision_ratio, survey_date FROM sde.SDE.Survey', conn)
    return df

#Do I need this?
# Initialize an empty list to store data
#data = []

 # Iterate over the rows in the feature class and extract data
#with arcpy.da.SearchCursor(sezsurveytable, incisionfields) as cursor:
 #   for row in cursor:
  #      data.append(row)

# Convert the data to a Pandas DataFrame
#df = pd.DataFrame(data, columns=incisionfields)

def process_grade_incision(df, draft=True):
#----------------------------------------------------------------#
#Add correct info to dataframe
#----------------------------------------------------------------#
    #use this until we fix the domain- did we fix it? 12/9/24
    df['Assessment_Unit_Name'] = df['Assessment_Unit_Name'].replace({'Blackwood Creek - upper 2': 'Blackwood Creek - Upper 2', 'Taylor Creek marsh - 1': 'Taylor Creek marsh'})
    # Create a new column 'SEZ ID'
    #This code is for the excel look up dictionary
    df['SEZ_ID'] = df['Assessment_Unit_Name'].map(lookup_dict)

    # Fill NaN values with a specific value, such as 0
    df['SEZ_ID'] = df['SEZ_ID'].fillna(0)

    # Convert SEZ ID column to integer
    df['SEZ_ID'] = df['SEZ_ID'].astype(int)

    #calculate year column 
    df['Year'] = df['survey_date'].dt.year
    #----------------------------------------------------------------#
    #Grade, Score
    #----------------------------------------------------------------#
    df['Incision_Rating']=df['incision_ratio'].apply(categorize_incision)
    df['Incision_Score']= df['Incision_Rating'].apply(score_indicator)

    df['Incision_Data_Source'] = 'TRPA' 
    
    #----------------------------------------------------------------#
    #post ending dataframe to incision staging table in sde
    #----------------------------------------------------------------#
def post_incision(df, draft=True):
    # Define the name of the feature class
    feature_class_name = 'incision'

    # Define the full path to the feature class
    feature_class_path = stage_incision 

    #Field Mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'Year': 'Year',
        'Incision_Data_Source': 'Incision_Data_Source',
        'incision_ratio': 'Incision_Ratio',
        'Incision_Rating': 'Incision_Rating',
        'Incision_Score': 'Incision_Score',
        'SEZ_ID': 'SEZ_ID'
    }

    # Rename fields based on field mappings
    incisionfinaldf = df.rename(columns=field_mapping).drop(columns=[col for col in df.columns if col not in field_mapping])

    readydf = incisionfinaldf.groupby(['SEZ_ID', 'Year']).first().reset_index()

    # Convert DataFrame to a list of dictionaries
    data = readydf.to_dict(orient='records')

    # Get the field names from the field mapping
    field_names = list(readydf.columns)

    # Append data to existing table
    with arcpy.da.InsertCursor(stage_incision, field_names) as cursor:
        for row in data:
            cursor.insertRow([row[field] for field in field_names])

