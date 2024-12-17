from SEZ_scripts.utils import *
#global variable
#get path to save file
#outchart=local_path.parents{1}/ '2023/SEZ/Analysis blah blah

#Get Erosion Data from SDE Collect
def get_erosion_data():
    # make sql database connection with pyodbc
    engine = get_conn('sde_collection')
    # get BMP Status data as dataframe from BMP SQL Database
    with engine.begin() as conn:
        # create dataframe from sql query
        df  = pd.read_sql('SELECT Assessment_Unit_Name, Shape.STLength(), Bank_Type, Survey_Date FROM ##!!sde.Collect.SDE.Monitoring', conn)
    return df
#Clean Up Raw Erosion Data
def process_erosion(df):
    #convert df to pandas or is it already there?
    #do i need to add SEZ ID? If I end up needing it place here
    # Replace NaN values in 'Assessment_Unit_Name' column with 'Skylandia SEZ'
    #erosiondf['Assessment_Unit_Name'] = erosiondf['Assessment_Unit_Name'].fillna('Skylandia SEZ')
    # Replace specific values in 'Assessment_Unit_Name' column
    df['Assessment_Unit_Name'] = df['Assessment_Unit_Name'].replace({'Blackwood Creek - upper 2': 'Blackwood Creek - Upper 2', 'Taylor Creek marsh - 1': 'Taylor Creek marsh'})


    #Add SEZ_ID column using lookup dictionary
    #erosiondf['SEZ_ID'] = erosiondf['Assessment_Unit_Name'].map(lambda x: lookup_dict.get(x, {}).get('SEZ_ID', None))

    #This code is for the excel look up dictionary
    df['SEZ_ID'] = df['Assessment_Unit_Name'].map(lookup_dict)


    # Fill NaN values with a specific value, such as 0
    df['SEZ_ID'] = df['SEZ_ID'].fillna(0)

    # Convert SEZ ID column to integer
    df['SEZ_ID'] = df['SEZ_ID'].astype(int)
    
    # Replace 'both_banks' with 'Both Banks' in Bank_Type column
    df['Bank_Type'] = df['Bank_Type'].replace(['both_banks', 'Both banks'], 'Both Banks' )
    df['Bank_Type'] = df['Bank_Type'].replace(['one_bank', 'One bank'], 'One Bank')
    df['Bank_Type'] = df['Bank_Type'].replace(['no_bank', 'No bank'], 'No Bank')

    #calculate year column 
    df['Year'] = df['Survey_Date'].dt.year

    #----------------------------------------------------------------#
    #Process Data
    #----------------------------------------------------------------#

    # Initialize variables
    df['bank_multiplier'] = df['Bank_Type'].apply(lambda x: 2 if x == 'Both Banks' else (1 if x == 'One Bank' else 0))


    # Calculate the product of 'Shape.STLength()' and 'bank_multiplier' to get the eroded banks per row
    df['eroded_banks_per_row'] = df['Shape.STLength()'] * df['bank_multiplier']

    # Group by Assessment_Unit_Name and year and sum the lengths of banks for each unit to get total banks assessed
    df['banks_assessed_per_unit'] = df.groupby(['Assessment_Unit_Name', 'Year'])['Shape.STLength()'].transform('sum') * 2

    # Group by Assessment_Unit_Name and sum the eroded banks per row for each unit
    df['SEZ_total_eroded'] = df.groupby(['Assessment_Unit_Name', 'Year'])['eroded_banks_per_row'].transform('sum')

    # Calculate percent unstable Bank Stability per Assessment Unit
    df['Bank_Stability_Percent_Unstable'] = (df['SEZ_total_eroded'] / df['banks_assessed_per_unit']) * 100

    #----------------------------------------------------------------#
    #Grade, Score
    #----------------------------------------------------------------#
    df['Bank_Stability_Rating']= df['Bank_Stability_Percent_Unstable'].apply(categorize_erosion)
    df['Bank_Stability_Score']= df['Bank_Stability_Rating'].apply(score_indicator)

    df['Bank_Stability_Data_Source'] = 'TRPA' 
    #print some kind of QA tool here?
    return df
#DO QA Before you post data t0 table
def post_erosion(df):
    # Define the name of the feature class
    feature_class_name = 'bank_stability'

    # Define the full path to the feature class--grab from sde collect
    feature_class_path = stage_bank_stability 

    #Field Mapping
    field_mapping = {
        'Assessment_Unit_Name': 'Assessment_Unit_Name',
        'Year': 'Year',
        'Bank_Stability_Data_Source': 'Bank_Stability_Data_Source',
        'Bank_Stability_Percent_Unstable': 'Bank_Stability_Percent_Unstable',
        'Bank_Stability_Rating': 'Bank_Stability_Rating',
        'Bank_Stability_Score': 'Bank_Stability_Score',
        'SEZ_ID': 'SEZ_ID'
    }

    # Rename fields based on field mappings
    bank_stabilitydf = df.rename(columns=field_mapping).drop(columns=[col for col in df.columns if col not in field_mapping])

    readydf = bank_stabilitydf.groupby(['SEZ_ID', 'Year']).first().reset_index()   
    # Convert DataFrame to a list of dictionaries
    data = readydf.to_dict(orient='records')

    # Get the field names from the field mapping
    field_names = list(readydf.columns)

   
    
    # Append data to existing table
    with arcpy.da.InsertCursor(stage_bank_stability, field_names) as cursor:
       for row in data:
          cursor.insertRow([row[field] for field in field_names])
 

   