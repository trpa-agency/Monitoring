from utils import *

#This brings in teh Basic SEZ Info like Acres, SHAPE, Assessment Unit Name ,Type as well as data from the old table???

    #Create Plant Priority look up dictionary 
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
    
    # Group by assessment unit and year and summarize the priority level of plants in each unit
    invasive_summary = df.groupby(['Assessment_Unit_Name', 'Year', 'Priority', 'Source'], dropna=False).size().reset_index(name='Count')

    # Pivot to summarize by priority
    invasive_summary_priority = invasive_summary.pivot_table(index=['Assessment_Unit_Name', 'Year', 'Source'], columns='Priority', values='Count', fill_value=0)

    # Reset the index to flatten the DataFrame
    invasive_summary_priority.reset_index(inplace=True)
    priority_columns = [col for col in invasive_summary_priority.columns if isinstance(col, int)]
    invasive_summary_priority['Invasives_Rating'] = invasive_summary_priority[priority_columns].apply(rate_invasive, axis=1)
    invasive_summary_priority['Number_of_Invasives'] = invasive_summary_priority[priority_columns].sum(axis=1)

    # Apply the rating function to the summary DataFrame
    #invasive_summary_priority['Invasives_Rating'] = invasive_summary_priority[[1, 2, 3, 4]].apply(rate_invasive, axis=1)

    # Calculate the score for the SEZ
    invasive_summary_priority['Invasives_Score'] = invasive_summary_priority['Invasives_Rating'].apply(score_indicator)

    # Calculate total number of invasives per SEZ per year
    #invasive_summary_priority['Number_of_Invasives'] = invasive_summary_priority[[1, 2, 3, 4]].sum(axis=1)

    # Define SEZ ID based on Assessment_Unit_Name for QA on SEZ Name
    invasive_summary_priority['SEZ_ID'] = invasive_summary_priority['Assessment_Unit_Name'].map(lookup_dict)

    invasive_summary_priority['all_plants'] = combined_plant_types['all_plant_types']

    # Field Mapping
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
    readydf = invasive_summary_priority.rename(columns=field_mapping).drop(columns=[col for col in invasive_summary_priority.columns if col not in field_mapping])

    # Final SEZ ID check
    readydf['SEZ_ID'] = readydf['Assessment_Unit_Name'].map(lookup_dict)

    return grouped_df, combined_plant_types, readydf
