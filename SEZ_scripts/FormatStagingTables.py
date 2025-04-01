from utils import *

 #------------------#
#Biotic Integrity
#------------------#
#Prep data- Add any scores and find average oif there are two stream sites for one sez. Also rename data source so it includes are streams that were averaged
# Function to average scores and concatenate data sources for each Year and Assessment_Unit_Name    
def average_biotic_scores(dfbiotic, unit_col='Assessment_Unit_Name', year_col='Year', score='Biotic_Integrity_CSCI', source_col='Biotic_Integrity_Data_Source'):
    # Group by Assessment Unit and Year
    group = dfbiotic.groupby([unit_col, year_col])
        
    # Calculate the mean of the scores
    averaged_scores = group[score].mean().reset_index()
        
    # Concatenate the data sources with specific formatting
    def concatenate_sources(x, year):
        formatted_sources = []
        for entry in x:
            parts = entry.split(",")
            if len(parts) >= 3:
                formatted_sources.append(f'TRPA, {parts[1].strip()}, {parts[-1].strip()}')  # Extract station code and year
        if formatted_sources:
            return '/ '.join(formatted_sources)
        else:
            return None  # Return None if all entries are invalid
            
    # Apply concatenate_sources to each group
    concatenated_sources = group.apply(lambda grp: concatenate_sources(grp[source_col], grp[year_col])).reset_index(name=source_col)
            
    # Merge the averaged scores with concatenated sources
    averaged_df = pd.merge(averaged_scores, concatenated_sources, on=[unit_col, year_col], how='left')

    # Apply the rating function to the averaged biotic integrity scores
    averaged_df['Biotic_Integrity_Rating'] = averaged_df['Biotic_Integrity_CSCI'].apply(categorize_csci)

    # Calculate the biotic score for each SEZ
    averaged_df['Biotic_Integrity_Score'] = averaged_df['Biotic_Integrity_Rating'].apply(score_indicator)

    averaged_df['Biotic_Integrity_Score']=averaged_df['Biotic_Integrity_Score'].astype(int)

    return averaged_df

def process_data(data_dict, sezid_dict, columns_to_drop):
    processed_data = {}  # Store processed DataFrames
    
    for key, df in data_dict.items():
        df = df.copy()  # Avoid modifying the original DataFrame
        print(f"Processing DataFrame: {key}")
        print("Columns:", df.columns)
        
        # Step 1: Get most recent scores
        if 'Year' not in df.columns:
            raise KeyError(f"'Year' column is missing in DataFrame: {key}")

        df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
        df = df.dropna(subset=['Year'])  # Drop NaN years before using idxmax()

        if df.empty:
            print(f"Warning: DataFrame {key} is empty after dropping NaN 'Year' values.")
            continue

        if 'Assessment_Unit_Name' not in df.columns:
            raise KeyError(f"'Assessment_Unit_Name' column is missing in DataFrame: {key}")

        df_most_recent = df.loc[df.groupby('Assessment_Unit_Name')['Year'].idxmax()]

        # Step 2: Drop unnecessary columns
        df = df_most_recent.drop(columns=[col for col in columns_to_drop if col in df_most_recent.columns])

        # Step 3: Assign SEZ_ID
        df['SEZ_ID'] = df['Assessment_Unit_Name'].map(sezid_dict)

        if df['SEZ_ID'].isna().any():
            print(f"Warning: {df['SEZ_ID'].isna().sum()} rows in {key} have missing SEZ_IDs and will be dropped.")

        df = df.dropna(subset=['SEZ_ID']).copy()  # Ensure no chained assignment issues

        if df.empty:
            print(f"Warning: DataFrame {key} is empty after dropping NaN SEZ_ID values.")
            continue

        df['SEZ_ID'] = df['SEZ_ID'].astype(int)  # Convert safely

        # Store the processed DataFrame in the dictionary
        processed_data[key] = df

    return processed_data  # Return the processed dictionary
    
# Field Mapping so 2019 threshol data renamed so it can be joined to new data
field_mapping = {
    'Assessment_Unit_Name': 'Assessment_Unit_Name',
    'Threshold_Year': 'Threshold Year',
    'Comments': 'Comments',
    'SEZ_Type': 'SEZ_Type',
    'SEZ_ID': 'SEZ_ID',
    'Acres': 'Acres',
    'AquaticOrganismPassage_Barriers': 'AOP_BarriersPerMile',
    'AquaticOrganismPassage_NumberOf': 'AOP_NumberofBarriers',
    'AquaticOrganismPassage_Score': 'AOP_Score',
    'AquaticOrganismPassage_Rating': 'AOP_Rating',
    'AquaticOrganismPassage_StreamMi': 'AOP_StreamMiles',
    'AquaticOrganismPassage_DataSour': 'AOP_DataSource',
    'Bank_Stability_Data_Source': 'Bank_Stability_Data_Source',
    'Bank_Stability_Percent_Unstable': 'Bank_Stability_Percent_Unstable',
    'Bank_Stability_Rating': 'Bank_Stability_Rating',
    'Bank_Stability_Score': 'Bank_Stability_Score',
    'Biotic_Integrity_Rating': 'Biotic_Integrity_Rating',
    'Biotic_Integrity_CSCI': 'Biotic_Integrity_CSCI',
    'Biotic_Integrity_Data_Source': 'Biotic_Integrity_Data_Source',
    'Biotic_Integrity_Score': 'Biotic_Integrity_Score',
    'Conifer_Encroachment_Percent_En': 'Conifer_Percent_Encroached',
    'Conifer_Encroachment_Data_Sourc': 'Conifer_Encroachment_Data_Sourc',
    'Conifer_Encroachment_Rating': 'Conifer_Encroachment_Rating',
    'Conifer_Encroachment_Score': 'Conifer_Encroachment_Score',
    'ConiferEncroachment_Comments': 'ConiferEncroachment_Comments',
    'Ditches_Data_Source': 'Ditches_Data_Source',
    'Ditches_Length': 'Ditches_Length',
    'Ditches_Meadow_Length': 'Ditches_Meadow_Length',
    'Ditches_Percent': 'Ditches_Percent',
    'Ditches_Rating': 'Ditches_Rating',
    'Ditches_Score': 'Ditches_Score',
    'Feature_Type': 'Feature_Type',
    'Habitat_Fragmentation_Data_Sour': 'Habitat_Frag_Data_Source',
    'Habitat_Fragmentation_Imperviou': 'Habitat_Frag_Impervious_Acres',
    'Habitat_Fragmentation_Percent_I': 'Habitat_Frag_Percent_Impervious',
    'Habitat_Fragmentation_Rating': 'Habitat_Frag_Rating',
    'Habitat_Fragmentation_Score': 'Habitat_Frag_Score',
    'Headcuts_Data_Source': 'Headcuts_Data_Source',
    'Headcuts_Number_of_Headcuts':'Number_of_Headcuts',
    'Headcuts_Rating': 'Headcuts_Rating',
    'Headcuts_Score': 'Headcuts_Score',
    'Incision_Data_Source': 'Incision_Data_Source',
    'Incision_Rating': 'Incision_Rating',
    'Incision_Score': 'Incision_Score',
    'Incision_Ratio': 'Incision_Ratio',
    'Invasive_Percent_Cover': 'Invasive_Percent_Cover',
    'Invasive_Rating': 'Invasives_Rating',
    'Invasives_Data_Source': 'Invasives_Data_Source',
    'Invasives_Number_of_Invasives': 'Invasives_Number_of_Invasives',
    'Invasives_Plant_Types': 'Invasives_Plant_Types',
    'Invasives_Scores': 'Invasives_Scores',
    'NDVI_ID': 'NDVI_ID',
    'Ownership_Primary': 'Ownership_Primary',
    'Ownership_Secondary': 'Ownership_Secondary',
    'Ownership_Secondary_2': 'Ownership_Secondary_2',
    'Ownership_Secondary_3': 'Ownership_Secondary_3',
    'VegetationVigor_DataSource': 'VegetationVigor_DataSource',
    'VegetationVigor_Rating': 'VegetationVigor_Rating',
    'VegetationVigor_Raw': 'VegetationVigor_Raw',
    'VegetationVigor_Score': 'VegetationVigor_Score'
}

#create final table by bringing in most recent year of dta from our database and overwriting with new data
# Function to create the final table
def create_final_table(df, field_mapping, columns_to_drop):
    # Rename columns according to the field mapping
    df.rename(columns=field_mapping, inplace=True)

    # Drop unnecessary columns
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    # Reorder columns to match the desired output
    ordered_columns = list(field_mapping.values())
    df = df[ordered_columns]

    return df 