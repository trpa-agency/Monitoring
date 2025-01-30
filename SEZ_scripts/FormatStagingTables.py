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
    

    # Process data for large and small polygons
def process_data(data_dict, sezid_dict, columns_to_drop):
    for key, df in data_dict.items():
        # Step 1: Get most recent scores
        df.loc[df.groupby('Assessment_Unit_Name')['Year'].idxmax()]
        
        # Step 2: Drop unnecessary columns
        df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        # Step 3: Assign SEZ_ID
        #df_with_sez_id = assign_sez_ids(df_cleaned, sezid_dict)
        df['SEZ_ID'] = df['Assessment_Unit_Name'].map(sezid_dict)
        df = df.dropna(subset=['SEZ_ID'])
    
        # Use .loc to modify SEZ_ID safely
        df.loc[:, 'SEZ_ID'] = df['SEZ_ID'].astype(int)
            # Iterate over columns in the DataFrame
        for col in df.columns:
            # Check if the column name contains 'Data'
            if 'Data_' in col:
                # Add Year to the column if it contains 'Data'
                df[col] = df[col] + ', ' + df['Year'].astype(str)
    return df

# # Function to merge all DataFrames on multiple keys(why on multiple keys? and not just SEZ ID)
# def merge_dataframes(data_dict, keys):
#     return reduce(lambda left, right: pd.merge(left, right, on=keys, how='outer'), data_dict.values())
# Function to merge multiple DataFrames on SEZ_ID only
def merge_dataframes(data_dict, key='SEZ_ID'):
    """
    Merge multiple DataFrames stored in a dictionary on a single key.

    Parameters:
        data_dict (dict): Dictionary of DataFrames to merge.
        key (str): Column name to merge on.

    Returns:
        pd.DataFrame: Merged DataFrame with one row per unique SEZ_ID.
    """
    if not data_dict or key not in list(data_dict.values())[0].columns:
        raise ValueError(f"Invalid input: Ensure data_dict contains DataFrames with '{key}'.")

    # Convert dictionary values (DataFrames) to a list
    dataframes = list(data_dict.values())

    # Merge all DataFrames iteratively on SEZ_ID
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=key, how='outer'), dataframes)

    return merged_df


