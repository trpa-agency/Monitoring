from utils import *

#Get graded data From Staging Tables
#Create dataframes from Rest Services 
def get_staging_tables():
    dfbanks = get_fs_data(bank_stability_url)
    dfbiotic = get_fs_data(biotic_integrity_url)
    dfconifer = get_fs_data(conifer_url)
    dfditch = get_fs_data(ditches_url)
    dfinvasive = get_fs_data(invasives_url)
    dfhabitat = get_fs_data(Hab_Frag_url)
    dfvegetation = get_fs_data(vegetation_url)
    dfincision = get_fs_data(incision_url)
    dfheadcuts = get_fs_data(headcuts_url)
    dfAOP = get_fs_data(AOP_url)
    dfSEZ = get_fs_data_spatial(SEZ_url)
    # Return all DataFrames as a dictionary
    return {
        "banks": dfbanks,
        "biotic": dfbiotic,
        "conifer": dfconifer,
        "ditch": dfditch,
        "invasive": dfinvasive,
        "habitat": dfhabitat,
        "vegetation": dfvegetation,
        "incision": dfincision,
        "headcuts": dfheadcuts,
        "AOP": dfAOP,
        "SEZ": dfSEZ
    }

# #------------------#
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
            
    return averaged_df


    # Drop duplicates based on 'Assessment_Unit_Name' and 'Year'
    #dfbiotic = dfbiotic.drop_duplicates(subset=['Assessment_Unit_Name', 'Year', 'Biotic_Integrity_CSCI'])

    # Apply the function to dfbiotic
    averaged_biotic_df = average_biotic_scores(dfbiotic)

        # Apply the rating function to the averaged biotic integrity scores
        averaged_biotic_df['Biotic_Integrity_Rating'] = averaged_biotic_df['Biotic_Integrity_CSCI'].apply(categorize_csci)

        # Calculate the biotic score for each SEZ
        averaged_biotic_df['Biotic_Integrity_Score'] = averaged_biotic_df['Biotic_Integrity_Rating'].apply(score_indicator)

        averaged_biotic_df['Biotic_Integrity_Score']=averaged_biotic_df['Biotic_Integrity_Score'].astype(int)

        return averaged_biotic_df

    #-------------------
    # Headcuts 
    #------------------
    #Reorganize dfHeadcuts to drop small medium large headcut columns
    def format_headcut_staging():
        # Drop the columns 'small', 'medium', and 'large'
        dfheadcuts = dfheadcuts.drop(columns=['small', 'medium', 'large'])

    #---------------------#
    #Base Info SEZ Units#
    #---------------------#
        #Prep SEZ Baseline Data for assessment unit...will need to rethink if acreage changes.. or just manually change in sde
        keep_columns = ['SHAPE', 'SEZ_ID', 'Feature_Type', 'SEZ_Type', 'Ownership_Primary', 'Ownership_Secondary', 'Ownership_Secondary_2', 'Ownership_Secondary_3', 'Acres', 'Comments']
    #dfSEZ is assessment unit information from SDE?
    dfSEZinfo=dfSEZ.loc[:,keep_columns].copy()

    dfSEZinfo['SEZ_ID']= dfSEZinfo['SEZ_ID'].astype(int)

    #-------------------#
    #Formatting#
    #-------------------#
    #Add Year to Source to match final format of SEZ Table
    #add year to data source so we can drop the year column later (Dont double run this)

    #Create Dictionary of Dataframes to adjust year to be in datashource column and not its own column
    yeartodatasource = {
        'dfbanks': dfbanks,
        'dfheadcuts': dfheadcuts,
        'dfincision': dfincision,
        'dfinvasive': dfinvasive
    }

    # Iterate over each DataFrame in meadowdata
    for name, df in yeartodatasource.items():
        # Iterate over columns in the DataFrame
        for col in df.columns:
            # Check if the column name contains 'Data'
            if 'Data_' in col:
                # Add Year to the column if it contains 'Data'
                df[col] = df[col] + ', ' + df['Year'].astype(str)

    #This next bit of code will make sure that all SEZ's are included based on small and large polygon so all SEZ IDs are present and all Feature Types are Present
    #Create Large Polygon and Small Polygon Data frames called meadow and riverine for now so we can assign the correct SEZ_ID
    #Same for meadow(large polygon) and riverine(small polygon) data drop these columns because not needed in final merge, will assign SEZ ID later
    columns_to_drop = {'Year', 'SEZ_ID', 'GlobalID', 'last_edited_user', 'created_date', 'OBJECTID', 'created_user', 'last_edited_date'}

    #Name dataframes so we can reference later
    largepolygondata= {'dfbanks': dfbanks, 
                'dfaveraged_biotic':averaged_biotic_df,
                    'dfconifer': dfconifer,
                    'dfditch': dfditch,
                    'dfinvasive': dfinvasive,
                    'dfhabitat': dfhabitat,
                    'dfvegetation': dfvegetation,
                    'dfincision': dfincision,
                    'dfheadcuts': dfheadcuts,
                    'dfAOP': dfAOP
    }


    #Staging Tables Riverine/ small polygons
    smallpolygondata = {'dfbanks': dfbanks, 
                    'dfaveraged_biotic':averaged_biotic_df,
                    'dfconifer': dfconifer,
                    'dfditch': dfditch,
                    'dfinvasive': dfinvasive,
                    'dfhabitat': dfhabitat,
                    'dfvegetation': dfvegetation,
                    'dfincision': dfincision,
                    'dfheadcuts': dfheadcuts,
                    'dfAOP': dfAOP
    }

    #Get most recent year of data for each Assessment Unit NAme
    # Function to get the most recent year of data
    # Function to get the most recent year of data
    def get_most_recent_scores(df, groupfield):
        return df.loc[df.groupby(groupfield)['Year'].idxmax()]

    #most_recent_small = get_most_recent_scores(smallpolygondata, 'Assessment_Unit_Name')
    #mosrecent_large = get_most_recent_scores(largepolygondata, 'Assessment_Unit_Name')

    # Function to drop unnecessary columns from DataFrames
    def drop_columns(df, columns_to_drop):
        return df.drop(columns=[col for col in columns_to_drop if col in df.columns])


    # Function to assign SEZ_ID to each DataFrame using the provided lookup dictionary
    def assign_sez_ids(df, sezid_dict):
        df['SEZ_ID'] = df['Assessment_Unit_Name'].map(sezid_dict)
        df = df.dropna(subset=['SEZ_ID'])
        
        # Use .loc to modify SEZ_ID safely
        df.loc[:, 'SEZ_ID'] = df['SEZ_ID'].astype(int)
        
        return df

    # Process data for large and small polygons
    def process_data(data_dict, sezid_dict, columns_to_drop):
        processed_data = {}
        for key, df in data_dict.items():
            # Step 1: Get most recent scores
            df_most_recent = get_most_recent_scores(df, 'Assessment_Unit_Name')
            
            # Step 2: Drop unnecessary columns
            df_cleaned = drop_columns(df_most_recent, columns_to_drop)
            
            # Step 3: Assign SEZ_ID
            df_with_sez_id = assign_sez_ids(df_cleaned, sezid_dict)
            
            # Store the processed DataFrame
            processed_data[key] = df_with_sez_id
        return processed_data

    # Process large polygon (meadow) and small polygon (riverine) data
    processed_largepolygon_data = process_data(largepolygondata, lookup_dict, columns_to_drop)
    processed_smallpolygon_data = process_data(smallpolygondata, lookup_riverine, columns_to_drop)


