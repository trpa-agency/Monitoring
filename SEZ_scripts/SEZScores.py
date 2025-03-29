from utils import *

# Define indicator mappings for each SEZ Type
SEZ_Score_Indicators = {
    'Riverine (Perennial)': ['AOP_Score', 'Bank_Stability_Score', 'Biotic_Integrity_Score', 'Habitat_Frag_Score', 'Incision_Score', 'Headcuts_Score'],
    'Riverine (Perennial) + Forested': ['AOP_Score', 'Bank_Stability_Score', 'Biotic_Integrity_Score', 'Ditches_Score', 'Habitat_Frag_Score', 'Incision_Score', 'Headcuts_Score'],
    'Non-Channeled Meadow': ['Invasives_Scores', 'Conifer_Encroachment_Score', 'Ditches_Score', 'Habitat_Frag_Score', 'Headcuts_Score', 'VegetationVigor_Score'],
    'Channeled Meadow': ['AOP_Score', 'Bank_Stability_Score', 'Biotic_Integrity_Score', 'Invasives_Scores', 'Conifer_Encroachment_Score', 'Ditches_Score', 'Habitat_Frag_Score', 'Incision_Score', 'Headcuts_Score', 'VegetationVigor_Score'],
    'Forested': ['Bank_Stability_Score', 'Ditches_Score', 'Habitat_Frag_Score', 'Headcuts_Score']
}

# Function to get the relevant indicators for each SEZ Type
def get_score_columns(sez_type):
    return SEZ_Score_Indicators.get(sez_type, [])


# Function to remove irrelevant scores
def filter_scores_by_sez_type(row, columns):
    allowed_columns = set(row['Score_Columns'])
    new_row = row.copy()
    for col in columns:
        if col not in allowed_columns and col not in ['SEZ_ID', 'SEZ_Type', 'Score_Columns']:
            new_row[col] = np.nan
    return new_row

# Function to calculate total points and points possible
def calculate_scores(row):
    score_columns = row['Score_Columns']
    if not score_columns or any(col not in row.index for col in score_columns):  # Check if columns are valid
        return pd.Series([np.nan, np.nan])
    total_points = row[score_columns].sum(skipna=True)
    points_possible = row[score_columns].notna().sum() * 12
    return pd.Series([total_points, points_possible])


