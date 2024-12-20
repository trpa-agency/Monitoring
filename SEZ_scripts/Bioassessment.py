from utils import *
def get_Stream_data():
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

def get_stream_data_fc():
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
    thesdf = dfSEZ.spatial.join(streamsdf, how='inner')

def process_grade_bioassessment():