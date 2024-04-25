from arcgis.features import FeatureLayer
import pandas as pd
# Gets spatially enabled dataframe with query
def get_fs_data_spatial_query(service_url, query_params):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query(query_params).sdf
    return query_result

