# typical runtime: 4m

def feature_model100_v2(model100_v2_data, concept_to_feature, concept):
    df = model100_v2_data
    df_features = ml_classify_df(df, return_features=True)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return df_features