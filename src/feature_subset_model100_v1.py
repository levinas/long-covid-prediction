# typical runtime: 5m

def feature_subset_model100_v1(subset_model100_v1, concept_to_feature, concept):
    df = subset_model100_v1
    df_features = ml_classify_df(df, return_features=True)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return df_features