# typical runtime: 4m

def feature_subset_model_z(subset_model_z, concept_to_feature, concept):
    df = subset_model_z
    df_features = ml_classify_df(df, model='XGB', return_features=True)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return df_features