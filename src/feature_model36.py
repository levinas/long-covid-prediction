# typical runtime: 1m

def feature_model36(model36_data, concept_to_feature, concept):
    df = model36_data
    df_features = ml_classify_df(df, return_features=True)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return df_features