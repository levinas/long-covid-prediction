# typical runtime: 5m

def feature_expansion_model36(model36_start, concept_to_feature, concept):
    df = model36_start
    df_features = ml_classify_df(df, seed=42, return_features=True)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return df_features