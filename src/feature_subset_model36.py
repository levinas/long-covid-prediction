# typical runtime: 4m

def feature_subset_model36(subset_model36, concept_to_feature, concept):
    df = subset_model36
    df_features = ml_classify_df(df, seed=42, return_features=True)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return df_features