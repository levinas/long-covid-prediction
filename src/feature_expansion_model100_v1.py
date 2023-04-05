# typical runtime: 6m;  output shape: 7 x 1102

def feature_expansion_model100_v1(model100_start, concept_to_feature, concept):
    df = model100_start
    df_features = ml_classify_df(df, return_features=True)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return df_features