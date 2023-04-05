# typical runtime: 6m

def feature_expansion_model100_v2(model100_start, concept_to_feature, concept):
    cols = ['zip_id', 'months_from_last_visit', 'covid_index_to_last_visit_end']
    df = model100_start.drop(*cols)    
    df_features = ml_classify_df(df, return_features=True, seed=2023)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return df_features