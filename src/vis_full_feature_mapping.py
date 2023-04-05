# can be removed

def vis_full_feature_mapping(vis_full_features, concept_to_feature, concept):
    df = vis_full_features
    df_features = ml_classify_df(df, return_features=True)
    df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
        .orderBy(col('importance').desc())
    return aggregate_feature_importance_to_categories(df_features)