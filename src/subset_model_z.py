# typical runtime: 15s

def subset_model_z(model_z_start, feature_expansion_model_z):
    df_feature = feature_expansion_model_z
    df_group = aggregate_feature_importance_to_groups(df_feature)
    df_rep = keep_last_occurrence(df_group, 'group', 'group_score') \
        .where(col('categorical') | (col('domain_id') == 'Measurement'))
    
    n_keep = 96
    subset1 = df_rep.toPandas()['feature'].tolist()
    df_remain = df_feature.where(~col('feature').isin(subset1))    
    n = n_keep - len(subset1)
    subset2 = extract_list_head_from_df(df_remain, 'importance', 'feature', n)

    subset = subset1 + subset2
    df = model_z_start.select('person_id', 'pasc', 'time_to_pasc', *subset)

    return df  