# typical runtime: 10s

def model100_v2_data(subset_model100_v2, feature_subset_model100_v2):
    df_feature = feature_subset_model100_v2
    df_group = aggregate_feature_importance_to_groups(df_feature)
    df_rep = keep_last_occurrence(df_group, 'group', 'group_score') \
        .where(col('categorical') | (col('domain_id') == 'Measurement'))
    
    n_keep = 200
    subset1 = df_rep.toPandas()['feature'].tolist()
    df_remain = df_feature.where(~col('feature').isin(subset1))    
    n = n_keep - len(subset1)
    subset2 = extract_list_head_from_df(df_remain, 'importance', 'feature', n)

    subset = subset1 + subset2
    df = subset_model100_v2.select('person_id', 'pasc', 'time_to_pasc', *subset)

    return df  