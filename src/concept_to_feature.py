# typical runtime: 1m;  output shape: 17 x 57010

def concept_to_feature(event_stats, concept_sets):
    df_concept = event_stats # assumes columns: concept_id, concept_name
    df_bundle = concept_sets  # assumes columns: concept_id, concept_name, codeset_id, concept_set_name, bundle_id, bundle_name

    selected_concept_ids = get_selected_concept_ids()
    selected_set_names = get_selected_concept_set_names()
    custom_sets = create_custom_concept_sets(df_bundle, df_concept)

    null = lit(None)

    case1 = col('concept_id').isin(selected_concept_ids)
    df1 = df_concept.where(case1) \
        .withColumn('feature_source', lit('concept_id')) \
        .withColumn('feature_id', format_string('c%d', 'concept_id')) \
        .withColumn('feature_name', format_string('C: %s', 'concept_name'))
    
    df1.count()

    case2 = col('concept_set_name').isin(selected_set_names)
    df2 = df_bundle.where(case2) \
        .withColumn('feature_source', lit('codeset_id')) \
        .withColumn('feature_id', format_string('s%d', 'codeset_id')) \
        .withColumn('feature_name', format_string('S: %s', 'concept_set_name'))
    
    df2.count()

    case3 = col('concept_set_name').startswith('ARIScience')
    df3 = df_bundle.where(case3) \
        .withColumn('feature_source', lit('codeset_id')) \
        .withColumn('feature_id', format_string('s%d', 'codeset_id')) \
        .withColumn('feature_name', format_string('A: %s', 
            regexp_extract('concept_set_name', 'ARIScience\s+[-–]\s+(.*?)\s*[-–]*\s*[A-Z]*$', 1)))
    
    df3.count()

    df4 = df_bundle.where(col('bundle_id').isNotNull()) \
        .withColumn('feature_source', lit('bundle_id')) \
        .withColumn('feature_id', col('bundle_id')) \
        .withColumn('feature_name', format_string('B: %s', 'bundle_name'))

    df4.count()

    df_custom_sets = custom_sets.select('concept_id', 'custom_set_id', 'custom_set_name')
    df5 = df_concept.join(df_custom_sets, on='concept_id') \
        .withColumn('feature_source', lit('custom_set_id')) \
        .withColumn('feature_id', col('custom_set_id')) \
        .withColumn('feature_name', format_string('X: %s', 'custom_set_name'))
    
    df5.count()

    dfs = [df1, df2, df3, df4, df5]
    cols = ['concept_id', 'feature_id', 'feature_name', 'feature_source']
    df_union = reduce(DataFrame.union, [d.select(cols) for d in dfs]).distinct()

    df_union.count()

    df = df_concept.join(df_union, on='concept_id', how='left')

    df = move_cols_to_front(df, ['concept_id', 'concept_name', 'domain_id', 
        'feature_id', 'feature_name', 'feature_source'])
    df = df.orderBy(col('cmi').desc())

    return df
  