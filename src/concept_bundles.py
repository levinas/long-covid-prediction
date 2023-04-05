def concept_bundles(raw_concept_bundles,
                    concept_set_members,
                    curated_bundles):
    df1 = raw_concept_bundles.select(
        'tag_name',
        col('concept_set_name').alias('raw_concept_set_name'),
        col('best_version_id'))
    df2 = concept_set_members.drop('concept_id', 'concept_name').distinct()
    df2_current = df2.where(col('is_most_recent_version') & ~col('archived'))
    df = df1.join(df2, df1.best_version_id == df2.codeset_id, how='left')
    df_outdated = df.where(~col('is_most_recent_version') | col('archived')) \
        .drop('is_most_recent_version', 'archived') \
        .select('tag_name', 'raw_concept_set_name', 'concept_set_name',
            col('codeset_id').alias('old_codeset_id'),
            col('version').alias('old_version'))
    df_current = df2_current.join(df.select('tag_name', 'codeset_id'),
                                  on='codeset_id')
    df_updated = df_outdated.join(df2_current, on='concept_set_name')
    cols = [
        'tag_name', 'codeset_id', 'concept_set_name', 'version',
        'is_most_recent_version', 'archived'
    ]
    df = df_current.select(cols).union(df_updated.select(cols)) \
        .withColumnRenamed('tag_name', 'bundle_name') \
        .join(curated_bundles, on='bundle_name', how='left') \
        .orderBy('bundle_name', 'concept_set_name')
    return df
