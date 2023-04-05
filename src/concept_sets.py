# typical runtime: 20s;  output shape: 10 x 4848231

def concept_sets(concept_set_members,
                 concept_bundles):
    df = concept_set_members.where(col('is_most_recent_version') & ~col('archived'))
    df = df.join(concept_bundles.select('bundle_name', 'bundle_id','codeset_id'),
                 on='codeset_id',
                 how='left')
    df_count = df.groupBy('codeset_id').count().withColumnRenamed('count', 'member_count')
    df = df.join(df_count, on='codeset_id', how='left')
    df = df.orderBy(col('version').desc(), col('codeset_id'))
    return df
