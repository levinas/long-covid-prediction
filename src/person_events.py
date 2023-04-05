# typical runtime: 20s;  output shape: 11 x 21443038

def person_events(merged_events, silver):
    df = merged_events \
        .join(silver.select('person_id', 'covid_index'), on='person_id', how='left') \
        .withColumn('covid_days', datediff(col('date'), col('covid_index'))) \
        .orderBy(col('person_id'), col('date'))
    df = move_cols_to_front(df, ['person_id', 'concept_id', 'concept_name', 'domain_id', 'covid_days'])
    return df