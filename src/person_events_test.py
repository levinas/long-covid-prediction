def person_events_test(merged_events_test, silver_test):
    df = merged_events_test \
        .join(silver_test.select('person_id', 'covid_index'), on='person_id', how='left') \
        .withColumn('covid_days', datediff(col('date'), col('covid_index'))) \
        .orderBy(col('person_id'), col('date'))
    df = move_cols_to_front(df, ['person_id', 'concept_id', 'concept_name', 'domain_id', 'covid_days'])
    return df