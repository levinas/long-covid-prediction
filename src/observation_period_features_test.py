def observation_period_features_test(observation_period_test, silver_test):
    df_end = observation_period_test.select('person_id', 'observation_period_end_date')
    df = keep_last_occurrence(df_end, 'person_id', 'observation_period_end_date')
    df = silver_test.select('person_id', 'covid_index').join(df, on='person_id', how='left')
    df = df.withColumn('months_from_observation_end', 
        F.round(months_between(to_date(lit('2023-03-31')), col('observation_period_end_date')), 1)) \
        .withColumn('covid_index_to_observation_end', 
            datediff(col('observation_period_end_date'), col('covid_index')))
    df = df.select('person_id', 'months_from_observation_end', 'covid_index_to_observation_end')
    return df