def mvisit_features(microvisits_to_macrovisits, silver):
    df = microvisits_to_macrovisits.withColumn('visit_length', 
        datediff(col('visit_end_date'), col('visit_start_date'))) \
        .select('person_id', 'visit_start_date', 'visit_end_date', 'visit_length')        
    df_covid_index = silver.select('person_id', 'covid_index')
    df = df_covid_index.join(df, on='person_id', how='left')
    df1 = df.groupBy('person_id').agg(
        count('person_id').alias('num_visits'), 
        F.max(col('visit_length')).alias('max_visit_length'), 
        F.sum(col('visit_length')).alias('total_visit_length'))

    df_last = keep_last_occurrence(df.select('person_id', 'visit_end_date'), 
        'person_id', 'visit_end_date')
    df_last = df_covid_index.join(df_last, on='person_id', how='left')
    df_last = df_last.withColumn('covid_index_to_last_visit_end', 
        datediff(col('visit_end_date'), col('covid_index')))
    
    df_last2 = keep_last_occurrence(df.select('person_id', 'visit_start_date'), 
        'person_id', 'visit_start_date') 
    df_last2 = df_last2.withColumn('months_from_last_visit', 
            F.round(months_between(to_date(lit('2023-03-31')), col('visit_start_date')), 1))

    df_first = keep_first_occurrence(df.select('person_id', 'visit_start_date'), 
        'person_id', 'visit_start_date')
    df_first = df_first.withColumn('months_from_first_visit', 
        F.round(months_between(to_date(lit('2023-03-31')), col('visit_start_date')), 1))
    
    df = df_last.select('person_id', 'covid_index_to_last_visit_end') \
        .join(df_last2.select('person_id', 'months_from_last_visit'), on='person_id', how='left') \
        .join(df_first.select('person_id', 'months_from_first_visit'), on='person_id', how='left') \
        .join(df1, on='person_id', how='left')

    return df