def person_vaccine_test(person_test, vaccine_test, silver_test):
    df_vax = vaccine_test.withColumn('last_vax_date',
        when(col('4_vax_date').isNotNull(), col('4_vax_date')) \
        .when(col('3_vax_date').isNotNull(), col('3_vax_date')) \
        .when(col('2_vax_date').isNotNull(), col('2_vax_date')) \
        .otherwise(col('1_vax_date')))
    df = person_test.join(silver_test.select('person_id', 'covid_index'), on='person_id') \
        .join(df_vax.select('person_id', 'vaccine_txn', 'last_vax_date'), on='person_id', how='left') \
        .withColumn('vax_time_to_covid', datediff(col('covid_index'), col('last_vax_date')))
    df = move_cols_to_front(df, ['person_id', 'vaccine_txn', 'vax_time_to_covid'])
    return df