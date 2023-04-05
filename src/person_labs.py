# typical runtime: 50s;  output shape: 9 x 32569723

def person_labs(measurement, silver):
    df = measurement \
        .join(silver.select('person_id', 'covid_index'), on='person_id', how='left') \
        .withColumn('covid_days', datediff(col('measurement_date'), col('covid_index'))) \
        .orderBy(col('person_id'), col('measurement_date'))
    # df = df.select('person_id', 'measurement_date', 'covid_days', 'measurement_concept_id', 'measurement_concept_name', 'harmonized_value_as_number', 'value_as_concept_id', 'value_as_concept_name', 'covid_index')
    df = df.select('person_id', 'measurement_date', 'covid_days', 'measurement_concept_id', 'value_as_concept_id', 'value_as_number', 'covid_index')
    return df