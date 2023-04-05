def person_data_completeness(person_events, person_labs):
    cat_domains = ['Condition', 'Drug', 'Procedure', 'Visit', 'Observation']
    df_cat = person_events.groupBy('person_id').pivot('domain_id', cat_domains).agg(count('date'))

    lab_domains = ['Measurement']
    df_lab = person_labs.groupBy('person_id').agg(countDistinct('measurement_date').alias('Measurement'))

    df = df_cat.join(df_lab, on='person_id', how='outer').fillna(0)

    tau = {   # analogous to time constants
        'Condition': 80,
        'Visit': 20,
        'Drug': 10,
        'Procedure': 10,
        'Observation': 10, 
        'Measurement': 10,  # on distinct days
    }

    for d in lab_domains + cat_domains:
        df = df.withColumn(d+'_score', F.round(1 - F.exp(-col(d) / tau[d]), 3))

    df = df.withColumn('completeness_score', F.round(
        col('Condition_score') * 0.3 + col('Measurement_score') * 0.2 
        + col('Visit_score') * 0.15 + col('Drug_score') * 0.125 
        + col('Procedure_score') * 0.1 + col('Observation_score') * 0.1, 3))
        
    df = move_cols_to_front(df, ['person_id', 'completeness_score'])

    return df 