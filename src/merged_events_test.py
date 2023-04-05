# def merged_events_test(concept, condition_occurrence_test, device_exposure_test, drug_exposure_test, observation_test, procedure_occurrence_test, note_test, note_nlp_test, visit_occurrence_test):
def merged_events_test(concept, condition_occurrence_test, drug_exposure_test, observation_test, procedure_occurrence_test, visit_occurrence_test):
    df1 = condition_occurrence_test.select('person_id', 
        col('condition_concept_id').alias('concept_id'), 
        # col('condition_concept_name').alias('concept_name'), 
        col('condition_start_date').alias('date'), 
        datediff(col('condition_end_date'), col('condition_start_date')).alias('duration'),
        # col('condition_type_concept_id').alias('type_concept_id'),
        # col('condition_type_concept_name').alias('type_concept_name')
    )

    # df2 = device_exposure_test.select('person_id', 
    #     col('device_concept_id').alias('concept_id'), 
    #     col('device_concept_name').alias('concept_name'), 
    #     col('device_exposure_start_date').alias('date'), 
    #     datediff(col('device_exposure_end_date'), col('device_exposure_start_date')).alias('duration'),
    #     col('quantity').alias('value'),
    #     col('device_type_concept_id').alias('type_concept_id'),
    #     col('device_type_concept_name').alias('type_concept_name')
    # )

    df3 = drug_exposure_test.select('person_id', 
        col('drug_concept_id').alias('concept_id'), 
        # col('drug_concept_name').alias('concept_name'), 
        col('drug_exposure_start_date').alias('date'), 
        datediff(col('drug_exposure_end_date'), col('drug_exposure_start_date')).alias('duration'),
        col('quantity').alias('value'),
        # col('drug_type_concept_id').alias('type_concept_id'),
        # col('drug_type_concept_name').alias('type_concept_name')
    )

    df4 = observation_test.select('person_id', 
        col('observation_concept_id').alias('concept_id'), 
        # col('observation_concept_name').alias('concept_name'), 
        col('observation_date').alias('date'), 
        col('value_as_number').alias('value'),
        # col('observation_type_concept_id').alias('type_concept_id'),
        # col('observation_type_concept_name').alias('type_concept_name')
    )

    df5 = procedure_occurrence_test.select('person_id', 
        col('procedure_concept_id').alias('concept_id'), 
        # col('procedure_concept_name').alias('concept_name'), 
        col('procedure_date').alias('date'), 
        col('quantity').alias('value'),
        # col('procedure_concept_id').alias('type_concept_id'),
        # col('procedure_concept_name').alias('type_concept_name')
    )

    # df6 = note_test.select('person_id', 'note_id', 'note_date', 'note_type_concept_id', 'note_type_concept_name') \
    #     .join(note_nlp_test.select('note_id', 'note_nlp_concept_id', 'note_nlp_concept_name'), on='note_id') \
    #     .select('person_id', 
    #     col('note_nlp_concept_id').alias('concept_id'), 
    #     col('note_nlp_concept_name').alias('concept_name'), 
    #     col('note_date').alias('date'), 
    #     col('note_type_concept_id').alias('type_concept_id'),
    #     col('note_type_concept_name').alias('type_concept_name')
    # )

    df7 = visit_occurrence_test.select('person_id', 
        col('visit_concept_id').alias('concept_id'), 
        # col('visit_concept_name').alias('concept_name'), 
        col('visit_start_date').alias('date'), 
        datediff(col('visit_end_date'), col('visit_start_date')).alias('duration'),
        # col('visit_concept_id').alias('type_concept_id'),
        # col('visit_concept_name').alias('type_concept_name')
    )

    df = df1
    # for d in [df2, df3, df4, df5, df6, df7]:
    for d in [df3, df4, df5, df7]:
        df = df.unionByName(d, allowMissingColumns=True)

    df = df.distinct() \
        .join(concept.select('concept_id', 'domain_id', 'concept_name'), on='concept_id', how='left') \
        .orderBy(col('person_id'), col('date'))

    df = move_cols_to_front(df, ['person_id', 'concept_id', 'concept_name', 'domain_id'])

    return df