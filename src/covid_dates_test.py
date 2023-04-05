def covid_dates_test(concept, 
                     measurement_test, 
                     merged_events_test, 
                     person_test, 
                     concept_set_members):
    df1 = covid_index_from_measurement(concept, measurement_test, person_test, concept_set_members, mark_all=True)
    df2 = covid_index_from_concepts(merged_events_test, person_test, use_custom_covid_set=True, mark_all=True)

    df = df1.unionByName(df2, allowMissingColumns=True)
    df = df.select('person_id', 'date', 
        'covid_test_positive', 'covid_concept_positive', 
        'concept_id', 'concept_name')
    df = df.orderBy('person_id', 'date')
 
    return df