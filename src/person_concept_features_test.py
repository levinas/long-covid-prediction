def person_concept_features_test(person_events_test, concept_to_feature):
    df = person_events_test.select('person_id', 'concept_id', 'date', 'covid_days', 'covid_index').distinct()
    df_feature = concept_to_feature.select('concept_id', 'concept_name', 'domain_id',
        'feature_id', 'feature_name', 'feature_source', 'mutual_information', 'cmi')
    df = df.join(df_feature, on='concept_id', how='left') \
        .orderBy('person_id', 'date', 'mutual_information')
    return df