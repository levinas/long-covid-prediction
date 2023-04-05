# typical runtime: 40s;  output shape: 8 x 20400301

def event_outcome(person_events, 
                  silver):
    df_events = person_events.select('person_id', 'concept_id', 'concept_name', 'domain_id', 'covid_days', 'date', 'covid_index').distinct()
    df = silver.select('person_id', 'time_to_pasc') \
        .join(df_events, on='person_id', how='left') \
        .orderBy(col('person_id'), col('date'))
    return df