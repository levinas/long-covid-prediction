def per_person_info_test(silver_test, demographics_test, person_vaccine_test, covid_episodes_test, observation_period_features_test, mvisit_features_test):

    df_person = silver_test.select('person_id')

    vaccine_cols = ['vaccine_txn', 'vax_time_to_covid']
    df_vaccine = person_vaccine_test.select('person_id', *vaccine_cols)

    episode_cols = ['num_covid_episodes', 'total_episode_length', 'max_episode_length',
        'months_from_covid_index', 'months_from_first_covid']
    df_episode = covid_episodes_test.select('person_id', *episode_cols)

    df_obsv_period = observation_period_features_test
    df_mvisit = mvisit_features_test

    df = df_person.join(demographics_test, on='person_id', how='left') \
        .join(df_vaccine, on='person_id', how='left') \
        .join(df_episode, on='person_id', how='left') \
        .join(df_obsv_period, on='person_id', how='left') \
        .join(df_mvisit, on='person_id', how='left')

    df = df.fillna(0)

    return df