# typical runtime: 20s;  output shape: 34 x 57672

def per_person_info(silver, demographics, person_vaccine, covid_episodes, observation_period_features, mvisit_features):

    df_person = silver.select('person_id')

    df_outcome = silver.select('person_id', 'time_to_pasc') \
        .withColumn('pasc', when(col('time_to_pasc').isNull(), lit(0)).otherwise(lit(1)))

    vaccine_cols = ['vaccine_txn', 'vax_time_to_covid']
    df_vaccine = person_vaccine.select('person_id', *vaccine_cols)

    episode_cols = ['num_covid_episodes', 'total_episode_length', 'max_episode_length',
        'months_from_covid_index', 'months_from_first_covid']
    df_episode = covid_episodes.select('person_id', *episode_cols)

    df_obsv_period = observation_period_features
    df_mvisit = mvisit_features

    df = df_outcome.join(demographics, on='person_id', how='left') \
        .join(df_vaccine, on='person_id', how='left') \
        .join(df_episode, on='person_id', how='left') \
        .join(df_obsv_period, on='person_id', how='left') \
        .join(df_mvisit, on='person_id', how='left')

    df = df.fillna(0)

    return df