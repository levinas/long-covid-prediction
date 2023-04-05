# typical runtime: 30s;  output shape: 18 x 57672

def covid_episodes(covid_dates, 
                   silver):
    df = compute_covid_diagnostic_windows(covid_dates, silver)
    cols = ['person_id', 'time_to_pasc', 'covid_index',
        'num_covid_episodes', 'total_episode_length', 'max_episode_length',
        'months_from_covid_index', 'months_from_first_covid']
    # for i in range(1, 6):
    for i in range(1, 2):
        cols.append(f'covid_{i}_first')
        cols.append(f'covid_{i}_last')
    df = silver.select('person_id', 'time_to_pasc') \
        .join(df, on='person_id', how='left')
    df = df.select(cols).distinct().orderBy('person_id')
    return df