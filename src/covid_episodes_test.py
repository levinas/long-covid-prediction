def covid_episodes_test(covid_dates_test, 
                        silver_test):
    df = compute_covid_diagnostic_windows(covid_dates_test, silver_test)
    cols = ['person_id', 'covid_index',
        'num_covid_episodes', 'total_episode_length', 'max_episode_length',
        'months_from_covid_index', 'months_from_first_covid']
    # for i in range(1, 6):
    for i in range(1, 2):
        if f'covid_{i}_first' in df.columns:
            cols.append(f'covid_{i}_first')
            cols.append(f'covid_{i}_last')
    df = df.select(cols).distinct().orderBy('person_id')
    return df