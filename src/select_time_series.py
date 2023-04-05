def select_time_series(single_person_event_order):   
    df = single_person_event_order.where(col('covid_days') >= -7) 

    k_cat = 20
    k_lab = 10

    df_lab = df.where(col('domain_id') == 'Measurement')
    df_cat = df.where(col('domain_id') != 'Measurement')

    n_lab = df_lab.count()
    n_cat = df_cat.count()

    if n_lab > k_lab: df_lab = df_lab.sample(k_lab / n_lab)

    if n_cat > k_cat: df_cat = df_cat.sample(k_cat / n_cat)

    df = df_lab.union(df_cat)

    return df