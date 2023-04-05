def train_test_model(model100_v1_data, model100_v2_data, model36_data, model_z_data, silver, model100_v1_test, model100_v2_test, model36_test, model_z_test, silver_test):
    
    production_run = True

    if production_run:
        df_model100_v1 = model100_v1_data
        df_model100_v2 = model100_v2_data
        df_model36 = model36_data
        df_model_z = model_z_data
        df_model100_v1_test = model100_v1_test
        df_model100_v2_test = model100_v2_test
        df_model36_test = model36_test
        df_model_z_test = model_z_test
    else:
        print('Dry run with 80/20 split for train/test.')
        df_train_person, df_test_person = person_split_by_hash(model_z_data) 
        df_model100_v1 = model100_v1_data.join(df_train_person, on='person_id')
        df_model100_v2 = model100_v2_data.join(df_train_person, on='person_id')
        df_model36 = model36_data.join(df_train_person, on='person_id')
        df_model_z = model_z_data.join(df_train_person, on='person_id')
        df_model100_v1_test = model100_v1_data.join(df_test_person, on='person_id')
        df_model100_v2_test = model100_v2_data.join(df_test_person, on='person_id')
        df_model36_test = model36_data.join(df_test_person, on='person_id')
        df_model_z_test = model_z_data.join(df_test_person, on='person_id')

    print('Number of training samples:', df_model_z.count())
    print('Number of testing samples:', df_model_z_test.count())

    model100_v1 = ml_train_classifier(df_model100_v1, 'LGBM')
    model100_v2 = ml_train_classifier(df_model100_v2, 'LGBM', seed=2023)
    model36 = ml_train_classifier(df_model36, 'LGBM')
    # model_z = ml_train_classifier(df_model_z, 'XGB')
    model_z = ml_train_classifier(df_model_z, 'LGBM')

    preds_model100_v1 = ml_run_classifier(df_model100_v1_test, model100_v1, out='model100_v1')    
    preds_model100_v2 = ml_run_classifier(df_model100_v2_test, model100_v2, out='model100_v2')    
    preds_model36 = ml_run_classifier(df_model36_test, model36, out='model36')
    preds_model_z = ml_run_classifier(df_model_z_test, model_z, out='model_z')

    if production_run:
        df_test = silver_test.select('person_id')
    else:
        df_test = df_model_z_test.select('person_id')
        df_y = silver.select('person_id', 'time_to_pasc') \
            .withColumn('pasc', when(col('time_to_pasc').isNull(), lit(0)).otherwise(lit(1)))
        df_test = df_test.join(df_y, on='person_id').drop('time_to_pasc')

    # merge in pandas to avoid slowdown 
    df = df_test.toPandas() \
        .merge(preds_model100_v1, on='person_id', how='left') \
        .merge(preds_model100_v2, on='person_id', how='left') \
        .merge(preds_model36, on='person_id', how='left') \
        .merge(preds_model_z, on='person_id', how='left')

    df['model100'] = df['model100_v1'] * 0.5 + df['model100_v2'] * 0.5 
    df['outcome_likelihoods'] = df['model100'] * 0.8 + df['model36'] * 0.15 + df['model_z'] * 0.05
    df['ensemble3'] = df['model100_v1'] * 0.3 + df['model100_v2'] * 0.5 + df['model36'] * 0.2
    
    # if the test data contains 0, there might have been issue with running the test transformations
    print('Check null values:', df.isnull().sum().sum())  
    # df = df.fillna(0.2)  # in case we missed any person

    if not production_run:
        scores = ml_eval_prediction(df, 'model100_v1')
        scores = ml_eval_prediction(df, 'model100_v2')
        scores = ml_eval_prediction(df, 'model100')
        scores = ml_eval_prediction(df, 'model36')
        scores = ml_eval_prediction(df, 'model_z')
        scores = ml_eval_prediction(df, 'ensemble3')
        scores = ml_eval_prediction(df, 'outcome_likelihoods')

    # time bracket prediction: 3-month and 6-month PASC risk
    
    df_model_t3m = df_model36.withColumn('pasc_3month', 
        when((col('pasc') == 1) & (col('time_to_pasc') <= 92), lit(1)).otherwise(lit(0)))

    df_model_t6m = df_model36.withColumn('pasc_6month', 
        when((col('pasc') == 1) & (col('time_to_pasc') <= 183), lit(1)).otherwise(lit(0)))

    model_t3m = ml_train_classifier(df_model_t3m, 'LGBM', y_col='pasc_3month')
    model_t6m = ml_train_classifier(df_model_t6m, 'LGBM', y_col='pasc_6month')

    preds_model_t3m = ml_run_classifier(df_model36_test, model_t3m, out='model_t_3month')
    preds_model_t6m = ml_run_classifier(df_model36_test, model_t6m, out='model_t_6month')

    df = df \
        .merge(preds_model_t3m, on='person_id', how='left') \
        .merge(preds_model_t6m, on='person_id', how='left')

    display_cols = ['person_id', 'outcome_likelihoods']
    extra_cols = ['model100', 'model36', 'model_z', 'model_t_3month', 'model_t_6month'] 
    if not production_run:
        display_cols.append('pasc')

    df = df[display_cols + extra_cols]

    return df