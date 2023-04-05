def vis_demographics_model(vis_demographics_features):
    clf = XGBClassifier()
    # training_data_np = vis_demographics_features.drop('time_to_pasc').filter("pmod(hash(person_id), 100) < 80").drop('person_id').toPandas().to_numpy()
    training_data_np = vis_demographics_features.drop('time_to_pasc').drop('person_id').toPandas().to_numpy()
    X_train = training_data_np[:, 1:]
    y_train = training_data_np[:, 0]

    clf.fit(X_train, y_train)
    return clf