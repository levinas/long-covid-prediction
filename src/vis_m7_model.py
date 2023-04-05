def vis_m7_model(vis_m7_features):
    clf = LGBMClassifier()
    training_data_np = vis_m7_features.drop('time_to_pasc').filter("pmod(hash(person_id), 100) < 80").drop('person_id').toPandas().to_numpy()
    X_train = training_data_np[:, 1:]
    y_train = training_data_np[:, 0]
    clf.fit(X_train, y_train)
    return clf
