def fig_global_features_beeswarm_plot(model36_start, person_concept_features):
    plt.figure(dpi=300)
    model36bsp_subset_features = model36_start.select(*['person_id', 'pasc', 'vaccine_txn', 'vax_time_to_covid', 'num_covid_episodes', 'total_episode_length', 'max_episode_length', 'months_from_covid_index', 'months_from_first_covid', 'months_from_observation_end', 'covid_index_to_observation_end', 'covid_index_to_last_visit_end', 'months_from_last_visit', 'months_from_first_visit', 'num_visits', 'max_visit_length', 'total_visit_length'])
    persons = person_concept_features.select('person_id').distinct()
    df = persons.join(model36bsp_subset_features, on='person_id', how='left')
    bsp_features = df.drop('time_to_pasc')
    clf = XGBClassifier()
    training_data_np = bsp_features.drop('person_id').toPandas().to_numpy()
    X_train = training_data_np[:, 1:]
    y_train = training_data_np[:, 0]
    clf.fit(X_train, y_train)
    explainer = shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(X_train)
    shap.summary_plot(shap_values, X_train, feature_names=bsp_features.columns[2:], max_display=40, show=False)
    plt.tight_layout()
    plt.show()