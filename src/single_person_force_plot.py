from typing import Any


def single_person_force_plot(vis_demographics_model: Any, per_person_info):
    selected_person_id = zz_vis_selected_person
    clf = vis_demographics_model
    per_person_info_minified = per_person_info.select('person_id', 'pasc', 'year_of_birth', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'vaccine_txn', 'vax_time_to_covid')
    data_full = per_person_info_minified.drop('time_to_pasc').drop('person_id').toPandas().to_numpy()
    X_full = data_full[:, 1:]
    data = per_person_info_minified.filter(col('person_id')==selected_person_id).drop('time_to_pasc').drop('person_id').toPandas().to_numpy()
    X = data[:, 1:]
    explainer = shap.TreeExplainer(clf, data=X_full, model_output="probability", feature_perturbation='interventional')
    expected_value = explainer.expected_value
    shap_values = explainer.shap_values(X)
    print(expected_value)
    print(shap_values.tolist())
    print(per_person_info_minified.columns[2:])
    shap.force_plot(expected_value, shap_values, per_person_info_minified.columns[2:], matplotlib=True, show=False, figsize=(20, 3))
    plt.tight_layout()
    plt.show()