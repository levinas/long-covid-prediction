from typing import Any


def pasc_waterfall_demographics(vis_full_model_xgb: Any, vis_full_features, vis_full_feature_mapping):
    # shape: (1, 79)
    X = vis_full_features.filter(col('person_id')==zz_vis_selected_person).drop('person_id', 'pasc').toPandas().to_numpy()
    clf = vis_full_model_xgb
    explainer = shap.Explainer(clf)
    exp = explainer(X)
    
    feature_data = X[0].tolist()
    feature_data[1] = gender_cid_to_name[str(int(feature_data[1]))]
    feature_data[2] = race_cid_to_name[str(int(feature_data[2]))]
    feature_data[3] = ethnicity_cid_to_name[str(int(feature_data[3]))]

    waterfall_custom(exp.base_values[0], exp.values[0], [0, 1, 2, 3, 4, 5], ['Year of Birth', 'Gender', 'Race', 'Ethnicity', 'Number of Vaccines', 'Vaccine Time to COVID'], feature_data, show=False)
    plt.tight_layout()
    plt.show()