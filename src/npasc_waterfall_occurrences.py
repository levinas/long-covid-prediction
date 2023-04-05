from typing import Any


def npasc_waterfall_occurrences(vis_full_model_xgb: Any, vis_full_features, vis_full_feature_mapping):
    # shape: (1, 79)
    X = vis_full_features.filter(col('person_id')==zz_vis_nonpasc_person).drop('person_id', 'pasc').toPandas().to_numpy()
    clf = vis_full_model_xgb
    explainer = shap.Explainer(clf)
    exp = explainer(X)
    base_value = exp.base_values[0]
    shap_values = exp.values[0]

    feature_name_to_idx = {name:idx for idx, name in enumerate(vis_full_features.columns[2:])}
    # ['category', 'feature_name', 'display_name']
    mapping_list = vis_full_feature_mapping.withColumn('display_name', when(col('feature_name').isNotNull(), col('feature_name')).otherwise(col('concept_name')))\
        .select('category', 'feature', 'display_name').toPandas().to_numpy().tolist()
    feature_mapping_list = [i for i in mapping_list if i[0] in ['Visits', 'Conditions', 'COVID disgnosis']]
    selected_feature_idx_list = [feature_name_to_idx[i[1]] for i in feature_mapping_list]

    # the shap values that are not needed and put into others features, no operation needed
    unselected_mask = np.ones_like(shap_values, dtype=bool)
    unselected_mask[selected_feature_idx_list] = False
    unselected_shap_values = shap_values[unselected_mask]

    # create a dict with the selected features {'display_name_1': [feature_name_1, ...], 'display_name_2': [...], ...}
    selected_features_dict = {}
    for _, feature_name, display_name in feature_mapping_list:
        if display_name not in selected_features_dict:
            selected_features_dict[display_name] = [feature_name]
        else:
            selected_features_dict[display_name].append(feature_name)

    selected_shap_values = []
    selected_shap_values_display_names = []
    for k, v in selected_features_dict.items():
        val = 0
        for feature_name in v:
            val += shap_values[feature_name_to_idx[feature_name]]
        selected_shap_values.append(val)
        selected_shap_values_display_names.append(k)

    final_shap_values = np.concatenate([np.array(selected_shap_values), unselected_shap_values])
    
    waterfall_custom(base_value, final_shap_values, list(range(len(selected_shap_values))), selected_shap_values_display_names, show=False)
    plt.tight_layout()
    plt.show()