def vis_m7_features(per_person_info, person_concept_features, person_labs):
    person_concept_features = person_concept_features.filter(col('date') < F.date_sub(col('covid_index'), 7))
    person_labs = person_labs.filter(col('measurement_date') < F.date_sub(col('covid_index'), 7))
    return vis_model_z_calculate_features(per_person_info, person_concept_features, person_labs)
 