def single_diffv1_features(mini_per_person_info, ordered_single_person_concept_features_diffv1, ordered_single_measurement_diffv1):
    ordered_single_measurement_diffv1 = ordered_single_measurement_diffv1.withColumnRenamed('concept_id', 'measurement_concept_id').withColumnRenamed('date', 'measurement_date')
    return vis_model_z_calculate_features(mini_per_person_info, ordered_single_person_concept_features_diffv1, ordered_single_measurement_diffv1)
