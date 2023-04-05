def vis_demographics_features(model_z_start):
    return model_z_start.select('person_id','pasc', 'year_of_birth', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'vaccine_txn', 'vax_time_to_covid')
