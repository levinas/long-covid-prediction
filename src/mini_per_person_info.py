from typing import Any

def mini_per_person_info(single_person_event_order, per_person_info, vis_m7_model: Any, vis_full_model: Any):
    selected_person_id = zz_vis_selected_person
    clf = vis_full_model
    clf_m7 = vis_m7_model

    ordered_single_person_concept_features = single_person_event_order.filter(col('domain_id')!='Measurement').drop('harmonized_value_as_number')
    ordered_single_measurement = single_person_event_order.filter(col('domain_id')=='Measurement')
    min_time_order = max(np.min(single_person_event_order.filter(col('date')>=F.date_sub(col('covid_index'),7)).select('time_order').toPandas().to_numpy())-1, 0)
    event_count = single_person_event_order.count()

    # code block: mini_per_person_info
    per_person_info_minified = per_person_info.select('person_id', 'pasc', 'year_of_birth', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'vaccine_txn', 'vax_time_to_covid')
    mini_per_person_info = per_person_info_minified.filter(col('person_id')==selected_person_id).drop('person_id')
    mini_per_person_info = mini_per_person_info.withColumn('explode_list', F.array([lit(f'person_{i}') for i in range(1, event_count+1)]))
    return mini_per_person_info.select('pasc', 'year_of_birth', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'vaccine_txn', 'vax_time_to_covid', F.explode('explode_list').alias('person_id'))
