from typing import Any


def ordered_single_person_concept_features_diffv1(single_person_event_order, per_person_info, vis_m7_model: Any, vis_full_model: Any, select_time_series):
    selected_person_id = zz_vis_selected_person

    ordered_single_person_concept_features = single_person_event_order.filter(col('domain_id')!='Measurement').drop('harmonized_value_as_number')
    ordered_single_measurement = single_person_event_order.filter(col('domain_id')=='Measurement')
    min_time_order = max(np.min(single_person_event_order.filter(col('date')>=F.date_sub(col('covid_index'),7)).select('time_order').toPandas().to_numpy())-1, 0).item()
    event_count = single_person_event_order.count()

    # code block: ordered_single_person_concept_features_diffv1
    time_order_list = [i for i in select_time_series.select('time_order').toPandas().to_numpy().flatten().tolist() if i >= min_time_order]
    start_idx = time_order_list[0]
    cid_c = ordered_single_person_concept_features.filter(col('time_order')==start_idx).select('concept_id', 'date').collect()
    if cid_c:
        cid = cid_c[0][0]
        date = cid_c[0][1]
        df = ordered_single_person_concept_features.filter(~((col('date')==date) & (col('concept_id')==cid))).withColumn('fake_pid', lit(f'person_{start_idx}'))
    else:
        df = ordered_single_person_concept_features.filter(col('time_order')!=start_idx).withColumn('fake_pid', lit(f'person_1'))
    for i in time_order_list[1:]:
        cid_c = ordered_single_person_concept_features.filter(col('time_order')==i).select('concept_id', 'date').collect()
        if cid_c:
            cid = cid_c[0][0]
            date = cid_c[0][1]
            df_tmp = ordered_single_person_concept_features.filter(~((col('date')==date) & (col('concept_id')==cid))).withColumn('fake_pid', lit(f'person_{i}'))
        else:
            df_tmp = ordered_single_person_concept_features.filter(col('time_order')!=i).withColumn('fake_pid', lit(f'person_{i}'))
        df = df.unionByName(df_tmp)
    return df.drop('person_id').withColumnRenamed('fake_pid', 'person_id')