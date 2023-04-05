from typing import Any


def single_accu_features(single_person_event_order, per_person_info, vis_m7_model: Any, vis_full_model: Any, mini_per_person_info):
    selected_person_id = zz_vis_selected_person

    ordered_single_person_concept_features = single_person_event_order.filter(col('domain_id')!='Measurement').drop('harmonized_value_as_number')
    ordered_single_measurement = single_person_event_order.filter(col('domain_id')=='Measurement')
    min_time_order = max(np.min(single_person_event_order.filter(col('date')>=F.date_sub(col('covid_index'),7)).select('time_order').toPandas().to_numpy())-1, 0).item()
    event_count = single_person_event_order.count()

    selected_time_order_accu = sorted(single_person_event_order.groupBy('covid_days').agg(F.max(col('time_order'))).toPandas().to_numpy().tolist(),key=lambda x:x[0])
    init_idx = 0
    for idx, (d, i) in enumerate(selected_time_order_accu):
        if d >= -7:
            init_idx = max(idx-1, 0)
            break
    selected_time_order_accu = [j for i, j in selected_time_order_accu[init_idx:]]
    # print(selected_time_order_accu)

    # new code attempted
    df_orig_columns = single_person_event_order.columns
    df = single_person_event_order.withColumn('_range', F.sequence(col("time_order"), lit(event_count)))\
        .withColumn('_selected_time_order', F.array([lit(i) for i in selected_time_order_accu]))\
        .withColumn('_intersect', F.array_intersect(col('_range'), col('_selected_time_order')))\
        .drop('_range', '_selected_time_order')

    df = df.select(*df.columns, F.explode('_intersect').alias('_fake_pid'))\
        .withColumn('person_id', F.format_string('person_%d', col('_fake_pid')))\
        .drop('_fake_pid', '_intersect')

    df_concepts = df.filter(col('domain_id')!='Measurement')
    df_labs = df.filter(col('domain_id')=='Measurement')

    df_labs = df_labs.withColumnRenamed('concept_id', 'measurement_concept_id').withColumnRenamed('date', 'measurement_date')
    return vis_model_z_calculate_features(mini_per_person_info, df_concepts, df_labs)
