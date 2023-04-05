def single_person_event_order(person_concept_features, person_labs):
    sel_measurements = list(set(zz_vis_sel_lab_count_ids+zz_vis_sel_lab_value_ids))

    single_person_concept_features = person_concept_features.filter(col('person_id')==zz_vis_selected_person)\
        .dropDuplicates(['feature_id', 'date', 'concept_id'])\
        .filter(col('feature_id').isin(zz_vis_sel_cat_ids))
    single_person_labs = person_labs.filter(col('person_id')==zz_vis_selected_person)\
        .filter(col('measurement_concept_id').isin(sel_measurements))\
        .select(col('measurement_concept_id').alias('concept_id'), col('measurement_concept_id').alias('feature_id'), col('measurement_date').alias('date'), col('measurement_concept_name').alias('concept_name'), col('measurement_concept_name').alias('feature_name'), 'harmonized_value_as_number', 'covid_index')\
        .withColumn('domain_id', lit('Measurement'))\
        .withColumn('covid_days', F.datediff(col('date'), col('covid_index')))\
        .withColumn('person_id', lit(zz_vis_selected_person))

    df = single_person_concept_features.unionByName(single_person_labs, allowMissingColumns=True).drop('mutual_information', 'cmi', 'feature_source')
    df = df.withColumn('time_order', F.row_number().over(Window.partitionBy().orderBy(col('date'))))
    return df
