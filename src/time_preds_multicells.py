def time_preds_multicells(single_diffv1_preds, single_accu_preds, single_person_event_order):
    single_diffv1_moreinfo = single_person_event_order.join(single_diffv1_preds.select('time_order', 'y_pred', 'y_score', 'y_pred_m7', 'y_score_m7', 'y_true'), on='time_order', how='left')\
        .withColumn('_coeff', F.datediff(col('date'), F.date_sub(col('covid_index'), 7))/35)\
        .withColumn('coeff', when(col('_coeff')>=0, col('_coeff')).otherwise(0))\
        .drop('_coeff')\
        .withColumn('avg_y_pred', col('coeff')*col('y_score') + (1-col('coeff'))*col('y_score_m7'))\
        .sort('time_order')
    single_accu_moreinfo = single_person_event_order.join(single_accu_preds.select('time_order', 'y_pred', 'y_score', 'y_pred_m7', 'y_score_m7', 'y_true'), on='time_order', how='left')\
        .withColumn('_coeff', F.datediff(col('date'), F.date_sub(col('covid_index'), 7))/35)\
        .withColumn('coeff', when(col('_coeff')>=0, col('_coeff')).otherwise(0))\
        .drop('_coeff')\
        .withColumn('avg_y_pred', col('coeff')*col('y_score') + (1-col('coeff'))*col('y_score_m7'))\
        .sort('time_order')

    single_diffv1_moreinfo = single_diffv1_moreinfo.select('time_order', col('y_score').alias('diffv1_y_score'), col('y_score_m7').alias('diffv1_y_score_m7'), 'y_true', 'concept_id', 'person_id', 'date', 'covid_days', 'covid_index', 'concept_name', 'domain_id', 'feature_id', 'feature_name', 'harmonized_value_as_number')
    single_accu_moreinfo = single_accu_moreinfo.select('time_order', col('y_score').alias('accu_y_score'), col('y_score_m7').alias('accu_y_score_m7'), col('avg_y_pred').alias('accu_avg_y'))
    return single_accu_moreinfo.join(single_diffv1_moreinfo, on='time_order')
