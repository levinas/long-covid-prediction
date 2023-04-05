# typical runtime: 2m;  output shape: 14 x 46437

def event_stats(event_outcome):
    df = event_outcome
    cid_covid = 37311061
    cid_cough = 254761
    cid_dyspnea = 312437
    cid_pneumonia = 3661408
    cid_emergency = 2514437

    df_mi = compute_mutual_information(df, exclude_null_rows=True)
    cmi1 = compute_conditional_mutual_information(df, [cid_covid]) \
        .select('concept_id', col('cmi').alias('cmi1'))
    cmi2 = compute_conditional_mutual_information(df, [cid_covid, cid_cough]) \
        .select('concept_id', col('cmi').alias('cmi2'))
    cmi3 = compute_conditional_mutual_information(df, [cid_covid, cid_dyspnea]) \
        .select('concept_id', col('cmi').alias('cmi3'))
    cmi4 = compute_conditional_mutual_information(df, [cid_covid, cid_pneumonia]) \
        .select('concept_id', col('cmi').alias('cmi4'))
    cmi5 = compute_conditional_mutual_information(df, [cid_emergency]) \
        .select('concept_id', col('cmi').alias('cmi5'))

    ret = df_mi
    for df_cmi in [cmi1, cmi2, cmi3, cmi4, cmi5]: 
        ret = ret.join(df_cmi, on='concept_id', how='left')
    ret = ret.fillna(0)

    ret = ret.withColumn('cmi', F.round(expr('(cmi1 + cmi2 + cmi3 + cmi4 + cmi5) / 5'), 7)) 
    ret = ret.orderBy(col('cmi').desc())
    ret = move_cols_to_front(ret, 
        ['concept_id', 'concept_name', 'domain_id', 'mutual_information', 
        'cmi', 'cmi1', 'cmi2', 'cmi3', 'cmi4', 'cmi5'])

    return ret