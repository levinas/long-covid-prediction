# def demographics_test(person_test, location_test):
def demographics_test(person_test):
    cols = ['year_of_birth', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id']#, 'data_partner_id']
    df = person_test.select('person_id', *cols)    

    # df_dp = data_partner_id_to_onehot(df)
    # df = df.join(df_dp, on='person_id', how='left')

    # df_loc = location_test.dropDuplicates(['location_id'])
    # df_zip = person_test.select('person_id', 'location_id') \
    #     .join(df_loc, on='location_id', how='left') \
    #     .select('person_id', 'zip') \
    #     .withColumn('zip_id', col('zip').astype(IntegerType())) \
    #     .drop('zip')

    # df = df.join(df_zip, on='person_id', how='left').fillna(0)
    df = df.fillna(0)

    return df