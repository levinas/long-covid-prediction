# Key columns in this submission:
#   person_id            
#   outcome_likelihoods:  final prediction on patient PASC probability 
#   confidence_estimate:  proxy quality estimate based on data completeness
#   likelihood_3month:    predicted probability of PASC within 3 months after COVID index
#   likelihood_6month:    predicted probability of PASC within 6 months after COVID index

# Additional columns:
#   model100_pred:        prediction of Model_100 with 100 temporal features
#   model36_pred:         prediction of Model_36, a simple model with 36 temporal features
#   model_z_pred:         prediction of Model_Z, an aspiring "zero-bias" model


import pandas as pd

def Convalesco_predictions(train_test_model: pd.DataFrame, 
                           person_data_completeness_test):
    df = spark.createDataFrame(train_test_model)

    # add confidence estimate
    df_quality = person_data_completeness_test \
        .select('person_id', 'completeness_score') \
        .join(df.select('person_id'), on='person_id', how='right') \
        .fillna(0)            

    df = df.join(df_quality, on='person_id', how='left')
    
    # round numbers for better display
    df = df.select('person_id',
        F.round(col('outcome_likelihoods'), 8).alias('outcome_likelihoods'),
        F.round(col('completeness_score'), 3).alias('confidence_estimate'),
        F.round(col('model_t_3month'), 6).alias('likelihood_3month'),
        F.round(col('model_t_6month'), 6).alias('likelihood_6month'),
        F.round(col('model100'), 6).alias('model100_pred'),
        F.round(col('model36'), 6).alias('model36_pred'),
        F.round(col('model_z'), 6).alias('model_z_pred'),
    )

    return df
