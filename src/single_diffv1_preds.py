from typing import Any


def single_diffv1_preds(single_diffv1_features, vis_full_model: Any, vis_m7_model: Any):
    clf = vis_full_model
    clf_m7 = vis_m7_model
    df_pd = single_diffv1_features.drop('time_to_pasc').toPandas()
    persons = df_pd['person_id'].tolist()
    val_data_np = df_pd.drop(columns=['person_id']).to_numpy()
    X_val = val_data_np[:, 1:]
    y_val = val_data_np[:, 0]
    y_pred = clf.predict(X_val)
    y_score = clf.predict_proba(X_val)[:, 1]
    y_pred_m7 = clf_m7.predict(X_val)
    y_score_m7 = clf_m7.predict_proba(X_val)[:, 1]
    return spark.createDataFrame(pd.DataFrame({'time_order': [int(i.split('_')[1]) for i in persons], 'y_pred': y_pred, 'y_score': y_score, 'y_pred_m7': y_pred_m7, 'y_score_m7': y_score_m7, 'y_true': y_val}))
