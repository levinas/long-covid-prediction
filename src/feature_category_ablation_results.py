# This code does ablation experiments to analyze the impact of 7 feature cateogories.

# By default, experiments will not rerun, because with reps they can be slow.
# To reproduce the experiments, please set rerun_experiments to True.    
# The number of reps can be controlled with num_runs (default: 10).

def feature_category_ablation_results(model36_start, 
                                      concept_to_feature, 
                                      concept):

    rerun_experiments = False
    num_runs = 10

    if not rerun_experiments:
        d = {'exp_name': {0: 'superset - Measurements', 1: 'superset - Data source', 2: 'superset - Conditions', 3: 'superset - COVID diagnosis', 4: 'superset - Visits', 5: 'superset - Vaccine', 6: 'superset', 7: 'superset - Demographics', 8: 'Measurements', 9: 'Data source', 10: 'Conditions', 11: 'Demographics', 12: 'Vaccine', 13: 'COVID diagnosis', 14: 'Visits'}, 'acc_mean': {0: 0.92055937, 1: 0.91462929, 2: 0.9152084200000001, 3: 0.9194323000000001, 4: 0.9045637400000001, 5: 0.9207483600000002, 6: 0.9208818800000002, 7: 0.9202541999999999, 8: 0.8608700899999999, 9: 0.8354296699999999, 10: 0.8715095600000001, 11: 0.8011253400000001, 12: 0.7998786099999999, 13: 0.8566808799999999, 14: 0.889553}, 'acc_std': {0: 0.0002660367311732583, 1: 0.0004955981603633829, 2: 0.0003803556020357697, 3: 0.0005353748344228861, 4: 0.00020466964818677418, 5: 0.0004092857822543735, 6: 0.0004084768422648374, 7: 0.00031492907137953415, 8: 0.0003957734718244612, 9: 0.0002917991586691158, 10: 0.00030803395267407, 11: 0.00011422288348274927, 12: 0.00021097113783642523, 13: 0.0005121647018077022, 14: 0.00042252782419885805}, 'auc_mean': {0: 0.9690258399999999, 1: 0.9622930199999999, 2: 0.9653107499999999, 3: 0.96836618, 4: 0.95496094, 5: 0.96903167, 6: 0.96906897, 7: 0.96847719, 8: 0.88815094, 9: 0.8885103200000002, 10: 0.90597999, 11: 0.6904163600000001, 12: 0.53986256, 13: 0.8894611599999998, 14: 0.9377764099999999}, 'auc_std': {0: 0.00015126680769788234, 1: 0.00012966564524020531, 2: 0.00014485734937056446, 3: 0.00022007417335476377, 4: 0.00016842666191680837, 5: 0.000167557711782507, 6: 0.00015891291150955754, 7: 0.0001334239646140542, 8: 0.00023000436807447085, 9: 8.9968498190579e-05, 10: 0.0002252552303005854, 11: 0.0005865267355845858, 12: 0.0008966325423494292, 13: 0.00025131430166671145, 14: 0.00020856990541409193}}
        return spark.createDataFrame(pd.DataFrame(d))

    else: # rerun experiments to collect ablation data

        model36ab_features = model36_start.select(*['person_id', 'time_to_pasc', 'pasc', 'year_of_birth', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'data_partner_id',  'vaccine_txn', 'vax_time_to_covid', 'num_covid_episodes', 'total_episode_length', 'max_episode_length', 'months_from_covid_index', 'months_from_first_covid', 'months_from_observation_end', 'covid_index_to_observation_end', 'covid_index_to_last_visit_end', 'months_from_last_visit', 'months_from_first_visit', 'num_visits', 'max_visit_length', 'total_visit_length', '3000963_bc7_val-3000963_1w_val', '3004501_bc_val-3004501_bc7_val', '3009744_1w-3009744_2_4w', '3012888_bc7_val-3012888_1w_val', '3013650_bc_val', '3013682_bc_val', '3013762_bc_val', '3016723_bc_val', '3020891_bc7_val-3020891_1w_val', '3024128_bc_val', '40762499_bc7_val-40762499_1w_val', '706163_1w-706163_2_4w', 'b026_weight_max', 'c36714927_weight_max', 'c37016200_during_vs_before', 'c4203722_weight_sum', 'c9202_weight_max', 's122805878_weight_sum', 's231912125_weight_sum', 's348930395_weight_sum', 's400852949_weight_max', 's402442153_weight_sum', 's546116974_weight_max', 's571514014_weight_sum', 's689261095_weight_max', 's799270877_weight_sum', 's809092939_b1w', 's947341641_weight_max', 's972465851_after_vs_before', 'x003_weight_max', 'x005_weight_sum', 'x012_weight_sum', 'x013_weight_sum', 'x014_weight_max', 'x015_weight_sum', 'x016_b1w',  'd124', 'd134', 'd399',  's809092939_before',  'x005_weight_max', 'd75', 'x012_weight_max', 'b026_weight_sum', 'x013_weight_max', 'd569', '3020891_bc_val-3020891_bc7_val', '3012888_bc-3012888_bc7', 'x013_after_vs_during', '706163_bc', 's231912125_b1w', '3000963_2_4w_val', 'd294', '3004501_bc7_val-3004501_1w_val', '3000963_bc_val', 's972465851_weight_sum', 'd526', 'x016_weight_sum', 's546116974_weight_sum', '3013682_bc_val-3013682_bc7_val', '3016723_bc_val-3016723_bc7_val', '40762499_bc_val', '3013650_bc_val-3013650_bc7_val', 'd798',  '3012888_bc_val-3012888_bc7_val', 'x005_a1w_vs_b1w', 's400852949_weight_sum'])

        df_features = ml_classify_df(model36ab_features, return_features=True)
        df_features = map_feature_to_name(df_features, 'feature', concept_to_feature, concept) \
            .orderBy(col('importance').desc())
        model36ab_feature_mapping = aggregate_feature_importance_to_categories(df_features)

        categories = model36ab_feature_mapping.select('category').distinct().toPandas()['category'].tolist()
        results = {}
        results_keys = ['exp_name', 'run', 'model', 'accuracy_score', 'roc_auc_score', 'matthews_corrcoef', 'precision_score', 'recall_score', 'f1_score']
        for k in results_keys:
            results[k] = []

        # superset ifself
        for i in range(1, num_runs+1):
            scores_dict = ml_classify_df(model36ab_features, seed=i, return_spark=False)
            results['exp_name'].append(f'superset')
            results['run'].append(i)
            for k in results_keys[2:]:
                results[k].append(scores_dict[k])

        # superset - X
        for c in categories:
            new_cats = [i for i in categories if i != c]
            features = model36ab_feature_mapping.filter(col('category').isin(new_cats)).select('feature').distinct().toPandas()['feature'].tolist()
            feature_df = model36ab_features.select('person_id', 'time_to_pasc', 'pasc', *features)
            for i in range(1, num_runs+1):
                scores_dict = ml_classify_df(feature_df, seed=i, return_spark=False)
                results['exp_name'].append(f'superset - {c}')
                results['run'].append(i)
                for k in results_keys[2:]:
                    results[k].append(scores_dict[k])

        # X
        for c in categories:
            new_cats = [c]
            features = model36ab_feature_mapping.filter(col('category').isin(new_cats)).select('feature').distinct().toPandas()['feature'].tolist()
            feature_df = model36ab_features.select('person_id', 'time_to_pasc', 'pasc', *features)
            for i in range(1, num_runs+1):
                scores_dict = ml_classify_df(feature_df, seed=i, return_spark=False)
                results['exp_name'].append(f'{c}')
                results['run'].append(i)
                for k in results_keys[2:]:
                    results[k].append(scores_dict[k]) 

        print(results)
        ab_run_results = spark.createDataFrame(pd.DataFrame(results))

        df = ab_run_results.select('exp_name', 'accuracy_score', 'roc_auc_score')
        return df.groupBy('exp_name').agg(F.avg('accuracy_score').alias('acc_mean'), F.stddev('accuracy_score').alias('acc_std'), F.avg('roc_auc_score').alias('auc_mean'), F.stddev('roc_auc_score').alias('auc_std'))
