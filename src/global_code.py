import re

import numpy as np
import pandas as pd

from datetime import datetime
from functools import reduce
from io import StringIO

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import avg, col, concat, count, countDistinct, datediff, expr, first, format_string, lag, last, least, lit, log2, mean, months_between, regexp_extract, row_number, stddev, to_date, variance, when

from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GroupKFold, StratifiedKFold

from lightgbm import LGBMClassifier
from xgboost import XGBClassifier

import shap
import matplotlib as mpl
from matplotlib import pyplot as plt


import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

def move_cols_to_front(df, cols_to_front):
    original = df.columns
    cols_to_front = [c for c in cols_to_front if c in original]
    cols_other = [c for c in original if c not in cols_to_front]
    df = df.select(*cols_to_front, *cols_other)
    return df

def pandas_move_cols_to_front(df, cols_to_front):
    original = df.columns
    cols_to_front = [c for c in cols_to_front if c in original]
    cols_other = [c for c in original if c not in cols_to_front]
    df = df[cols_to_front + cols_other]
    return df

def rename_cols_with_postfix(df, cols, postfix):
    return df.select(*[col(c).alias(c+'_'+postfix) if c in cols else c for c in df.columns])

def keep_one_occurrence(df, feature_columns, date_column, first=False):
    if not isinstance(feature_columns, list):
        feature_columns = [feature_columns]
    order = col(date_column).asc() if first else col(date_column).desc()
    window = Window.partitionBy(*feature_columns).orderBy(order)
    df = df.withColumn('row_number', row_number().over(window)) \
        .where(col('row_number') == 1).drop('row_number')
    return df

def keep_last_occurrence(df, feature_columns, date_column):
    return keep_one_occurrence(df, feature_columns, date_column, first=False)

def keep_first_occurrence(df, feature_columns, date_column):
    return keep_one_occurrence(df, feature_columns, date_column, first=True)

def compute_mutual_information(df, cid='concept_id', outcome='time_to_pasc', keeps=['concept_name', 'domain_id'], exclude_null_rows=True):
    if exclude_null_rows: # col(cid).isNull()
        df = df.where(col(cid).isNotNull())

    df_all = df.groupBy(cid) \
        .agg(count('person_id').alias('count_all'),
            countDistinct('person_id').alias('count_all_distinct_person'))
    df_pos = df.where(col(outcome).isNotNull() & (col(outcome) > 0)).groupBy(cid) \
        .agg(count('person_id').alias('count_pos'),
            countDistinct('person_id').alias('count_pos_distinct_person'))

    count_cols = ['count_all', 'count_pos', 'count_all_distinct_person', 'count_pos_distinct_person']
    stats = df_all.join(df_pos, on=cid, how='left') \
        .fillna(0, subset=count_cols) \
        .join(df.select(cid, *keeps).distinct(), on=cid, how='left')

    n = df.select('person_id').distinct().count()
    ny1 = df.where(col(outcome).isNotNull() & (col(outcome) > 0)).select('person_id').distinct().count()

    if n == 0:
        n = 1

    py1 = ny1 / n
    py0 = 1 - py1
    # print(n, ny1, py1, py0)

    stats = stats.withColumn('px1', col('count_all_distinct_person')/n) \
        .withColumn('px0', 1 - col('px1')) \
        .withColumn('px1y1', col('count_pos_distinct_person')/n) \
        .withColumn('px1y0', (col('count_all_distinct_person') - col('count_pos_distinct_person'))/n) \
        .withColumn('px0y1', (ny1 - col('count_pos_distinct_person'))/n) \
        .withColumn('px0y0', (n - col('count_all_distinct_person') + col('count_pos_distinct_person') - ny1)/n) \
        .withColumn('mutual_information', 
            col('px1y1') * log2(col('px1y1') / (col('px1')*py1) + 1e-9) + \
            col('px1y0') * log2(col('px1y0') / (col('px1')*py0) + 1e-9) + \
            col('px0y1') * log2(col('px0y1') / (col('px0')*py1) + 1e-9) + \
            col('px0y0') * log2(col('px0y0') / (col('px0')*py0) + 1e-9) )
    
    ret = stats.select(cid, *keeps, 
        F.round('mutual_information', 7).alias('mutual_information'), 
        *count_cols) \
        .orderBy(col('mutual_information').desc())
    ret = ret.limit(ret.count()) # force sort

    return ret

def compute_conditional_mutual_information(df, condition_id_list, cid='concept_id', outcome='time_to_pasc', keeps=['concept_name', 'domain_id'], exclude_null_rows=True):
    df_sub = df.groupBy('person_id').pivot('concept_id', condition_id_list).count()
    filter_expr = ' AND '.join([f'(`{x}` > 0)' for x in condition_id_list])
    df_sub = df_sub.where(expr(filter_expr)).select('person_id')
    df = df.join(df_sub.select('person_id'), on='person_id')
    stats = compute_mutual_information(df, cid, outcome, keeps, exclude_null_rows)
    ret = stats.select(cid, *keeps, 
        F.round('mutual_information', 7).alias('cmi'))
    return ret

def log_feature_list(df, list_name, cid='feature_id', cname='feature_name', score='group_score'):
    df = df.select(cid, cname, score).toPandas()
    print('#', list_name+':', len(df))
    print(list_name+'_ids = [')
    for index, row in df.iterrows():
        print("    '{:s}', {} # {}  ({})".format(row[cid], 
            ' '*(10-len(row[cid])), row[cname], row[score]))
    print(']\n')

# curation of concept sets and transformations for predefined tables

def get_broad_covid_diagnosis_set():
    covid_set = [
        37311061, # COVID-19
        704996,   # Patient meets COVID-19 laboratory diagnostic criteria
        3661408,  # Pneumonia caused by SARS-CoV-2
        45756093, # Emergency use of U07.1 | COVID-19, virus identified
        4100065,  # Disease due to Coronaviridae
        40479642, # Pneumonia due to Severe acute respiratory syndrome coronavirus
        439676,   # Coronavirus infection
        3655976,  # Acute hypoxemic respiratory failure due to disease caused by Severe acute respiratory syndrome coronavirus 2
        37310286, # Infection of upper respiratory tract caused by Severe acute respiratory syndrome coronavirus 2
        3655975,  # Sepsis due to disease caused by Severe acute respiratory syndrome coronavirus 2
        3661632,  # Thrombocytopenia due to Severe acute respiratory syndrome coronavirus 2
        3661406,  # Acute respiratory distress syndrome due to disease caused by Severe acute respiratory syndrome coronavirus 2
    ]
    return covid_set

def covid_index_from_measurement(concept, measurement, person_table, concept_set_members, use_latest=False, mark_all=False, join_person_table=False):
    if use_latest:
        test_codeset_id = 386776576
        pos_codeset_id = 23400628    # most recent, includes 'Abnormal'
    else: # replicates covid_index in silver standards
        test_codeset_id = 651620200  # not the latest, does not include 36032419
        pos_codeset_id = 400691529   # not the latest, does not include 'Abnormal'
    covid_concept_id_list = concept_set_members \
        .where(col("codeset_id") == test_codeset_id) \
        .select('concept_id').toPandas()['concept_id'].tolist()
    positive_concept_id_list = concept_set_members \
        .where(col("codeset_id") == pos_codeset_id) \
        .select('concept_id').toPandas()['concept_id'].tolist()
    df = measurement \
        .select('person_id', 'measurement_concept_id', # 'measurement_concept_name',
                'measurement_date', 'value_as_concept_id') \
        .where((col('measurement_concept_id').isin(covid_concept_id_list)) \
            & (col('value_as_concept_id').isin(positive_concept_id_list)))
    if not mark_all:
        df = df.orderBy(col('measurement_date').asc()) \
            .dropDuplicates(subset=['person_id']) \
            .select('person_id', col('measurement_date').alias('covid_index'))
    else: 
        df = df.select('person_id', col('measurement_date').alias('date'), 
                col('measurement_concept_id').alias('concept_id'), 
                # col('measurement_concept_name').alias('concept_name'),
                ) \
            .withColumn('covid_test_positive', lit(1))
    if join_person_table:
        df = person_table.join(df, on='person_id', how='left')
    df = df.join(concept.select('concept_id', 'domain_id', 'concept_name'), on='concept_id', how='left')
    return df

def covid_index_from_concepts(merged_concepts, person_table, use_custom_covid_set=False, mark_all=False, join_person_table=False):
    covid_set = [37311061]
    if use_custom_covid_set:
        covid_set = get_broad_covid_diagnosis_set()        
    df = merged_concepts \
        .select('person_id', 'concept_id', 'concept_name', 'date') \
        .where(col('concept_id').isin(covid_set))
    if not mark_all:
        df = df.orderBy(col('date').asc()) \
        .dropDuplicates(subset=['person_id']) \
        .select('person_id', col('date').alias('concept_covid_index'))
    else:
        df = df.select('person_id', 'date', 'concept_id', 'concept_name') \
            .withColumn('covid_concept_positive', lit(1))
    if join_person_table:
        df = person_table.join(df, on='person_id', how='left')
    return df

def covid_index_from_condition(condition_occurrence, person_table, use_custom_covid_set=False, mark_all=False, join_person_table=False):
    covid_set = [37311061]
    if use_custom_covid_set:
        covid_set = get_broad_covid_diagnosis_set()        
    df = condition_occurrence \
        .select('person_id', 'condition_concept_id', 'condition_start_date') \
        .where(col('condition_concept_id').isin(covid_set))
    if not mark_all:
        df = df.orderBy(col('condition_start_date').asc()) \
        .dropDuplicates(subset=['person_id']) \
        .select('person_id', col('condition_start_date').alias('condition_covid_index'))
    if join_person_table:
        df = person_table.join(df, on='person_id', how='left')
    return df

def create_or_add_to_custom_set(df, custom_set=None):
    if custom_set is None:
        return df
    return custom_set.union(df)

def custom_concept_set_from_existing_set(concept_set_members, custom_set_name, custom_set_id, existing_set_name, custom_set=None):
    df = concept_set_members.select('concept_id', 'concept_name', 'concept_set_name') \
        .where(col('concept_set_name') == existing_set_name).drop('concept_set_name') \
        .withColumn('custom_set_name', lit(custom_set_name)) \
        .withColumn('custom_set_id', lit(custom_set_id))
    return create_or_add_to_custom_set(df, custom_set)

def custom_concept_set_with_ids(concept_table, set_name, set_id, concept_id_list, custom_set=None):
    df = concept_table.select('concept_id', 'concept_name')
    df = df.where(col('concept_id').isin(concept_id_list)) \
        .withColumn('custom_set_name', lit(set_name)) \
        .withColumn('custom_set_id', lit(set_id))
    return create_or_add_to_custom_set(df, custom_set)

def custom_concept_set_with_names(concept_table, set_name, set_id, concept_name_list, custom_set=None):
    df = concept_table.select('concept_id', 'concept_name')
    df = df.where(col('concept_name').isin(concept_name_list)) \
        .withColumn('custom_set_name', lit(set_name)) \
        .withColumn('custom_set_id', lit(set_id))
    return create_or_add_to_custom_set(df, custom_set)

def custom_concept_set_with_names_containing(concept_table, set_name, set_id, pattern, custom_set=None):
    df = concept_table.select('concept_id', 'concept_name')
    df = df.where(col('concept_name').contains(pattern)) \
        .withColumn('custom_set_name', lit(set_name)) \
        .withColumn('custom_set_id', lit(set_id))
    return create_or_add_to_custom_set(df, custom_set)

def custom_concept_set_with_names_starting_with(concept_table, set_name, set_id, pattern, custom_set=None):
    df = concept_table.select('concept_id', 'concept_name')
    df = df.where(col('concept_name').startswith(pattern)) \
        .withColumn('custom_set_name', lit(set_name)) \
        .withColumn('custom_set_id', lit(set_id))
    return create_or_add_to_custom_set(df, custom_set)

def custom_concept_set_with_names_rlike(concept_table, set_name, set_id, pattern, custom_set=None, case_sensitive=False):
    case = '' if case_sensitive else '(?i)'
    df = concept_table.select('concept_id', 'concept_name')
    df = df.where(col('concept_name').rlike(case+pattern)) \
        .withColumn('custom_set_name', lit(set_name)) \
        .withColumn('custom_set_id', lit(set_id))
    return create_or_add_to_custom_set(df, custom_set)

def filter_concept_set_on_domain(df, concept_table, excludes=None, includes=None):
    has_domain = 'domain_id' in df.columns
    if not has_domain:
        df_concept = concept_table.select('concept_id', 'domain_id')
        df = df.join(df_concept, on='concept_id', how='left')
    if excludes is not None:
        df = df.where(~col('domain_id').isin(excludes))
    if includes is not None:
        df = df.where(col('domain_id').isin(includes))
    if not has_domain:
        df = df.drop('domain_id')
    return df

# curation of concept sets

def get_selected_concept_set_names():
    selected_set_names = ['covid19diagnosis', 'SS_EKG_1870024', 
        'Computed Tomography (CT) Scan',
        'Critical Care [oneils]', 
        '[COVID19 Dx] Weak positive dx', #'Weak Positive (SNOMED codes)',
        'Long COVID Clinic Visit', 
        'UVA Hyperlipidemia', 'Metabolic Disorders', 
        'LWW_remdesivir', '[DM] Aspirin', 'Anesthesia Medications (SIANES)',
        'Immunosuppression L04', 'Systemic Antibiotics', '[DATOS] Diuretics', 
        '[PASC] Vasopressors', '[PASC] Antidepressant', '[RHDT] Vitamin D',
        '[RP-6B45AE]Body/muscle pain and aches', 'pain-72', 'gestation', 
        'O2 device', 'RHDT - Supplemental Oxygen',
        '[Cardioonc] Liquid Malignancies', 'Lung Cancer', 
        'Breast Cancer', 'Colorectal Cancer', 'UC - prostate cancer',
        '[DM]Type1 Diabetes Mellitus', '[DM]Type2 Diabetes Mellitus', 
        'LongCOVIDFatigue', 'Long Hauler symptoms from LANCET paper', 
        'pasc_577',
    ]
    return selected_set_names

def get_selected_bundle_dict():
    selected_bundle_dict = {
        'b001': 'ACE inhibitors',
        'b002': 'ARBs - angiotensin II receptor blockers',
        'b003': 'K-sparing diuretics',
        'b004': 'Platelet inhibitors',
        'b005': 'Antiarrhythmics',
        'b006': 'PCSK9 inhibitors',
        'b007': 'Long-acting nitrates',
        'b008': 'ARNi - Angiotensin Receptor-Neprilysin Inhibitors',
        'b009': 'Loop diuretics',
        'b010': 'Alpha antagonists (BPH agents excluded)',
        'b011': 'Thiazide/Thiazide-like diuretics',
        'b012': 'Calcium channel blockers',
        'b013': 'Beta blockers',
        'b014': 'Minoxidil',
        'b015': 'Central alpha 2 agonists',
        'b016': 'Alpha antagonists',
        'b017': 'Anticoagulants',
        'b018': 'Hemodynamic/vasoactive agents',
        'b019': 'COVID treatment - repurposed meds',
        'b020': 'Chronic kidney diseases',
        'b021': 'Monoclonal antibodies',
        'b022': 'Cancer treatments',
        'b023': 'Diabetes treatments',
        'b024': 'Diabetes labs',
        'b025': 'Diabetes kidney disease',
        'b026': 'COVID tests',
        'b027': 'COVID qualitative results',
        'b028': 'COVID vaccines',
        'b029': 'Blood gases',
        'b030': 'Ventilation invasive treatments',
        'b031': 'Glascow Coma Scale score',
        'b032': 'Critical visits',        
    }
    return selected_bundle_dict

def get_selected_bundle_names():
    selected_bundle_names = list(get_selected_bundle_dict().values())
    return selected_bundle_names

def get_additional_pasc_broad_conditions():
    return [
        77670    , # Chest pain
        444070   , # Tachycardia
        436096   , # Chronic pain
        31967    , # Nausea
        196523   , # Diarrhea
        433316   , # Dizziness and giddiness
        442752   , # Muscle pain
        442588   , # Obstructive sleep apnea syndrome
        437663   , # Fever
        436962   , # Insomnia
        75860    , # Constipation
        27674    , # Nausea and vomiting
        4169095  , # Bradycardia
        318736   , # Migraine
        314754   , # Wheezing
        4236484  , # Paresthesia
        4305080  , # Abnormal breathing
        317376   , # Tachypnea
        442165   , # Loss of appetite
        40405599 , # Fibromyalgia
        318800   , # Gastroesophageal reflux disease
        313459   , # Sleep apnea
        4195085  , # Nasal congestion
        4034235  , # Tight chest
        4182187  , # Foot swelling
        436222   , # Altered mental status
        440704   , # Chronic pain syndrome
        434173   , # Fever symptoms
        315361   , # Orthopnea
        435524   , # Sleep disorder
        135618   , # Pruritic rash
        4275423  , # Supraventricular tachycardia
        136184   , # Pruritus of skin
        377575   , # Tinnitus
        73754    , # Restless legs
        139900   , # Urticaria
        4057995  , # Excessively deep breathing
        4021339  , # Feeling suicidal
        439147   , # Amnesia
        381549   , # Migraine with aura
        4087166  , # Labored breathing
        4273391  , # Suicidal thoughts
        438134   , # Hypersomnia
        134159   , # Precordial pain
        4143064  , # Suicidal
        4266361  , # Aggressive behavior
        45763549 , # Bilateral tinnitus
        436676   , # Posttraumatic stress disorder
        4103295  , # Ventricular tachycardia
        438867   , # Generalized aches and pains
        197675   , # Incontinence of feces
        45772876 , # Suffering
        4196636  , # Dysarthria
        4027314  , # Mental health impairment
        197607   , # Excessive and frequent menstruation
        375527   , # Headache disorder
        373786   , # Abnormal vision
        4133044  , # Chest discomfort
        43531003 , # Essential tremor
        4164633  , # Clouded consciousness
        435657   , # Dyssomnia
        378165   , # Nystagmus
        4218878  , # Subjective vertigo
        443432   , # Impaired cognition
        439383   , # Vertigo
        42872394 , # Daytime somnolence
        381273   , # Confusional state
        4094008  , # Bloating symptom
        4302555  , # Menorrhagia
        433031   , # Hallucinations
        4184149  , # Feeling irritable
        43022069 , # Primary central sleep apnea
        4023572  , # Abdominal bloating
        4304008  , # Memory impairment
        4150129  , # Musculoskeletal pain
        37311082 , # Erythematous rash
        4239155  , # Diaphragmatic breathing
        372886   , # Refractory migraine with aura
        4129155  , # Vaginal bleeding
        4059915  , # Fluttering heart
        436681   , # Insomnia disorder related to known organic factor
        256717   , # Bronchospasm
        4021498  , # Panic attack
        439708   , # Disorders of initiating and maintaining sleep
        4012381  , # Restlessness
        4029498  , # Seizure disorder
        436235   , # Taste sense altered
        4187507  , # Psychomotor agitation
        194696   , # Dysmenorrhea
        4319324  , # Polymyalgia
        255348   , # Polymyalgia rheumatica
        435786   , # Disorder of sleep-wake cycle
        4011938  , # Intermittent vertigo
        140803   , # Idiopathic urticaria
        81893    , # Ulcerative colitis
        313236   , # Cough variant asthma
        4012870  , # Positional vertigo
        4086811  , # Rapid shallow breathing
        4159659  , # Postural orthostatic tachycardia syndrome
        4078201  , # Mood swings
        4150759  , # Myofascial pain
        80141    , # Functional diarrhea
        313792   , # Paroxysmal tachycardia
        4036946  , # Floaters in visual field
        439013   , # Insomnia disorder related to another mental disorder
        381278   , # Cluster headache
        45768908 , # Exercise induced bronchospasm
        4214898  , # Decreased respiratory function
        4245464  , # Flat affect
        4223938  , # Dizziness
        4096245  , # Resting tremor
        439794   , # Central sleep apnea syndrome
        381035   , # Vertigo of central origin
        75580    , # Chronic ulcerative proctitis
        4084730  , # Fidgeting
        440087   , # Parasomnia
        4302535  , # Slow shallow breathing
        437579   , # Paroxysmal ventricular tachycardia
        373463   , # Cough headache syndrome
        4327815  , # Feeling angry
        42538688 , # Chronic musculoskeletal pain
        133834   , # Atopic dermatitis
        45757810 , # Abnormal uterine bleeding
        4137754  , # Secondary dysmenorrhea
        4242106  , # Occult blood in stools
        201627   , # Abnormal vaginal bleeding
        37110753 , # Generalized rash
        4012382  , # Weakness present
        442187   , # Chronic paroxysmal hemicrania
        381864   , # Subjective tinnitus
        4012875  , # Constant vertigo
        44783805 , # Hypogeusia
        4046994  , # Transient paresthesia
        373215   , # Latent nystagmus
        3661632  , # Thrombocytopenia due to Severe acute respiratory syndrome coronavirus 2
        433225   , # Ventricular flutter
        37110488 , # Chronic insomnia
        3661406  , # Acute respiratory distress syndrome due to disease caused by Severe acute respiratory syndrome coronavirus 2
        4155092  , # C/O nasal congestion
        45765899 , # Moderate cognitive impairment
        4187714  , # Excessive somnolence
        436522   , # Irregular sleep-wake pattern
        4085732  , # Intermittent confusion
        4316217  , # Primary fibromyalgia syndrome
        4012422  , # Severe vertigo
        376698   , # Internuclear ophthalmoplegia
        317893   , # Paroxysmal supraventricular tachycardia
        439150   , # Hypersomnia with sleep apnea
        40479837 , # Chronic ulcerative colitis
        440082   , # Persistent insomnia
        4120400  , # Moody
        4168860  , # Outbursts of anger
        45773430 , # Complete fecal incontinence
        43530629 , # Chronic vertigo
        4096147  , # Poor concentration
        4198855  , # Chronic urticaria
        40480274 , # Nonsustained ventricular tachycardia
        4012874  , # Paroxysmal vertigo
        4105160  , # End-position nystagmus
        132702   , # Erythema multiforme        
    ]

def create_custom_concept_sets(concept_set_members, concept):
    cs1 = custom_concept_set_from_existing_set(concept_set_members, 
        'maintenance fluids', 'x001', 
        'mf-fluids')    
    cs1 = custom_concept_set_with_names_rlike(concept, 
        'maintenance fluids', 'x001', 
        'sodium chloride.*(Injection|Cartridge|Syringe)', cs1)

    cs2 = custom_concept_set_with_names_rlike(concept, 
        'sodium chloride nasal', 'x002',  
        'sodium chloride.*(Nasal|Inhalation)')

    cs3 = custom_concept_set_with_names_starting_with(concept, 
        'chest x-ray', 'x003', 
        'Radiologic examination, chest')
    cs3 = custom_concept_set_with_names(concept, 
        'chest x-ray', 'x003', 
        'Radiographic procedure of chest', cs3)

    cs4 = custom_concept_set_with_names_starting_with(concept, 
        'x-ray', 'x004', 
        'Radiologic examination', cs3)

    cs5 = custom_concept_set_with_names_starting_with(concept, 
        'outpatient visit', 'x005', 
        'Office or other outpatient visit')

    cs6 = custom_concept_set_with_names_containing(concept, 
        'polyethylene glycol', 'x006', 
        'polyethylene glycol')

    cs7 = custom_concept_set_from_existing_set(concept_set_members, 
        'CT scan', 'x007', 
        'Computed Tomography (CT) Scan')    
    cs7 = custom_concept_set_with_names_rlike(concept, 
        'CT scan', 'x007', 
        '^(Computed tomograph|CT of)', cs7)

    cs8 = custom_concept_set_with_names_rlike(concept, 
        'CT chest', 'x008',  
        '^(CT|Computed tomograph).*(chest|lung)')

    cs9 = custom_concept_set_from_existing_set(concept_set_members, 
        'depression', 'x009', 
        'ARIScience - Depression - JA')
    cs9 = custom_concept_set_with_names(concept, 
        'depression', 'x009', [ 
        'Depressed mood', 'Depressive episode',
        'Adjustment disorder with depressed mood',
        'Adjustment disorder with mixed anxiety and depressed mood',
        'Feeling down, depressed or hopeless in last 2 weeks.frequency [Reported PHQ-9 CMS]',
        'Symptoms of depression'], cs9)

    cs10 = custom_concept_set_from_existing_set(concept_set_members, 
        'NSAID', 'x010', 
        'NSAIDs')
    cs10 = custom_concept_set_from_existing_set(concept_set_members, 
        'NSAID', 'x010', 
        '[PASC] NSAID', cs10)

    cs11 = custom_concept_set_with_ids(concept,
        'COVID broad dx', 'x011',
        get_broad_covid_diagnosis_set())

    cs12 = custom_concept_set_from_existing_set(concept_set_members, 
        'PASC broad', 'x012', 
        'LongCOVIDFatigue')
    cs12 = custom_concept_set_from_existing_set(concept_set_members, 
        'PASC broad', 'x012', 
        'Long Hauler symptoms from LANCET paper', cs12)
    cs12 = custom_concept_set_with_ids(concept, 
        'PASC broad', 'x012', 
        get_additional_pasc_broad_conditions(), cs12)

    cs13 = custom_concept_set_from_existing_set(concept_set_members, 
        'covid weak dx', 'x013', 
        '[COVID19 Dx] Weak positive dx')
    cs13 = filter_concept_set_on_domain(cs13, concept, excludes='Drug')
    
    cs14 = custom_concept_set_from_existing_set(concept_set_members, 
        'drugs for covid weak dx', 'x014', 
        '[COVID19 Dx] Weak positive dx')
    cs14 = filter_concept_set_on_domain(cs14, concept, includes='Drug')
    cs14 = custom_concept_set_with_names(concept, 
        'drugs for covid weak dx', 'x014', 
        ['fluticasone'], cs14)

    cs15 = custom_concept_set_from_existing_set(concept_set_members, 
        'all pain', 'x015', 
        'pain-72')
    cs15 = custom_concept_set_with_names(concept, 
        'all pain', 'x015', [
            'Characteristic of pain',
            'Pain of right knee joint',
            'Pain of left knee region',
            'Pain of knee region',
            'Pain management (specialty)', 
            'Temporomandibular joint-pain-dysfunction syndrome',
            '[X]Other chest pain',
            'Joint pain in ankle and foot',
        ], cs15)

    cs16 = custom_concept_set_with_names(concept, 
        'non-hospital visit', 'x016', [
            'Telehealth',
            'Non-hospital institution Visit',
        ])

    all_cs = [cs1, cs2, cs3, cs4, cs5, cs6, cs7, cs8, cs9, cs10, cs11, cs12, cs13, cs14, cs15, cs16]
    custom_sets = reduce(DataFrame.union, all_cs).distinct()

    return custom_sets

def get_custom_concept_set_ids():
    custom_set_ids = ['x{:03d}'.format(i) for i in range(1, 17)]
    return custom_set_ids

def get_selected_concept_ids():
    selected_concept_ids = [
        705076,   # Post-acute COVID-19
        37311061, # COVID-19
        37310285, # Pneumonia caused by SARS-CoV-2 ...
        3661408,  # Pneumonia caused by SARS-CoV-2
        312437,   # Dyspnea
        4223659,  # Fatigue
        9201,     # Inpatient Visit
        9202,     # Outpatient Visit
        9203,     # Emergency Room Visit
        3005879,  # First Respiration rate Set
        3019237,  # Chief complaint - Reported
        4203942,  # Admitting diagnosis
        40217302, # Clinical decision support mechanism ...
        3010247,  # Hospital discharge Dx
        4203722,  # Patient encounter procedure
        2313990,  # Duplex scan of extremity veins ...
        45765920, # Never used tobacco
        45765917, # Ex-tobacco user
        4138763,  # Acceptable pain level status
        4192791,  # Pain management (specialty)
        37016200, # Exposure to viral disease
        36714927, # Sequelae of infectious disease
        2786229,  # Introduction of Anti-inflammatory ...
        2787749,  # Introduction of Anti-inflammatory ...
        4307376,  # Final inpatient visit with instructions ...
        4093836,  # Glasgow coma score
        759536,   # Infectious disease (viral respiratory ...
        2313869,  # Echocardiography, transthoracic, ...
        439224,   # Allergy to drug
        1149380,  # fluticasone
        40483286, # Critical illness myopathy
        4152283,  # Main spoken language
        5083,     # Telehealth
        42898160, # Non-hospital institution Visit
        254761,   # Cough
        4034235,  # Tight chest
        4182187,  # Foot swelling
        434173,   # Fever symptoms        
        35605482,  # 2 ML ondansetron 2 MG/ML Injection
        38004250,  # Ambulatory Radiology Clinic / Center
        725069,    # Radiologic examination, chest; 2 views
        19020053,  # acetaminophen 500 MG Oral Tablet
        4193704,   # Type 2 diabetes mellitus without complication
        37311059,  # Exposure to SARS-CoV-2
        257011,    # Acute upper respiratory infection
        4195694,   # Acute respiratory distress syndrome
        2615309,   # Non-covered item or service
        36308879,  # Never used
    ]
    return selected_concept_ids

def get_feature_to_category_dict():
    demographics = [
        'year_of_birth',
        'gender_concept_id',
        'race_concept_id',
        'ethnicity_concept_id',
    ]

    vaccine = [
        'vaccine_txn',
        'vax_time_to_covid',
    ]

    data_source = [
        'data_partner_id',
        'd124',
        'd399',
        'd134',
        'd888',
        'd75',
        'd526',
        'd294',
        'd798',
        'd569',
        'd828',
        'zip_id',
    ]

    covid_diagnosis = [
        'num_covid_episodes',
        'total_episode_length',
        'max_episode_length',
        'months_from_covid_index',
        'months_from_first_covid',
        'x013',        # X: covid weak dx
        'b026',        # B: COVID tests
    ]

    visits = [
        'months_from_observation_end',
        'covid_index_to_observation_end',
        'covid_index_to_last_visit_end',
        'months_from_last_visit',
        'months_from_first_visit',
        'num_visits',
        'max_visit_length',
        'total_visit_length',
        'x005',        # X: outpatient visit
        'x016',        # X: non-hospital visit
        's809092939',  # A: Outpatient
        's231912125',  # A: Emergency Room Visits
        's122805878',  # S: Long COVID Clinic Visit
        's972465851',  # A: Hospitalization
        'b032',        # B: Critical visits
        'c9202',       # C: Outpatient Visit
        'c4203722',    # C: Patient encounter procedure
        'c38004250',   # C: Ambulatory Radiology Clinic / Center
        'c5083',       # C: Telehealth
    ]

    condition_occurrences = [
        'x012',        # X: PASC broad
        'x003',        # X: chest x-ray
        'x015',        # X: all pain
        'x014',        # X: drugs for covid weak dx
        'x010',        # X: NSAID
        'x001',        # X: maintenance fluids
        'x007',        # X: CT scan
        's400852949',  # A: Cough
        's348930395',  # S: LongCOVIDFatigue
        's947341641',  # A: Albuterol
        's402442153',  # A: Fatigue
        's571514014',  # A: Drug Corticosteroids Systemic
        's799270877',  # A: Hypertension
        's961458756',  # A: Respiratory Disorder
        's875380843',  # S: Metabolic Disorders
        's546116974',  # A: Inflammation Resp
        's562288165',  # A: Acute Disease
        's689261095',  # A: Dyspnea
        's313998782',  # S: gestation
        's295952371',  # A: Drug Fentanyl
        's959049707',  # S: Long Hauler symptoms from LANCET paper
        's995642183',  # S: Immunosuppression L04
        's812896616',  # A: Abdominal Pain
        's541737679',  # A: ARDS
        's672055106',  # A: Cancer
        's777909573',  # A: Electrolyte IV
        's160955543',  # A: Alcohol
        's305217555',  # A: Vaccines
        's533705532',  # A: Obesity
        's908267911',  # A: Chest Pain
        's540936013',  # A: Cerebral
        's478273027',  # A: Renal Limited
        's777788102',  # S: [PASC] Antidepressant
        's892966630',  # A: Prednisone
        's216227734',  # S: Anesthesia Medications (SIANES)
        's481594340',  # A: Vertigo Dizziness
        's967701631',  # A: Mets CCI
        's275632871',  # A: Non Smoker
        's414583656',  # A: Chemotherapy Endocrine
        's849907658',  # A: Insomnia
        's379995033',  # A: Elevated Cholesterol
        's603185723',  # A: Drug Clonazepam
        's456422283',  # A: Drug Pseudoephedrine
        's949429075',  # A: Weight Loss
        'b017',        # B: Anticoagulants
        'c36714927',   # C: Sequelae of infectious disease
        'c37016200',   # C: Exposure to viral disease
        'c3019237',    # C: Chief complaint - Reported
        'c4223659',    # C: Fatigue
        'c725069',     # C: Radiologic examination, chest; 2 views
        'c35605482',   # C: 2 ML ondansetron 2 MG/ML Injection
        'c3661408',    # C: Pneumonia caused by SARS-CoV-2
        'c254761',     # C: Cough
        'c19020053',   # C: acetaminophen 500 MG Oral Tablet
        'c257011',     # C: Acute upper respiratory infection
        'c4193704',    # C: Type 2 diabetes mellitus without complication
        'c1149380',    # C: fluticasone
    ]

    measurements = [
        '3012888',     # Diastolic blood pressure
        '3004501',     # Glucose [Mass/volume] in Serum or Plasma
        '706163',      # SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection
        '3016723',     # Creatinine [Mass/volume] in Serum or Plasma
        '3024128',     # Bilirubin.total [Mass/volume] in Serum or Plasma
        '3020891',     # Body temperature
        '3013682',     # Urea nitrogen [Mass/volume] in Serum or Plasma
        '3000963',     # Hemoglobin [Mass/volume] in Blood
        '3013762',     # Body weight Measured
        '3013650',     # Neutrophils [#/volume] in Blood by Automated count
        '3014576',     # Chloride [Moles/volume] in Serum or Plasma
        '3004249',     # Systolic blood pressure
        '40762499',    # Oxygen saturation in Arterial blood by Pulse oximetry
        '3019550',     # Sodium [Moles/volume] in Serum or Plasma
        '4099154',     # Body weight
        '4301868',     # Pulse rate
        '3024171',     # Respiratory rate
        '3004327',     # Lymphocytes [#/volume] in Blood by Automated count
        '3027018',     # Heart rate
        '3009744',     # MCHC [Mass/volume] by Automated count
        '3023103',     # Potassium [Moles/volume] in Serum or Plasma
        '3012030',     # MCH [Entitic mass] by Automated count
        '3019800',     # Troponin T.cardiac [Mass/volume] in Serum or Plasma
        '3028615',     # Eosinophils [#/volume] in Blood by Automated count
        '3023314',     # Hematocrit [Volume Fraction] of Blood by Automated count
        '3024929',     # Platelets [#/volume] in Blood by Automated count
        '3003841',     # Heart rate Peripheral artery by palpation
        '4154790',     # Diastolic blood pressure
        '3006906',     # Calcium [Mass/volume] in Serum or Plasma
        '4313591',     # Respiratory rate
    ]

    feature_to_category = {}

    for feature in demographics:
        feature_to_category[feature] = 'Demographics'

    for feature in data_source:
        feature_to_category[feature] = 'Data source'

    for feature in visits:
        feature_to_category[feature] = 'Visits'

    for feature in vaccine:
        feature_to_category[feature] = 'Vaccine'

    for feature in covid_diagnosis:
        feature_to_category[feature] = 'COVID diagnosis'

    for feature in condition_occurrences:
        feature_to_category[feature] = 'Conditions'

    for feature in measurements:
        feature_to_category[feature] = 'Measurements'

    return feature_to_category

def compute_covid_diagnostic_windows(covid_dates, silver, 
    reinfection_threshold=92, min_covid_episode_length=7):
    df = covid_dates.where(expr('covid_test_positive > 0') | expr('covid_concept_positive > 0')) \
        .withColumn('covid_diagnosis', lit(1))
    df = silver.select('person_id', 'covid_index') \
        .join(df, on='person_id', how='left') \
        .withColumn('covid_days', datediff(col('date'), col('covid_index')))
    
    person_window = Window.partitionBy('person_id').orderBy('date')
    df = df.withColumn('lag', datediff(col('date'), lag('date', 1).over(person_window))) \
        .withColumn('jump', (col('lag').isNull() | (col('lag') > reinfection_threshold)).cast('int')) \
        .withColumn('covid_episode', F.sum(col('jump')).over(person_window)).drop('jump', 'lag')

    episode_window = Window.partitionBy('person_id', 'covid_episode').orderBy('date')
    df = df.withColumn('lag', datediff(col('date'), lag('date', 1).over(episode_window))) \
        .withColumn('increment', when(col('lag').isNull(), min_covid_episode_length).otherwise(col('lag'))) \
        .withColumn('episode_days', F.sum(col('increment')).over(episode_window)).drop('increment', 'lag')

    episode_desc_window = Window.partitionBy('person_id', 'covid_episode').orderBy(col('date').desc())
    df2 = df.withColumn('row', row_number().over(episode_desc_window)) \
        .where(col('row') == 1).drop('row') \
        .select('person_id', 'covid_episode', col('episode_days').alias('episode_length'))

    df3 = df2.groupBy('person_id').agg(
        F.sum('episode_length').alias('total_episode_length'), 
        F.max('episode_length').alias('max_episode_length'), 
        F.max('covid_episode').alias('num_covid_episodes'))

    df = df.join(df2, on=['person_id', 'covid_episode'], how='left') \
        .join(df3, on='person_id', how='left')

    df_episode = df.withColumn('episode_name', concat(lit('covid_'), col('covid_episode'))) \
        .groupBy('person_id').pivot('episode_name').agg(
            first('date').alias('first'), 
            last('date').alias('last')
        ).drop('episode_name')

    df = df.join(df_episode, on='person_id', how='left')

    df = df \
        .withColumn('months_from_first_covid', 
            F.round(months_between(to_date(lit('2023-03-31')), col('covid_1_first')), 1)) \
        .withColumn('months_from_covid_index', 
            F.round(months_between(to_date(lit('2023-03-31')), col('covid_index')), 1))

    df = move_cols_to_front(df, ['person_id', 'time_to_pasc', 'covid_days', 'date', 'covid_episode', 'episode_length', 'num_covid_episodes', 'total_episode_length', 'max_episode_length', 'episode_days'])
    df = df.orderBy('person_id', 'date')

    return df

def map_feature_to_name(df, col='feature', df_feature=None, df_concept=None):
    # df_feature, if provided, should have columns: feature_id, feature_name
    # df_concept, if provided, should have columns: concept_id, concept_name
    if df_feature and 'feature_id' not in df.columns:
        df = df.withColumn('feature_id', regexp_extract(col, r'^([abcsx]\d+?)(_.*|)$', 1))
    if df_feature and 'feature_name' not in df.columns:
        df = df.join(df_feature.select('feature_id', 'feature_name').distinct(), 
            on='feature_id', how='left')
    if df_concept and 'concept_id' not in df.columns:
        df = df.withColumn('concept_id', regexp_extract(col, r'^(\d{4,})(_.*|)$', 1))
    if df_concept and 'concept_name' not in df.columns:
        cols = ['concept_id', 'concept_name']
        if 'domain_id' not in df.columns and 'domain_id' in df_concept.columns:
            cols.append('domain_id')
        df = df.join(df_concept.select('concept_id', 'concept_name', 'domain_id'), 
            on='concept_id', how='left')

    return df

def data_partner_id_to_onehot(person_table, subset='dp10'):
    if subset == 'dp10':
        subset = [124, 399, 134, 888, 75, 526, 294, 798, 569, 828]
    df = person_table.select('person_id', 'data_partner_id') \
        .withColumn('dp', concat(lit('d'), col('data_partner_id')))
    if subset is not None:
        subset = [f'd{x}' if isinstance(x, int) else x for x in subset]
    df = df.groupBy('person_id').pivot('dp', subset).agg(count('person_id'))
    df = df.fillna(0)
    return df

# temporal feature engineering

def temporal_engineered_concept_features(person_concept_features, pivot_cols, agg_concepts=True):
    df_features = person_concept_features

    distinct_cols = [] if agg_concepts else ['concept_id']

    df_all = df_features \
        .groupBy('person_id') \
        .pivot('feature_id', pivot_cols) \
        .agg(countDistinct('date', *distinct_cols)) 
    
    df_all.count()

    df_before = df_features.where(expr('covid_days < -7')) \
        .groupBy('person_id') \
        .pivot('feature_id', pivot_cols) \
        .agg(countDistinct('date', *distinct_cols))
    df_before = rename_cols_with_postfix(df_before, pivot_cols, 'before')

    df_before.count()

    df_during = df_features.where(expr('covid_days >= -7') & expr('covid_days <= 7')) \
        .groupBy('person_id') \
        .pivot('feature_id', pivot_cols) \
        .agg(countDistinct('date', *distinct_cols))
    df_during = rename_cols_with_postfix(df_during, pivot_cols, 'during')

    df_during.count()

    df_after = df_features.where(expr('covid_days > 7') & expr('covid_days <= 28')) \
        .groupBy('person_id') \
        .pivot('feature_id', pivot_cols) \
        .agg(countDistinct('date', *distinct_cols))
    df_after = rename_cols_with_postfix(df_after, pivot_cols, 'after')

    df_after.count()

    df_b1w = df_features.where(expr('covid_days >= -7') & expr('covid_days < 0')) \
        .groupBy('person_id') \
        .pivot('feature_id', pivot_cols) \
        .agg(countDistinct('date', *distinct_cols))
    df_b1w = rename_cols_with_postfix(df_b1w, pivot_cols, 'b1w')

    df_b1w.count()

    df_a1w = df_features.where(expr('covid_days >= 0') & expr('covid_days <= 7')) \
        .groupBy('person_id') \
        .pivot('feature_id', pivot_cols) \
        .agg(countDistinct('date', *distinct_cols))
    df_a1w = rename_cols_with_postfix(df_a1w, pivot_cols, 'a1w')

    df_a1w.count()

    coeff = np.log(10) / 28
    df_weight = df_features.select('person_id', 'covid_days', 'feature_id').distinct() \
        .withColumn('time_weight',
            when(expr('covid_days > 28'), lit(10)) \
            .when(expr('covid_days >= -7'), F.exp(col('covid_days') * coeff)) \
            .otherwise(lit(0.5)))

    df_weight.count()

    df_weight_sum = df_weight \
        .groupBy('person_id') \
        .pivot('feature_id', pivot_cols) \
        .agg(F.sum('time_weight'))
    df_weight_sum = rename_cols_with_postfix(df_weight_sum, pivot_cols, 'weight_sum')

    df_weight_sum.count()

    df_weight_max = df_weight \
        .groupBy('person_id') \
        .pivot('feature_id', pivot_cols) \
        .agg(F.max('time_weight'))
    df_weight_max = rename_cols_with_postfix(df_weight_max, pivot_cols, 'weight_max')

    df_weight_max.count()

    df = df_all
    for d in [df_before, df_during, df_after, df_b1w, df_a1w, df_weight_sum, df_weight_max]:
        df = df.join(d, on='person_id', how='left')

    df = df.fillna(0)

    df.count()

    for c in pivot_cols:
        df = df.withColumn(c+'_a1w_vs_b1w', col(c+'_a1w')-col(c+'_b1w'))
        df = df.withColumn(c+'_after_vs_during', col(c+'_after')-col(c+'_during'))
        df = df.withColumn(c+'_during_vs_before', col(c+'_during')-col(c+'_before'))
        df = df.withColumn(c+'_after_vs_before', col(c+'_after')-col(c+'_before'))

    return df

def pyspark_column_name_add_suffix(df, suffix, exclude=[]):
    return df.select([F.col(c).alias(f'{c}_{suffix}') if c not in exclude else F.col(c) for c in df.columns])

def stats_to_feature_split_orig(df, selected_concept_ids, covid_index, use_diffs=True, mode = '2w_offset',
                           feature_column_name='condition_concept_id', feature_date_column_name='condition_start_date',
                           covid_date_column_name='covid_index', collect_values=False, 
                           feature_value_column_name='value_as_number'):
    
    date_names_dict = {
        '2w_offset_v2': ['bc', 'bc7', '1w', '2_4w'],
    }

    date_filters_dict = {
        '2w_offset_v2': [
            f'{feature_date_column_name} < {covid_date_column_name}',
            f"{feature_date_column_name} >= date_sub({covid_date_column_name}, 7) and {feature_date_column_name} < {covid_date_column_name}",
            f"{feature_date_column_name} >= {covid_date_column_name} and {feature_date_column_name} < date_add({covid_date_column_name}, 7)",
            f"{feature_date_column_name} >= date_add({covid_date_column_name}, 7)",
            ],
    }

    ret = covid_index.drop(covid_date_column_name)
    df_with_covid_index = df.join(covid_index, on='person_id')

    date_names = date_names_dict[mode]
    if collect_values:
        for i in range(len(date_names)):
            date_names[i] = date_names[i] + '_val'

    date_filters = date_filters_dict[mode]

    if collect_values:
        dfs = []
        for f in date_filters:
            pvts = df_with_covid_index.filter(f)\
                    .select('person_id', feature_column_name, feature_value_column_name)\
                    .groupBy('person_id')\
                    .pivot(feature_column_name, selected_concept_ids)
            dfs.append(pvts.mean(feature_value_column_name))
     
    else:
        dfs = [df_with_covid_index.filter(f)
               .select('person_id', feature_column_name)
               .groupBy('person_id')
               .pivot(feature_column_name, selected_concept_ids)
               .count()
               for f in date_filters]

    for sfx, df in zip(date_names, dfs):
        ret = ret.join(pyspark_column_name_add_suffix(df, sfx, ['person_id']), on='person_id', how='left')

    ret = ret.na.fill(0)

    if  use_diffs:
        for cid in selected_concept_ids:
            for i in range(len(date_names) - 1):
                col_name_1 = f'{cid}_{date_names[i]}'
                col_name_2 = f'{cid}_{date_names[i + 1]}'
                diff_col_name = f'{col_name_1}-{col_name_2}'
                ret = ret.withColumn(diff_col_name, F.col(col_name_1) - F.col(col_name_2))

    return ret

def stats_to_feature_split(df, selected_concept_ids, use_diffs=True, mode='2w_offset', feature_column_name='condition_concept_id', feature_date_column_name='condition_start_date', covid_date_column_name='covid_index', collect_values=False, feature_value_column_name='value_as_number'):
    
    date_names_dict = {
        '2w_offset_v2': ['bc', 'bc7', '1w', '2_4w'],
    }

    date_filters_dict = {'2w_offset_v2': [
        f'{feature_date_column_name} < {covid_date_column_name}',
        f"{feature_date_column_name} >= date_sub({covid_date_column_name}, 7) and {feature_date_column_name} < {covid_date_column_name}",
        f"{feature_date_column_name} >= {covid_date_column_name} and {feature_date_column_name} < date_add({covid_date_column_name}, 7)",
        f"{feature_date_column_name} >= date_add({covid_date_column_name}, 7)", ],
    }

    ret = df.select('person_id').distinct()

    date_names = date_names_dict[mode]
    if collect_values:
        for i in range(len(date_names)):
            date_names[i] = date_names[i] + '_val'

    date_filters = date_filters_dict[mode]

    if collect_values:
        dfs = []
        for f in date_filters:
            pvts = df.filter(f)\
                .select('person_id', feature_column_name, feature_value_column_name)\
                .groupBy('person_id')\
                .pivot(feature_column_name, selected_concept_ids)
            dfs.append(pvts.mean(feature_value_column_name))
     
    else:
        dfs = [df.filter(f)
            .select('person_id', feature_column_name)
            .groupBy('person_id')
            .pivot(feature_column_name, selected_concept_ids)
            .count()
            for f in date_filters]

    for sfx, df in zip(date_names, dfs):
        ret = ret.join(pyspark_column_name_add_suffix(df, sfx, ['person_id']), on='person_id', how='left')

    ret = ret.na.fill(0)

    if  use_diffs:
        for cid in selected_concept_ids:
            for i in range(len(date_names) - 1):
                col_name_1 = f'{cid}_{date_names[i]}'
                col_name_2 = f'{cid}_{date_names[i + 1]}'
                diff_col_name = f'{col_name_1}-{col_name_2}'
                ret = ret.withColumn(diff_col_name, F.col(col_name_1) - F.col(col_name_2))

    return ret

def temporal_engineered_lab_count_features(person_labs, lab_count_ids):
    df_lab_counts = stats_to_feature_split(person_labs, lab_count_ids, use_diffs=True, mode='2w_offset_v2', feature_column_name='measurement_concept_id', feature_date_column_name='measurement_date', covid_date_column_name='covid_index', collect_values=False)
    return df_lab_counts

def temporal_engineered_lab_value_features(person_labs, lab_value_ids):
    df_lab_values = stats_to_feature_split(person_labs, lab_value_ids, use_diffs=True, mode='2w_offset_v2', feature_column_name='measurement_concept_id', feature_date_column_name='measurement_date', covid_date_column_name='covid_index', collect_values=True, feature_value_column_name='value_as_number')
    return df_lab_values

def temporal_engineered_lab_features(person_labs, lab_count_ids, lab_value_ids):
    df_lab_counts = temporal_engineered_lab_count_features(person_labs, lab_count_ids)
    df_lab_values = temporal_engineered_lab_value_features(person_labs, lab_value_ids)
    df_lab = df_lab_counts.join(df_lab_values, on='person_id', how='left')
    return df_lab

def extract_list_head_from_df(df, order_col, keep_col, n=100, asc=False):
    '''local sort by converting to pandas; for small tables only'''
    keep_list = df.select(order_col, keep_col).toPandas() \
        .sort_values(order_col, ascending=asc)[keep_col].tolist()[:n]
    return keep_list

def extract_list_tail_from_df(df, order_col, keep_col, n=100):
    return extract_list_head_from_df(df, order_col, keep_col, n, asc=True)
 

def aggregate_feature_importance_to_groups(df_features):
    df = df_features.withColumn('group', 
        when(col('feature_id') != '', col('feature_id')) \
        .when(col('concept_id') != '', col('concept_id')) \
        .when(col('feature').rlike(r'\d{2,3}'), lit('data_partner_id')) \
        .otherwise(col('feature')))
    df = df.withColumn('categorical', 
        ~col('feature_id').isNull() & (col('feature_id') != ''))
    df = move_cols_to_front(df, ['group'])
    df2 = df.groupBy('group').agg(F.sum('importance').alias('group_score'))
    df = df.join(df2.select('group', F.round('group_score', 5).alias('group_score')), on='group', how='left')
    df = move_cols_to_front(df, ['feature', 'importance', 'group', 'group_score', 'feature_name', 'concept_name'])
    df = df.orderBy(col('group_score').desc(), col('group'), col('importance').desc())
    return df

def aggregate_feature_importance_to_categories(df_features):
    df = df_features.withColumn('group', 
        when(col('feature_id') != '', col('feature_id')) \
        .when(col('concept_id') != '', col('concept_id')) \
        .when(col('feature').rlike(r'\d{2,3}'), lit('data_partner_id')) \
        .otherwise(col('feature')))
    df = df.withColumn('categorical', 
        ~col('feature_id').isNull() & (col('feature_id') != ''))
    df = move_cols_to_front(df, ['group'])
    df2 = df.groupBy('group').agg(F.sum('importance').alias('group_score'))
    df = df.join(df2.select('group', F.round('group_score', 5).alias('group_score')), on='group', how='left')
    df = move_cols_to_front(df, ['feature', 'importance', 'group', 'group_score', 'feature_name', 'concept_name'])

    df = df.toPandas()
    feature_to_category = get_feature_to_category_dict()
    df['category'] = df['group'].map(feature_to_category)
    df = spark.createDataFrame(df)
    df3 = df.groupBy('category').agg(F.sum('importance').alias('category_score'))
    df = df.join(df3.select('category', F.round('category_score', 5).alias('category_score')), on='category', how='left')
    df = move_cols_to_front(df, ['category', 'category_score'])
    df = df.orderBy(col('category_score').desc(), col('category'), col('importance').desc())

    return df

def person_split_by_hash(df, train_percent=80):
    df = df.select('person_id').withColumn('train', 
        when(expr('pmod(hash(person_id), 100)') < train_percent, True).otherwise(False))
    df_train = df.where(col('train')).drop('train')
    df_test = df.where(~col('train')).drop('train')
    return df_train, df_test

# machine learning functions

def ml_get_model(model_or_name, threads=-1, classify=True, seed=0):
    classification_models = {
        'lgbm': LGBMClassifier, 
        'xgb': XGBClassifier,
        'rf': RandomForestClassifier,
        'lr': LogisticRegression,
    }
    regression_models = {}
    if isinstance(model_or_name, str):
        model_class = None
        if classify:
            model_class = classification_models.get(model_or_name.lower())
        else:
            model_class = regression_models.get(model_or_name.lower())    
        if not model_class:
            raise Exception("unrecognized model: '{}'".format(model_or_name))
        else:
            model = model_class(random_state=seed)
    else: 
        model = model_or_name
    name = re.search("\\w+", str(model)).group(0)
    return model, name

def ml_model_feature_importances(model):
    if hasattr(model, "feature_importances_"):
        fi = model.feature_importances_
    else:
        if hasattr(model, "coef_"):
            fi = model.coef_
        else:
            return
    return fi
    

def ml_sort_features_by_importance(fi, feature_names, n_top=2000):
    features = [(f, n) for f, n in zip(fi, feature_names)]
    top = sorted(features, key=lambda f: abs(f[0]), reverse=True)[:n_top]
    return top

def ml_top_important_features(model, feature_names, n_top=2000):
    fi = ml_model_feature_importances(model)
    return ml_sort_features_by_importance(fi, feature_names, n_top)

def ml_sprint_features(top_features, n_top=2000):
    str = ''
    for i, feature in enumerate(top_features):
        if i >= n_top:
            break
        str += '  {:10.5f}  {}\n'.format(feature[0], feature[1])
    return str

def ml_score_format(metric, score, signed=False, eol=''):
    if signed:
        return '{:<25} = {:+.5f}'.format(metric, score) + eol
    else:
        return '{:<25} =  {:.5f}'.format(metric, score) + eol

def ml_print_metrics(scores, model_name=None):
    metric_names = 'accuracy_gain accuracy_score roc_auc_score matthews_corrcoef precision_score recall_score f1_score'.split()
    score_dict = {} # dict with sorted keys and None values
    if model_name: 
        score_dict['model'] = model_name
    for m in metric_names:
        s = scores.get(m)
        signed = True if m.endswith('_gain') else False
        if s is not None:
            print(' ', ml_score_format(m, s, signed=signed))
            score_dict[m] = round(s, 7)
    return score_dict

def ml_get_splits(x, y, cv=5, seed=0):
    skf = StratifiedKFold(n_splits=cv, shuffle=True, random_state=seed)
    splits = skf.split(x, y)
    return list(splits)

def ml_classify(model, x, y, splits, features, seed=0, class_weight=None, return_features=False):
    model, name = ml_get_model(model, classify=True, seed=seed)

    train_scores, test_scores = [], []
    tests, preds = None, None
    probas = None
    best_model = None
    best_score = -np.Inf
    feature_importances = []

    print('>', name)
    print('Cross validation:')
    for i, (train_index, test_index) in enumerate(splits):
        x_train, x_test = x[train_index], x[test_index]
        y_train, y_test = y[train_index], y[test_index]
        model.set_params(class_weight=class_weight)
        model.fit(x_train, y_train)
        train_score = model.score(x_train, y_train)
        test_score = model.score(x_test, y_test)
        train_scores.append(train_score)
        test_scores.append(test_score)
        feature_importances.append(ml_model_feature_importances(model))
        print("  fold {}/{}: score = {:.3f}  (train = {:.3f})".format(i+1, len(splits), test_score, train_score))
        if test_score > best_score:
            best_model = model
            best_score = test_score

        y_pred = model.predict(x_test)
        preds = np.concatenate((preds, y_pred)) if preds is not None else y_pred
        tests = np.concatenate((tests, y_test)) if tests is not None else y_test
        if hasattr(model, "predict_proba"):
            probas_ = model.predict_proba(x_test)
            probas = np.concatenate((probas, probas_)) if probas is not None else probas_

    uniques, counts = np.unique(tests, return_counts=True)
    average = 'binary' if len(uniques) <= 2 else 'weighted'

    scores = {}
    if probas is not None:
        fpr, tpr, thresholds = metrics.roc_curve(tests, probas[:, 1], pos_label=1)
        scores['roc_auc_score'] = metrics.auc(fpr, tpr)
    naive_accuracy = max(counts) / len(tests)
    accuracy = np.sum(preds == tests) / len(tests)
    scores['accuracy_gain'] = accuracy - naive_accuracy
    metric_names = 'accuracy_score matthews_corrcoef precision_score recall_score f1_score'.split()
    for m in metric_names:
        s = None
        try:
            s = getattr(metrics, m)(tests, preds, average=average)
        except Exception:
            try:
                s = getattr(metrics, m)(tests, preds)
            except Exception:
                pass
        if s:
            scores[m] = s
    print('Average validation metrics:')
    score_dict = ml_print_metrics(scores, name)
    print('Feature importance:')
    fi = np.mean(np.stack(feature_importances), axis=0)
    # top_features = ml_top_important_features(best_model, features)
    top_features = ml_sort_features_by_importance(fi, features)
    print(ml_sprint_features(top_features))

    if return_features:
        return top_features
    else:
        return score_dict

def ml_classify_df(df, model='LGBM', x_cols=None, y_col='pasc', cv=5, seed=0, class_weight=None, return_features=False, return_spark=True):
    if x_cols is None:
        excludes = ['person_id', 'time_to_pasc', 'pasc']
        excludes.append(y_col)
        x_cols = [c for c in df.columns if not c in excludes]

    cols = [y_col] + x_cols
    df_all = df.select(cols).toPandas()

    y = df_all.loc[:, y_col].to_numpy()
    x = df_all.drop(y_col, axis=1).to_numpy()

    splits = ml_get_splits(x, y, cv=cv, seed=seed)

    ret = ml_classify(model, x, y, splits, features=x_cols, seed=seed, class_weight=class_weight, return_features=return_features)

    if return_spark:
        if return_features:
            ret = pd.DataFrame(ret, columns=['importance', 'feature'])
            ret = spark.createDataFrame(ret).select('feature', 'importance')
            ret = ret.orderBy(col('importance').desc())
        else:
            ret = pd.DataFrame([ret])
            ret = spark.createDataFrame(ret)

    return ret

def ml_incrementally_add_features(df, start_cols, full_cols, y_col='pasc', model='LGBM', cv=5):
    step = 0
    x_cols = start_cols
    start_desc = start_cols[0]
    print('Start features:', start_cols)
    s0 = ml_classify_df(df, model, x_cols=x_cols, y_col=y_col, cv=cv)
    best_score = s0['accuracy_score']
    rows = [{'step': step, 'feature': start_desc, **s0, 'score_change': 0}]
    for feature in full_cols:
        if feature in x_cols or feature not in df.columns:
            continue
        step += 1
        print(f'\nstep: {step}, checking feature: {feature}')
        x_cols.append(feature)
        scores = ml_classify_df(df, model, x_cols=x_cols, y_col='pasc', cv=5, return_spark=False)
        change = scores['accuracy_score'] - best_score
        if change > 0:
            best_score = scores['accuracy_score']
        print('feature:', feature, 'change:', change)
        rows.append({'step': step, 'feature': feature, **scores, 'score_change': round(change, 7)})
    return rows

def ml_train_classifier_xy(x, y, model='LGBM', seed=0, class_weight=None):
    model, name = ml_get_model(model, classify=True, seed=seed)
    model.set_params(class_weight=class_weight)
    model.fit(x, y)
    train_score = model.score(x, y)
    print('Trained classifier with {}: train_score = {:.3f}'.format(name, train_score))
    return model

def ml_train_classifier(df, model='LGBM', x_cols=None, y_col='pasc', seed=0, class_weight=None):
    if x_cols is None:
        excludes = ['person_id', 'time_to_pasc', 'pasc']
        excludes.append(y_col)
        x_cols = [c for c in df.columns if not c in excludes]

    cols = [y_col] + x_cols
    df_all = df.select(cols).toPandas()

    y = df_all.loc[:, y_col].to_numpy()
    x = df_all.drop(y_col, axis=1).to_numpy()

    return ml_train_classifier_xy(x, y, model, seed, class_weight)

def ml_run_classifier(df, model, out='prediction', x_cols=None, return_proba=True, return_spark=False):
    if x_cols is None:
        excludes = ['time_to_pasc', 'pasc']
        x_cols = [c for c in df.columns if not c in excludes]

    df_all = df.select(x_cols).toPandas()
    ret = df_all[['person_id']]
    x = df_all.drop('person_id', axis=1).to_numpy()
 
    if return_proba:
        preds = model.predict_proba(x)[:, 1]
    else:
        preds = model.predict(x)

    ret.insert(1, out, preds)

    if return_spark:
        ret = spark.createDataFrame(ret)

    return ret

def ml_eval_prediction(df, pred_col='prediction', y_col='pasc', seed=0, class_weight=None, in_proba=True, in_spark=False):
    if in_spark:
        df_all = df.select(y_col, pred_col).toPandas()
    else:
        df_all = df[[y_col, pred_col]]
        
    tests = df_all.loc[:, y_col].to_numpy()
    preds = df_all.loc[:, pred_col].to_numpy()

    if in_proba:
        probas = preds
        preds = (probas > 0.5).astype(int)
    else:
        probas = None

    uniques, counts = np.unique(tests, return_counts=True)
    average = 'binary' if len(uniques) <= 2 else 'weighted'

    scores = {}
    if probas is not None:
        fpr, tpr, thresholds = metrics.roc_curve(tests, probas, pos_label=1)
        scores['roc_auc_score'] = metrics.auc(fpr, tpr)

    naive_accuracy = max(counts) / len(tests)
    accuracy = np.sum(preds == tests) / len(tests)
    scores['accuracy_gain'] = accuracy - naive_accuracy
    metric_names = 'accuracy_score matthews_corrcoef precision_score recall_score f1_score'.split()
    for m in metric_names:
        s = None
        try:
            s = getattr(metrics, m)(tests, preds, average=average)
        except Exception:
            try:
                s = getattr(metrics, m)(tests, preds)
            except Exception:
                pass
        if s:
            scores[m] = s
    print('Evaluation metrics on {}:'.format(pred_col))
    score_dict = ml_print_metrics(scores, pred_col)

    return score_dict

##### zz vis global code start

zz_vis_selected_person = '4561121112875205437'
zz_vis_nonpasc_person = '1136670782565815091'

gender_cid_to_name = {'8507': 'Male', '8532': 'Female', '8551': 'Unknown'}

race_cid_to_name = {
    '36310364': 'Multiple races',
    '44814649': 'Other',
    '38003598': 'Black',
    '8557': 'Native Hawaiian or Other Pacific Islander',
    '8527': 'White',
    '4184984': 'Asian or Pacific islander',
    '4188159': 'Hispanic',
    '38003579': 'Chinese',
    '45878142': 'Other',
    '8522': 'Other Race',
    '44814660': 'Refuse to answer',
    '8552': 'Unknown',
    '45880900': 'More than one race',
    '4218674': 'Unknown racial group',
    '38003574': 'Asian Indian',
    '46237210': 'No information',
    '44814653': 'Unknown',
    '8516': 'Black or African American',
    '38003599': 'African American',
    '44814659': 'Multiple race',
    '8515': 'Asian',
    '38003613': 'Other Pacific Islander',
    '8657': 'American Indian or Alaska Native'
}

ethnicity_cid_to_name = {
    '38003564': 'Not Hispanic or Latino',
    '44814649': 'Other',
    '8552': 'Unknown',
    '44814650': 'No information',
    '45878545': 'Other/Unknown',
    '44814653': 'Unknown',
    '38003563': 'Hispanic or Latino'
}

zz_vis_feature_set = ['person_id', 'pasc', 'year_of_birth', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'vaccine_txn', 'vax_time_to_covid',  '3000963_2_4w_val', '3004501_1w_val', '3009744_1w-3009744_2_4w', '3012888_bc_val', '3013650_1w_val', '3013682_bc_val', '3013762_bc_val-3013762_bc7_val', '3016723_bc_val', '3020891_2_4w', '3024128_1w_val', '40762499_bc7_val-40762499_1w_val', '706163_1w-706163_2_4w', 'b026_weight_sum', 'c36714927_weight_sum', 'c37016200', 'c4203722_b1w', 'c9202_b1w', 's122805878', 's231912125_b1w', 's348930395_weight_sum', 's400852949_after', 's402442153_weight_max', 's546116974_weight_max', 's571514014_weight_max', 's689261095_after_vs_during', 's799270877_during', 's809092939_b1w', 's947341641_weight_max', 's972465851_before', 'x003_weight_max', 'x005_weight_max', 'x012_weight_max', 'x013_weight_sum', 'x014_weight_sum', 'x015', 'x016_b1w', 'b026_weight_max', 'x012_weight_sum', 'c36714927', 'b026', 's122805878_weight_sum', 's809092939', 'c9202', 'b026_a1w_vs_b1w', 'x013_weight_max', 's809092939_before', '3020891_bc_val-3020891_bc7_val', 's972465851_after_vs_before', '3020891_1w', 'c36714927_weight_max', 'x013_a1w_vs_b1w', 's400852949_a1w', 's400852949_weight_max', 'x005_weight_sum', 'x014_weight_max', 's400852949', 'x014_after', '3020891_bc-3020891_bc7', '3020891_bc', 's231912125_weight_max', 's972465851_during_vs_before', '3012888_bc', 'c9202_during', '3020891_bc7-3020891_1w', 'c4203722', '3012888_bc-3012888_bc7', 's809092939_during_vs_before', '3004501_2_4w_val', '3013762_bc_val', 'c37016200_after_vs_before', 'x016_after', 'x003_after', 'c9202_a1w_vs_b1w']

zz_vis_sel_custom_set_ids = [
    'x013',        # X: covid weak dx  (65.4)
    'x005',        # X: outpatient visit  (51.4)
    'x012',        # X: PASC broad  (51.4)
    'x015',        # X: all pain  (37.0)
    'x014',        # X: drugs for covid weak dx  (23.4)
    'x003',        # X: chest x-ray  (18.2)
    'x016',        # X: non-hospital visit  (17.8)
]

zz_vis_sel_concept_set_ids = [
    's809092939',  # A: Outpatient  (114.4)
    's231912125',  # A: Emergency Room Visits  (62.2)
    's122805878',  # S: Long COVID Clinic Visit  (53.0)
    's972465851',  # A: Hospitalization  (42.2)
    's947341641',  # A: Albuterol  (32.0)
    's400852949',  # A: Cough  (31.4)
    's571514014',  # A: Drug Corticosteroids Systemic  (29.0)
    's402442153',  # A: Fatigue  (24.8)
    's799270877',  # A: Hypertension  (24.6)
    's546116974',  # A: Inflammation Resp  (22.6)
    's689261095',  # A: Dyspnea  (20.8)
    's348930395',  # S: LongCOVIDFatigue  (20.2)
]

zz_vis_sel_bundle_ids = [
    'b026',        # B: COVID tests  (53.2)
]

zz_vis_sel_concept_ids = [
    'c36714927',   # C: Sequelae of infectious disease  (45.6)
    'c4203722',    # C: Patient encounter procedure  (34.2)
    'c37016200',   # C: Exposure to viral disease  (32.4)
    'c9202',       # C: Outpatient Visit  (28.8)
]

zz_vis_sel_lab_count_ids = [
    '706163',      # SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection  (37.0)
    '3020891',     # Body temperature  (31.6)
    '3009744',     # MCHC [Mass/volume] by Automated count  (12.4)
    '3016723',     # Creatinine [Mass/volume] in Serum or Plasma  (36.6)
    '3012888',     # Diastolic blood pressure  (53.2)
]

zz_vis_sel_lab_value_ids = [
    '3012888',     # Diastolic blood pressure  (53.2)
    '3004501',     # Glucose [Mass/volume] in Serum or Plasma  (48.2)
    '3016723',     # Creatinine [Mass/volume] in Serum or Plasma  (36.6)
    '3024128',     # Bilirubin.total [Mass/volume] in Serum or Plasma  (31.8)
    '3020891',     # Body temperature  (31.6)
    '3013682',     # Urea nitrogen [Mass/volume] in Serum or Plasma  (31.4)
    '3000963',     # Hemoglobin [Mass/volume] in Blood  (30.0)
    '3013762',     # Body weight Measured  (27.2)
    '3013650',     # Neutrophils [#/volume] in Blood by Automated count  (23.2)
    '40762499',    # Oxygen saturation in Arterial blood by Pulse oximetry  (21.4)
]

zz_vis_sel_cat_ids = zz_vis_sel_custom_set_ids + zz_vis_sel_concept_set_ids + zz_vis_sel_bundle_ids + zz_vis_sel_concept_ids

def vis_model_z_calculate_features(per_person_info, person_concept_features, person_labs):
    
    df_cat = temporal_engineered_concept_features(person_concept_features, zz_vis_sel_cat_ids, agg_concepts=False)    
    df_lab = temporal_engineered_lab_features(person_labs, zz_vis_sel_lab_count_ids, zz_vis_sel_lab_value_ids)

    df = per_person_info.select('person_id', 'pasc', 'year_of_birth', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'vaccine_txn', 'vax_time_to_covid') \
        .join(df_cat, on='person_id', how='right') \
        .join(df_lab, on='person_id', how='left')

    df = df.fillna(0)
    df = df.select(*zz_vis_feature_set)

    return df

import matplotlib
from matplotlib import pyplot as plt
import numpy as np
from shap.plots._labels import labels
from shap.utils import safe_isinstance, format_value
from shap.plots import colors
import builtins

def waterfall_custom(base_values, values, display_feature_index, feature_names, feature_data=None, show=False):
    # Turn off interactive plot
    if show is False:
        plt.ioff()

    # make sure we only have a single output to explain
    if (type(base_values) == np.ndarray and len(base_values) > 0) or type(base_values) == list:
        raise Exception("waterfall_plot requires a scalar base_values of the model output as the first "
                        "parameter, but you have passed an array as the first parameter! "
                        "Try shap.waterfall_plot(explainer.base_values[0], values[0], X[0]) or "
                        "for multi-output models try "
                        "shap.waterfall_plot(explainer.base_values[0], values[0][0], X[0]).")

    # make sure we only have a single explanation to plot
    if len(values.shape) == 2:
        raise Exception(
            "The waterfall_plot can currently only plot a single explanation but a matrix of explanations was passed!")

    # fallback feature names
    if feature_names is None:
        feature_names = np.array([labels['FEATURE'] % str(i) for i in range(len(values))])

    # init variables we use for tracking the plot locations
    num_features = len(display_feature_index) + 1
    row_height = 0.5
    rng = range(num_features - 1, -1, -1)
    order = display_feature_index
    pos_lefts = []
    pos_inds = []
    pos_widths = []
    pos_low = []
    pos_high = []
    neg_lefts = []
    neg_inds = []
    neg_widths = []
    neg_low = []
    neg_high = []
    loc = base_values + values.sum()
    yticklabels = ["" for i in range(num_features + 1)]

    # size the plot based on how many features we are plotting
    plt.gcf().set_size_inches(8, num_features * row_height + 1.5)

    # see how many individual (vs. grouped at the end) features we are plotting
    if num_features == len(values):
        num_individual = num_features
    else:
        num_individual = num_features - 1

    # compute the locations of the individual features and plot the dashed connecting lines
    for i in range(num_individual):
        sval = values[order[i]]
        loc -= sval
        if sval >= 0:
            pos_inds.append(rng[i])
            pos_widths.append(sval)
            pos_lefts.append(loc)
        else:
            neg_inds.append(rng[i])
            neg_widths.append(sval)
            neg_lefts.append(loc)
        if num_individual != num_features or i + 4 < num_individual:
            plt.plot([loc, loc], [rng[i] - 1 - 0.4, rng[i] + 0.4],
                     color="#bbbbbb", linestyle="--", linewidth=0.5, zorder=-1)

        if feature_data is None:
            yticklabels[rng[i]] = feature_names[order[i]]
        else:
            if np.issubdtype(type(feature_data[order[i]]), np.number):
                yticklabels[rng[i]] = format_value(float(feature_data[order[i]]), "%0.03f") + " = " + feature_names[order[i]]
            else:
                yticklabels[rng[i]] = feature_data[order[i]] + " = " + feature_names[order[i]]

    # add a last grouped feature to represent the impact of all the features we didn't show
    if num_features < len(values):
        yticklabels[0] = "%d other features" % (len(values) - num_features + 1)
        remaining_impact = base_values - loc
        if remaining_impact < 0:
            pos_inds.append(0)
            pos_widths.append(-remaining_impact)
            pos_lefts.append(loc + remaining_impact)
            c = colors.red_rgb
        else:
            neg_inds.append(0)
            neg_widths.append(-remaining_impact)
            neg_lefts.append(loc + remaining_impact)
            c = colors.blue_rgb

    points = pos_lefts + list(np.array(pos_lefts) + np.array(pos_widths)) + neg_lefts + \
        list(np.array(neg_lefts) + np.array(neg_widths))
    dataw = np.max(points) - np.min(points)

    # draw invisible bars just for sizing the axes
    label_padding = np.array([0.1*dataw if w < 1 else 0 for w in pos_widths])
    plt.barh(pos_inds, np.array(pos_widths) + label_padding + 0.02*dataw,
             left=np.array(pos_lefts) - 0.01*dataw, color=colors.red_rgb, alpha=0)
    label_padding = np.array([-0.1*dataw if -w < 1 else 0 for w in neg_widths])
    plt.barh(neg_inds, np.array(neg_widths) + label_padding - 0.02*dataw,
             left=np.array(neg_lefts) + 0.01*dataw, color=colors.blue_rgb, alpha=0)

    # define variable we need for plotting the arrows
    head_length = 0.08
    bar_width = 0.8
    xlen = plt.xlim()[1] - plt.xlim()[0]
    fig = plt.gcf()
    ax = plt.gca()
    xticks = ax.get_xticks()
    bbox = ax.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    width, height = bbox.width, bbox.height
    bbox_to_xscale = xlen/width
    hl_scaled = bbox_to_xscale * head_length
    renderer = fig.canvas.get_renderer()

    # draw the positive arrows
    for i in range(len(pos_inds)):
        dist = pos_widths[i]
        arrow_obj = plt.arrow(
            pos_lefts[i], pos_inds[i], builtins.max(dist-hl_scaled, 0.000001), 0,
            head_length=builtins.min(dist, hl_scaled),
            color=colors.red_rgb, width=bar_width,
            head_width=bar_width
        )

        if pos_low is not None and i < len(pos_low):
            plt.errorbar(
                pos_lefts[i] + pos_widths[i], pos_inds[i],
                xerr=np.array([[pos_widths[i] - pos_low[i]], [pos_high[i] - pos_widths[i]]]),
                ecolor=colors.light_red_rgb
            )

        txt_obj = plt.text(
            pos_lefts[i] + 0.5*dist, pos_inds[i], format_value(pos_widths[i], '%+0.02f'),
            horizontalalignment='center', verticalalignment='center', color="white",
            fontsize=12
        )
        text_bbox = txt_obj.get_window_extent(renderer=renderer)
        arrow_bbox = arrow_obj.get_window_extent(renderer=renderer)

        # if the text overflows the arrow then draw it after the arrow
        if text_bbox.width > arrow_bbox.width:
            txt_obj.remove()

            txt_obj = plt.text(
                pos_lefts[i] + (5/72)*bbox_to_xscale + dist, pos_inds[i], format_value(pos_widths[i], '%+0.02f'),
                horizontalalignment='left', verticalalignment='center', color=colors.red_rgb,
                fontsize=12
            )

    # draw the negative arrows
    for i in range(len(neg_inds)):
        dist = neg_widths[i]

        arrow_obj = plt.arrow(
            neg_lefts[i], neg_inds[i], -builtins.max(-dist-hl_scaled, 0.000001), 0,
            head_length=builtins.min(-dist, hl_scaled),
            color=colors.blue_rgb, width=bar_width,
            head_width=bar_width
        )

        if neg_low is not None and i < len(neg_low):
            plt.errorbar(
                neg_lefts[i] + neg_widths[i], neg_inds[i],
                xerr=np.array([[neg_widths[i] - neg_low[i]], [neg_high[i] - neg_widths[i]]]),
                ecolor=colors.light_blue_rgb
            )

        txt_obj = plt.text(
            neg_lefts[i] + 0.5*dist, neg_inds[i], format_value(neg_widths[i], '%+0.02f'),
            horizontalalignment='center', verticalalignment='center', color="white",
            fontsize=12
        )
        text_bbox = txt_obj.get_window_extent(renderer=renderer)
        arrow_bbox = arrow_obj.get_window_extent(renderer=renderer)

        # if the text overflows the arrow then draw it after the arrow
        if text_bbox.width > arrow_bbox.width:
            txt_obj.remove()

            txt_obj = plt.text(
                neg_lefts[i] - (5/72)*bbox_to_xscale + dist, neg_inds[i], format_value(neg_widths[i], '%+0.02f'),
                horizontalalignment='right', verticalalignment='center', color=colors.blue_rgb,
                fontsize=12
            )

    # draw the y-ticks twice, once in gray and then again with just the feature names in black
    # The 1e-8 is so matplotlib 3.3 doesn't try and collapse the ticks
    ytick_pos = list(range(num_features)) + list(np.arange(num_features)+1e-8)
    plt.yticks(ytick_pos, yticklabels[:-1] + [l.split('=')[-1] for l in yticklabels[:-1]], fontsize=13)

    # put horizontal lines for each feature row
    for i in range(num_features):
        plt.axhline(i, color="#cccccc", lw=0.5, dashes=(1, 5), zorder=-1)

    # mark the prior expected value and the model prediction
    plt.axvline(base_values, 0, 1/num_features, color="#bbbbbb", linestyle="--", linewidth=0.5, zorder=-1)
    fx = base_values + values.sum()
    plt.axvline(fx, 0, 1, color="#bbbbbb", linestyle="--", linewidth=0.5, zorder=-1)

    # clean up the main axis
    plt.gca().xaxis.set_ticks_position('bottom')
    plt.gca().yaxis.set_ticks_position('none')
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['left'].set_visible(False)
    ax.tick_params(labelsize=13)
    #plt.xlabel("\nModel output", fontsize=12)

    # draw the E[f(X)] tick mark
    xmin, xmax = ax.get_xlim()
    ax2 = ax.twiny()
    ax2.set_xlim(xmin, xmax)
    ax2.set_xticks([base_values, base_values+1e-8])  # The 1e-8 is so matplotlib 3.3 doesn't try and collapse the ticks
    ax2.set_xticklabels(["\n$E[f(X)]$", "\n$ = "+format_value(base_values, "%0.03f")+"$"], fontsize=12, ha="left")
    ax2.spines['right'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax2.spines['left'].set_visible(False)

    # draw the f(x) tick mark
    ax3 = ax2.twiny()
    ax3.set_xlim(xmin, xmax)
    # The 1e-8 is so matplotlib 3.3 doesn't try and collapse the ticks
    ax3.set_xticks([base_values + values.sum(), base_values + values.sum() + 1e-8])
    ax3.set_xticklabels(["$f(x)$", "$ = "+format_value(fx, "%0.03f")+"$"], fontsize=12, ha="left")
    tick_labels = ax3.xaxis.get_majorticklabels()
    tick_labels[0].set_transform(tick_labels[0].get_transform(
    ) + matplotlib.transforms.ScaledTranslation(-10/72., 0, fig.dpi_scale_trans))
    tick_labels[1].set_transform(tick_labels[1].get_transform(
    ) + matplotlib.transforms.ScaledTranslation(12/72., 0, fig.dpi_scale_trans))
    tick_labels[1].set_color("#999999")
    ax3.spines['right'].set_visible(False)
    ax3.spines['top'].set_visible(False)
    ax3.spines['left'].set_visible(False)

    # adjust the position of the E[f(X)] = x.xx label
    tick_labels = ax2.xaxis.get_majorticklabels()
    tick_labels[0].set_transform(tick_labels[0].get_transform(
    ) + matplotlib.transforms.ScaledTranslation(-20/72., 0, fig.dpi_scale_trans))
    tick_labels[1].set_transform(tick_labels[1].get_transform(
    ) + matplotlib.transforms.ScaledTranslation(22/72., -1/72., fig.dpi_scale_trans))

    tick_labels[1].set_color("#999999")

    # color the y tick labels that have the feature values as gray
    # (these fall behind the black ones with just the feature name)
    tick_labels = ax.yaxis.get_majorticklabels()
    for i in range(num_features):
        tick_labels[i].set_color("#999999")

    if show:
        plt.show()
    else:
        return plt.gcf()

##### zz vis global code end
