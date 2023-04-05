# typical runtime: 2m;  output shape: 426 x 57672

def model_z_start(per_person_info, person_concept_features, person_labs):

    # sel_custom_set: 7
    sel_custom_set_ids = [
        'x013',        # X: covid weak dx  (65.4)
        'x005',        # X: outpatient visit  (51.4)
        'x012',        # X: PASC broad  (51.4)
        'x015',        # X: all pain  (37.0)
        'x014',        # X: drugs for covid weak dx  (23.4)
        'x003',        # X: chest x-ray  (18.2)
        'x016',        # X: non-hospital visit  (17.8)
    ]

    # sel_concept_set: 12
    sel_concept_set_ids = [
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

    # sel_bundle: 1
    sel_bundle_ids = [
        'b026',        # B: COVID tests  (53.2)
    ]

    # sel_concept: 4
    sel_concept_ids = [
        'c36714927',   # C: Sequelae of infectious disease  (45.6)
        'c4203722',    # C: Patient encounter procedure  (34.2)
        'c37016200',   # C: Exposure to viral disease  (32.4)
        'c9202',       # C: Outpatient Visit  (28.8)
    ]

    # sel_measurement: 12
    sel_measurement_ids = [
        '3012888',     # Diastolic blood pressure  (53.2)
        '3004501',     # Glucose [Mass/volume] in Serum or Plasma  (48.2)
        '706163',      # SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection  (37.0)
        '3016723',     # Creatinine [Mass/volume] in Serum or Plasma  (36.6)
        '3024128',     # Bilirubin.total [Mass/volume] in Serum or Plasma  (31.8)
        '3020891',     # Body temperature  (31.6)
        '3013682',     # Urea nitrogen [Mass/volume] in Serum or Plasma  (31.4)
        '3000963',     # Hemoglobin [Mass/volume] in Blood  (30.0)
        '3013762',     # Body weight Measured  (27.2)
        '3013650',     # Neutrophils [#/volume] in Blood by Automated count  (23.2)
        '40762499',    # Oxygen saturation in Arterial blood by Pulse oximetry  (21.4)
        '3009744',     # MCHC [Mass/volume] by Automated count  (12.4)
    ]

    sel_cat_ids = sel_custom_set_ids + sel_concept_set_ids + sel_bundle_ids + sel_concept_ids
    print(f'Unique event features {len(sel_cat_ids)}:', sel_cat_ids)
    print(f'Unique lab features {len(sel_measurement_ids)}:', sel_measurement_ids)
    print('\nTotal unique features: {}\n'.format(len(sel_cat_ids) + len(sel_measurement_ids)))

    df_cat = temporal_engineered_concept_features(person_concept_features, sel_cat_ids, agg_concepts=False)    
    print('Time split event features:', len(df_cat.columns)-1)

    # split measurements into two sets for count and value engineering

    sel_lab_count_ids = [
        '706163',      # SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection  (37.0)
        '3020891',     # Body temperature  (31.6)
        '3009744',     # MCHC [Mass/volume] by Automated count  (12.4)
        '3016723',     # Creatinine [Mass/volume] in Serum or Plasma  (36.6)
        '3012888',     # Diastolic blood pressure  (53.2)
    ]

    sel_lab_value_ids = [
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
    
    df_lab = temporal_engineered_lab_features(person_labs, sel_lab_count_ids, sel_lab_value_ids)
    print('Time split measurement features:', len(df_lab.columns)-1)

    bias_cols = ['zip_id', 'data_partner_id']
    bias_cols += [f'd{x}' for x in [124, 399, 134, 888, 75, 526, 294, 798, 569, 828]]

    censor_cols = ['months_from_observation_end', 'covid_index_to_observation_end']
    censor_cols += ['covid_index_to_last_visit_end', 'months_from_last_visit',
        'months_from_first_visit', 'num_visits', 'max_visit_length', 'total_visit_length']

    df = per_person_info.drop(*bias_cols, *censor_cols) \
        .join(df_cat, on='person_id', how='left') \
        .join(df_lab, on='person_id', how='left')

    df = df.fillna(0)

    return df