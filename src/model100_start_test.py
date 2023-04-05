def model100_start_test(per_person_info_test, person_concept_features_test, person_labs_test):

    sel_custom_set_ids = [
        'x013',        # X: covid weak dx  (60.6)
        'x005',        # X: outpatient visit  (54.6)
        'x012',        # X: PASC broad  (54.2)
        'x003',        # X: chest x-ray  (32.6)
        'x015',        # X: all pain  (32.2)
        'x014',        # X: drugs for covid weak dx  (31.2)
        'x016',        # X: non-hospital visit  (28.0)
        'x010',        # X: NSAID  (17.6)
        'x001',        # X: maintenance fluids  (15.8)
        'x007',        # X: CT scan  (13.8)
    ]

    # sel_concept_set: 41
    sel_concept_set_ids = [
        's809092939',  # A: Outpatient  (114.0)
        's231912125',  # A: Emergency Room Visits  (57.8)
        's122805878',  # S: Long COVID Clinic Visit  (51.6)
        's400852949',  # A: Cough  (35.6)
        's972465851',  # A: Hospitalization  (34.8)
        's348930395',  # S: LongCOVIDFatigue  (31.6)
        's947341641',  # A: Albuterol  (29.2)
        's402442153',  # A: Fatigue  (28.2)
        's571514014',  # A: Drug Corticosteroids Systemic  (27.8)
        's799270877',  # A: Hypertension  (24.8)
        's961458756',  # A: Respiratory Disorder  (23.6)
        's875380843',  # S: Metabolic Disorders  (23.2)
        's546116974',  # A: Inflammation Resp  (20.6)
        's562288165',  # A: Acute Disease  (19.4)
        's689261095',  # A: Dyspnea  (19.4)
        's313998782',  # S: gestation  (18.8)
        's295952371',  # A: Drug Fentanyl  (16.8)
        's959049707',  # S: Long Hauler symptoms from LANCET paper  (16.4)
        's995642183',  # S: Immunosuppression L04  (16.4)
        's812896616',  # A: Abdominal Pain  (15.6)
        's541737679',  # A: ARDS  (15.2)
        's672055106',  # A: Cancer  (14.4)
        's777909573',  # A: Electrolyte IV  (14.0)
        's160955543',  # A: Alcohol  (13.6)
        's305217555',  # A: Vaccines  (13.0)
        's533705532',  # A: Obesity  (12.8)
        's908267911',  # A: Chest Pain  (12.6)
        's540936013',  # A: Cerebral  (11.6)
        's478273027',  # A: Renal Limited  (11.4)
        's777788102',  # S: [PASC] Antidepressant  (11.4)
        's892966630',  # A: Prednisone  (9.8)
        's216227734',  # S: Anesthesia Medications (SIANES)  (9.4)
        's481594340',  # A: Vertigo Dizziness  (8.8)
        's967701631',  # A: Mets CCI  (7.8)
        's275632871',  # A: Non Smoker  (7.6)
        's414583656',  # A: Chemotherapy Endocrine  (6.2)
        's849907658',  # A: Insomnia  (6.2)
        's379995033',  # A: Elevated Cholesterol  (6.0)
        's603185723',  # A: Drug Clonazepam  (5.2)
        's456422283',  # A: Drug Pseudoephedrine  (4.8)
        's949429075',  # A: Weight Loss  (4.6)
    ]

    # sel_bundle: 3
    sel_bundle_ids = [
        'b026',        # B: COVID tests  (55.8)
        'b017',        # B: Anticoagulants  (19.4)
        'b032',        # B: Critical visits  (12.0)
    ]

    # sel_concept: 16
    sel_concept_ids = [
        'c9202',       # C: Outpatient Visit  (44.6)
        'c36714927',   # C: Sequelae of infectious disease  (44.2)
        'c4203722',    # C: Patient encounter procedure  (38.0)
        'c37016200',   # C: Exposure to viral disease  (32.0)
        'c3019237',    # C: Chief complaint - Reported  (25.2)
        'c4223659',    # C: Fatigue  (19.4)
        'c38004250',   # C: Ambulatory Radiology Clinic / Center  (19.2)
        'c725069',     # C: Radiologic examination, chest; 2 views  (18.4)
        'c5083',       # C: Telehealth  (17.6)
        'c35605482',   # C: 2 ML ondansetron 2 MG/ML Injection  (17.0)
        'c3661408',    # C: Pneumonia caused by SARS-CoV-2  (14.6)
        'c254761',     # C: Cough  (10.4)
        'c19020053',   # C: acetaminophen 500 MG Oral Tablet  (9.6)
        'c257011',     # C: Acute upper respiratory infection  (9.2)
        'c4193704',    # C: Type 2 diabetes mellitus without complication  (8.4)
        'c1149380',    # C: fluticasone  (5.4)
    ]

    # sel_measurement: 30
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
        '3014576',     # Chloride [Moles/volume] in Serum or Plasma  (23.2)
        '3004249',     # Systolic blood pressure  (21.6)
        '40762499',    # Oxygen saturation in Arterial blood by Pulse oximetry  (21.4)
        '3019550',     # Sodium [Moles/volume] in Serum or Plasma  (20.4)
        '4099154',     # Body weight  (15.4)
        '4301868',     # Pulse rate  (14.6)
        '3024171',     # Respiratory rate  (13.8)
        '3004327',     # Lymphocytes [#/volume] in Blood by Automated count  (13.8)
        '3027018',     # Heart rate  (12.4)
        '3009744',     # MCHC [Mass/volume] by Automated count  (12.4)
        '3023103',     # Potassium [Moles/volume] in Serum or Plasma  (11.4)
        '3012030',     # MCH [Entitic mass] by Automated count  (11.2)
        '3019800',     # Troponin T.cardiac [Mass/volume] in Serum or Plasma  (9.4)
        '3028615',     # Eosinophils [#/volume] in Blood by Automated count  (9.0)
        '3023314',     # Hematocrit [Volume Fraction] of Blood by Automated count  (8.4)
        '3024929',     # Platelets [#/volume] in Blood by Automated count  (7.8)
        '3003841',     # Heart rate Peripheral artery by palpation  (7.8)
        '4154790',     # Diastolic blood pressure  (7.4)
        '3006906',     # Calcium [Mass/volume] in Serum or Plasma  (7.4)
        '4313591',     # Respiratory rate  (6.8)
    ]

    sel_cat_ids = sel_custom_set_ids + sel_concept_set_ids + sel_bundle_ids + sel_concept_ids
    print(f'Unique event features {len(sel_cat_ids)}:', sel_cat_ids)
    print(f'Unique lab features {len(sel_measurement_ids)}:', sel_measurement_ids)
    print('\nTotal unique features: {}\n'.format(len(sel_cat_ids) + len(sel_measurement_ids)))

    df_cat = temporal_engineered_concept_features(person_concept_features_test, sel_cat_ids)    
    print('Time split event features:', len(df_cat.columns)-1)

    # split measurements into two sets for count and value engineering

    sel_lab_count_ids = [
        '3012888',     # Diastolic blood pressure  (53.2)
        '706163',      # SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection  (37.0)
        '3016723',     # Creatinine [Mass/volume] in Serum or Plasma  (36.6)
        '3020891',     # Body temperature  (31.6)
        '3009744',     # MCHC [Mass/volume] by Automated count  (12.4)
        '3012030',     # MCH [Entitic mass] by Automated count  (11.2)
        '3028615',     # Eosinophils [#/volume] in Blood by Automated count  (9.0)
        '3023314',     # Hematocrit [Volume Fraction] of Blood by Automated count  (8.4)
        '3006906',     # Calcium [Mass/volume] in Serum or Plasma  (7.4)
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
        '3014576',     # Chloride [Moles/volume] in Serum or Plasma  (23.2)
        '3004249',     # Systolic blood pressure  (21.6)
        '40762499',    # Oxygen saturation in Arterial blood by Pulse oximetry  (21.4)
        '3019550',     # Sodium [Moles/volume] in Serum or Plasma  (20.4)
        '4099154',     # Body weight  (15.4)
        '4301868',     # Pulse rate  (14.6)
        '3024171',     # Respiratory rate  (13.8)
        '3004327',     # Lymphocytes [#/volume] in Blood by Automated count  (13.8)
        '3027018',     # Heart rate  (12.4)
        '3023103',     # Potassium [Moles/volume] in Serum or Plasma  (11.4)
        '3019800',     # Troponin T.cardiac [Mass/volume] in Serum or Plasma  (9.4)
        '3024929',     # Platelets [#/volume] in Blood by Automated count  (7.8)
        '3003841',     # Heart rate Peripheral artery by palpation  (7.8)
        '4154790',     # Diastolic blood pressure  (7.4)
        '4313591',     # Respiratory rate  (6.8)
    ]
    
    df_lab = temporal_engineered_lab_features(person_labs_test, sel_lab_count_ids, sel_lab_value_ids)
    print('Time split measurement features:', len(df_lab.columns)-1)
 
    df = per_person_info_test \
        .join(df_cat, on='person_id', how='left') \
        .join(df_lab, on='person_id', how='left')

    df = df.fillna(0)

    return df