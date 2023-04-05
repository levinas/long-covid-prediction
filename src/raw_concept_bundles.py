# typical runtime: 1m;  output shape: 4 x 580

def raw_concept_bundles():
    # import concept set bundles (categories) from:
    # https://unite.nih.gov/workspace/fusion/spreadsheet/ri.fusion.main.document.5ceffa4e-95e0-4097-a03a-7633de86eefe/spreadsheet/6960788842014549

    csv_string = """tag_name,concept_set_name,best_version_id,added_to_bundle_at
Aspirin,Aspirin 81mg-325mg dosage forms,380832941,12/1/21 12:00
ACE inhibitors,[AKI]enalapril,801947920,12/1/21 12:00
ACE inhibitors,[AKI]lisinopril,763943060,12/1/21 12:00
ACE inhibitors,[AKI]Benazepril,250324161,12/1/21 12:00
ACE inhibitors,[AKI]fosinopril,566238709,12/1/21 12:00
ACE inhibitors,[AKI]moexipril,162634454,12/1/21 12:00
ACE inhibitors,[AKI]perindopril,397240401,12/1/21 12:00
ACE inhibitors,[AKI]quinapril,59923084,12/1/21 12:00
ACE inhibitors,[AKI]ramipril,402462598,12/1/21 12:00
ACE inhibitors,[AKI]trandolapril,663697975,12/1/21 12:00
ACE inhibitors,[CVDT] Aliskerin,311234989,12/1/21 12:00
ARBs - angiotensin II receptor blockers,[AKI]azilsartan,468581880,12/1/21 12:00
ARBs - angiotensin II receptor blockers,[AKI]candesartan,353406235,12/1/21 12:00
ARBs - angiotensin II receptor blockers,[AKI]eprosartan,548028680,12/1/21 12:00
ARBs - angiotensin II receptor blockers,[AKI]irbesartan,860361329,12/1/21 12:00
ARBs - angiotensin II receptor blockers,[AKI]losartan,773017960,12/1/21 12:00
ARBs - angiotensin II receptor blockers,[AKI]olmesartan,892468750,12/1/21 12:00
ARBs - angiotensin II receptor blockers,[AKI]valsartan,400268646,12/1/21 12:00
K-sparing diuretics,[CVDT] Spironolactone,101047351,12/1/21 12:00
K-sparing diuretics,[CVDT] Eplerenone,743687863,12/1/21 12:00
K-sparing diuretics,[CVDT] Triamterene (K sparing diuretic),313111945,12/1/21 12:00
K-sparing diuretics,[CVDT] Amiloride (K sparing diuretic,652895929,12/1/21 12:00
K-sparing diuretics,[CVDT] Canrenoate (K sparing diuretic),292777476,12/1/21 12:00
K-sparing diuretics,[CVDT] Canrenone (K sparing diuretic),243304242,12/1/21 12:00
Platelet inhibitors,[CVDT] Clopidogrel,504215815,12/1/21 12:00
Platelet inhibitors,[CVDT] Prasugrel,203728796,12/1/21 12:00
Platelet inhibitors,[CVDT] Ticlopidine,879320451,12/1/21 12:00
Platelet inhibitors,[CVDT] Dipyridamole,618551249,12/1/21 12:00
Platelet inhibitors,[CVDT] Ticagrelor,2918381,12/1/21 12:00
Platelet inhibitors,[CVDT] Cangrelor,717859058,12/1/21 12:00
Antiarrhythmics,[CVDT] Amiodarone (IV and enteral),265298820,12/1/21 12:00
Antiarrhythmics,[CVDT] Propafenone,621450514,12/1/21 12:00
Antiarrhythmics,[CVDT] Sotalol,271301028,12/1/21 12:00
Antiarrhythmics,[CVDT] Dronedarone,299694530,12/1/21 12:00
Antiarrhythmics,[CVDT] Flecainide,356789070,12/1/21 12:00
Antiarrhythmics,[CVDT] Dofetilide,651734655,12/1/21 12:00
Statins,[CVDT] Pravastatin,750055299,12/1/21 12:00
Statins,[CVDT] Fluvastatin,176953155,12/1/21 12:00
Statins,[CVDT] Atorvastatin,963204452,12/1/21 12:00
Statins,[CVDT] Rosuvastatin,823104101,12/1/21 12:00
Statins,[CVDT] Simvastatin,718641699,12/1/21 12:00
Statins,[CVDT] Lovastatin,956703593,12/1/21 12:00
Statins,[CVDT] Cerivastatin,393296414,12/1/21 12:00
Statins,[CVDT} Pitavastatin,92126512,12/1/21 12:00
Statins,[CVDT] Ezetimibe,734560185,12/1/21 12:00
PCSK9 inhibitors,[CVDT] alirocumab (PCSK9 Inhibitor),909547737,12/1/21 12:00
PCSK9 inhibitors,[CVDT] evolocumab (PCSK9 Inhibitor),823138506,12/1/21 12:00
Long-acting nitrates,[CVDT] Isosorbide (mononitrate),271787229,12/1/21 12:00
Long-acting nitrates,[CVDT] Isosorbide (Dinitrate),269968789,12/1/21 12:00
Long-acting nitrates,[CVDT] Bidil,646870758,12/1/21 12:00
Long-acting nitrates,[CVDT] Digoxin (IV and enteral),645959029,12/1/21 12:00
Long-acting nitrates,[CVDT] Hydralazine (IV and enteral),312013470,12/1/21 12:00
ARNi - Angiotensin Receptor-Neprilysin Inhibitors,[CVDT] Sacubitril,199292654,12/1/21 12:00
ARNi - Angiotensin Receptor-Neprilysin Inhibitors,[CVDT] Entresto (sacubitril and valsartan),74327945,12/1/21 12:00
ARNi - Angiotensin Receptor-Neprilysin Inhibitors,SGLT2i (see note),266373873,12/1/21 12:00
ARNi - Angiotensin Receptor-Neprilysin Inhibitors,[CVDT] Canagliflozin,559858734,12/1/21 12:00
ARNi - Angiotensin Receptor-Neprilysin Inhibitors,[CVDT] Dapagliflozin,393740099,12/1/21 12:00
ARNi - Angiotensin Receptor-Neprilysin Inhibitors,[CVDT] Empagliflozin,366217695,12/1/21 12:00
ARNi - Angiotensin Receptor-Neprilysin Inhibitors,[CVDT] Ertugliflozin,80819041,12/1/21 12:00
Loop diuretics,[CVDT] Furosemide (IV and enteral),29425872,12/1/21 12:00
Loop diuretics,[CVDT] Bumetanide (IV and enteral),55581354,12/1/21 12:00
Loop diuretics,[CVDT] Torsemide,174644251,12/1/21 12:00
Loop diuretics,[CVDT] Ethacrynic acid,557022664,12/1/21 12:00
Alpha antagonists (BPH agents excluded),[CVDT] Prazosin,816254841,12/1/21 12:00
Alpha antagonists (BPH agents excluded),[CVDT] Doxazosin,30352425,12/1/21 12:00
Alpha antagonists (BPH agents excluded),[CVDT] Terazosin,330715582,12/1/21 12:00
Thiazide/Thiazide-like diuretics,{CVDT} Bendroflumethazide,576348184,12/1/21 12:00
Thiazide/Thiazide-like diuretics,[CVDT] Chlorthiazide,380007667,12/1/21 12:00
Thiazide/Thiazide-like diuretics,ARIScience – Drug Hydrochlorothiazide – AD,65784508,12/1/21 12:00
Thiazide/Thiazide-like diuretics,[CVDT] Chlorthalidone,608437806,12/1/21 12:00
Thiazide/Thiazide-like diuretics,[CVDT] Indapamide,671562747,12/1/21 12:00
Thiazide/Thiazide-like diuretics,[CVDT] Hydroflumethiazide,931256591,12/1/21 12:00
Thiazide/Thiazide-like diuretics,[CVDT] polythiazide,308298277,12/1/21 12:00
Thiazide/Thiazide-like diuretics,[CVDT] Cyclothiazide,850152609,12/1/21 12:00
Thiazide/Thiazide-like diuretics,[CVDT] Metolazone,792464508,12/1/21 12:00
Calcium channel blockers,[DATOS] Calcium Channel Blockers (all included),188852393,12/1/21 12:00
Calcium channel blockers,[DATOS] Calcium Channel Blockers (only verapamil and diltiazem),270650911,12/1/21 12:00
Calcium channel blockers,[CVDT] Diltiazem,91730626,12/1/21 12:00
Calcium channel blockers,[CVDT] Verapamil,381089356,12/1/21 12:00
Calcium channel blockers,[DATOS] Calcium Channel Blockers (only dihydropyridines),690529022,12/1/21 12:00
Calcium channel blockers,[CVDT] Amldipine,547766334,12/1/21 12:00
Calcium channel blockers,[CVDT] Bepridil,681410772,12/1/21 12:00
Calcium channel blockers,[CVDT] Felodipine,342285034,12/1/21 12:00
Calcium channel blockers,[CVDT] Isradipine,111500277,12/1/21 12:00
Calcium channel blockers,[CVDT] Nicardipine,531747104,12/1/21 12:00
Calcium channel blockers,[CVDT] Nifedipine,851520207,12/1/21 12:00
Calcium channel blockers,[CVDT] Nisoldipine,618727782,12/1/21 12:00
Calcium channel blockers,[CVDT] Nitrendipine,409269976,12/1/21 12:00
Calcium channel blockers,[CVDT] Nimodipine,466482222,12/1/21 12:00
Beta blockers,[DATOS] Beta Blockers (see note),788577109,12/1/21 12:00
Beta blockers,[CVDT] Atenolol,227118155,12/1/21 12:00
Beta blockers,[CVDT] Betaxolol oral,922243817,12/1/21 12:00
Beta blockers,[CVDT] Bisoprolol,556076497,12/1/21 12:00
Beta blockers,[CVDT] Metoprolol,86945105,12/1/21 12:00
Beta blockers,[CVDT] Nebivolol,848064854,12/1/21 12:00
Beta blockers,[CVDT] Nadolol,981490371,12/1/21 12:00
Beta blockers,[CVDT] Propranolol,860771029,12/1/21 12:00
Beta blockers,[CVDT] Acebutalol,476548662,12/1/21 12:00
Beta blockers,[CVDT] Penbutolol,113144832,12/1/21 12:00
Beta blockers,[CVDT] Pindolol,945428030,12/1/21 12:00
Beta blockers,[CVDT] Carvedilol,745170047,12/1/21 12:00
Beta blockers,[CVDT] Labetalol,955592916,12/1/21 12:00
Minoxidil,Minoxidil,844355286,12/1/21 12:00
Central alpha 2 agonists,[CVDT] Clonidine,962864682,12/1/21 12:00
Central alpha 2 agonists,[CVDT] Methyldopa,956948885,12/1/21 12:00
Central alpha 2 agonists,[CVDT] Guanfacine,459998698,12/1/21 12:00
Central alpha 2 agonists,[CVDT] Guanabenz,409983325,12/1/21 12:00
Central alpha 2 agonists,[CVDT] Tizanidine,488913787,12/1/21 12:00
Alpha antagonists,Alpha antagonists (BPH agents excluded),530294560,12/1/21 12:00
Alpha antagonists,[CVDT] Prazosin,816254841,12/1/21 12:00
Alpha antagonists,[CVDT] Doxazosin,30352425,12/1/21 12:00
Alpha antagonists,[CVDT] Terazosin,330715582,12/1/21 12:00
Anticoagulants,[CVDT] Enoxaparin,858278110,12/1/21 12:00
Anticoagulants,[CVDT] Fondaparinux,256831252,12/1/21 12:00
Anticoagulants,[CVDT] Dalteparin,224241803,12/1/21 12:00
Anticoagulants,[CVDT] Warfarin,441951686,12/1/21 12:00
Anticoagulants,[CVDT] Apixaban,366168231,12/1/21 12:00
Anticoagulants,[Pitt N3C] Dabigatran,23600781,12/1/21 12:00
Anticoagulants,[CVDT] Edoxaban,304660684,12/1/21 12:00
Anticoagulants,[CVDT] Rivaroxaban,30142640,12/1/21 12:00
Anticoagulants,Heparin,40510035,12/1/21 12:00
Anticoagulants,[Pitt N3C] Heparin Sodium (UFH),357794478,12/1/21 12:00
Hemodynamic/vasoactive agents,N3C Vasopressin Injectable (gtt),308291386,12/1/21 12:00
Hemodynamic/vasoactive agents,N3C Norepinephrine Injectable (gtt),9512899,12/1/21 12:00
Hemodynamic/vasoactive agents,N3C Phenylephrine Injectable (gtt),226615355,12/1/21 12:00
Hemodynamic/vasoactive agents,N3C Epinephrine,138921458,12/1/21 12:00
Hemodynamic/vasoactive agents,[DATOS] [4CE Severity] Epinephrine,574282982,12/1/21 12:00
Hemodynamic/vasoactive agents,N3C Dopamine Injectable,89980583,12/1/21 12:00
Hemodynamic/vasoactive agents,N3C Dobutamine Injectable,41638290,12/1/21 12:00
Hemodynamic/vasoactive agents,N3C Milrinone Injectable (gtt),316286704,12/1/21 12:00
COVID treatment - repurposed meds,Remdesivir,719693192,1/4/22 12:00
COVID treatment - repurposed meds,Ribavirin,27021448,1/4/22 12:00
COVID treatment - repurposed meds,Lonavir/Ritonavir,165611849,1/4/22 12:00
COVID treatment - repurposed meds,Favipiravir,775623550,1/4/22 12:00
COVID treatment - repurposed meds,Chloroquine,818210864,1/4/22 12:00
COVID treatment - repurposed meds,N3C Hydroxychloroquine,807281242,1/4/22 12:00
COVID treatment - repurposed meds,N3C Azithromycin excluding Ophthalmic,820662067,1/4/22 12:00
COVID treatment - repurposed meds,interferonbeta-1a,359012050,1/4/22 12:00
COVID treatment - repurposed meds,interferonbeta-1b,531467540,1/4/22 12:00
Chronic kidney diseases,ckd stage 3,633400665,1/4/22 12:00
Chronic kidney diseases,ckd stage 4,778290097,1/4/22 12:00
Chronic kidney diseases,ckd stage 5,742328432,1/4/22 12:00
Chronic kidney diseases,ckd esrd/dialysis,336612223,1/4/22 12:00
Chronic kidney diseases,ckd s/p transplant,527931409,1/4/22 12:00
Corticosteroids,Hydrocortisone (oral and injectable),964980879,1/4/22 12:00
Corticosteroids,hydrocortisone (systemic),932266800,1/4/22 12:00
Corticosteroids,methylprednisolone,940495880,1/4/22 12:00
Corticosteroids,Prednisone,783588396,1/4/22 12:00
Corticosteroids,"N3C Dexamethasone Non-Otic, Non-Opthalmic",749226335,1/4/22 12:00
Corticosteroids,Prednisolone,804783116,1/4/22 12:00
COVID treatment - repurposed meds,tocilizumab,276204116,1/4/22 12:00
COVID treatment - repurposed meds,sirulumab,349633484,1/4/22 12:00
COVID treatment - repurposed meds,Anakinra,614076948,1/4/22 12:00
Monoclonal antibodies,"Bamlanivimab Monotherapy (Injection route of admin, 35 mg/ml minimum dosage)",221251863,1/4/22 12:00
Monoclonal antibodies,"Etesevimab Monotherapy (Injection route of admin, 35 mg/ml minimum dosage)",985547691,1/4/22 12:00
Monoclonal antibodies,"Casirivimab Monotherapy (Injection route of admin, 120 MG/ML dose minimum)",211082324,1/4/22 12:00
Monoclonal antibodies,"Casirivimab + Imdevimab (Injection route of admin, 120 mg/ml minimum dosage)",204936358,1/4/22 12:00
Cancer,lung cancer,363230049,1/4/22 12:00
Cancer,breast cancer,409767469,1/4/22 12:00
Cancer,prostate cancer,424135997,1/4/22 12:00
Cancer,colorectal cancer,294360109,1/4/22 12:00
Cancer,MALIGNANT CANCER,984458646,1/4/22 12:00
Cancer,[Cardioonc] Liquid Malignancies (v2),909803804,1/4/22 12:00
Cancer treatments,Cytotoxic Chemotherapy,860935468,1/4/22 12:00
Cancer treatments,Immunotherapy Chemotherapy,121020271,1/4/22 12:00
Cancer treatments,Endocrine Chemotherapy,256517153,1/4/22 12:00
Cancer treatments,Targeted Chemotherapy,945843137,1/4/22 12:00
Diabetes treatments,Metformin,677036102,1/4/22 12:00
Diabetes treatments,Sulfanylurea,880450058,1/4/22 12:00
Diabetes treatments,dpp4,82538978,1/4/22 12:00
Diabetes treatments,glp1,167455542,1/4/22 12:00
Diabetes treatments,sglt2,624781538,1/4/22 12:00
Diabetes treatments,thiazo,165856330,1/4/22 12:00
Diabetes treatments,biguanides,267122637,1/4/22 12:00
Diabetes treatments,alpha_glucosidase,440051099,1/4/22 12:00
Diabetes treatments,meglitinide,312624342,1/4/22 12:00
Diabetes treatments,other_diabetes,912920320,1/4/22 12:00
Diabetes treatments,insulin,91074072,1/4/22 12:00
Diabetes,t1dm,671676235,1/4/22 12:00
Diabetes,t2dm,794639872,1/4/22 12:00
Diabetes,other_dm,29551606,1/4/22 12:00
Diabetes,gestational_dm,382227453,1/4/22 12:00
Diabetes labs,a1c_gt65,381434987,1/4/22 12:00
Diabetes treatments,a1c_max,381434987,1/4/22 12:00
Diabetes treatments,glucose,59698832,1/4/22 12:00
Diabetes treatments,creatinine24,676035513,1/4/22 12:00
Diabetes treatments,crp24,371622342,1/4/22 12:00
Diabetes treatments,d_dimer24,475972797,1/4/22 12:00
Diabetes treatments,lactate24,400660753,1/4/22 12:00
Diabetes kidney disease,ckd3,633400665,1/4/22 12:00
Diabetes kidney disease,ckd4,778290097,1/4/22 12:00
Diabetes kidney disease,ckd5,742328432,1/4/22 12:00
Diabetes kidney disease,ckd_esrd,336612223,1/4/22 12:00
Diabetes kidney disease,ckd_transplant,527931409,1/4/22 12:00
COVID tests,ATLAS SARS-CoV-2 rt-PCR and AG,386776576,1/4/22 12:00
COVID tests,Atlas #818 [N3C] CovidAntibody retry,263281373,1/4/22 12:00
COVID tests,covid19diagnosis,182296037,1/4/22 12:00
COVID tests,CovidAmbiguous,98126547,1/4/22 12:00
COVID qualitative results,result positive,754723376,1/4/22 12:00
COVID qualitative results,result negative,21890932,1/4/22 12:00
COVID qualitative results,result unknown,913889242,1/4/22 12:00
COVID vaccines,[EXPORT] COVID-19 Vaccines,960493440,1/4/22 12:00
COVID vaccines,Covid vaccine Moderna (v1),100751863,1/4/22 12:00
COVID vaccines,Covid vaccine Pfizer (v1),935484078,1/4/22 12:00
COVID vaccines,Covid Vaccine Janssen (v2),247572507,1/4/22 12:00
Blood gases,Arterial PH,430703468,1/4/22 12:00
Blood gases,Arterial O2,757880898,1/4/22 12:00
Blood gases,Arterial CO2,577942305,1/4/22 12:00
Blood gases,Pulse Ox,735794881,1/4/22 12:00
Blood gases,Arterial Pa O2,369811251,1/4/22 12:00
Blood gases,FIO2,545825848,1/4/22 12:00
Ventilation invasive treatments,ECMO,415149730,1/4/22 12:00
Ventilation invasive treatments,IMV,469361388,1/4/22 12:00
Glascow Coma Scale score,Glascow coma scale motor,962949507,1/4/22 12:00
Glascow Coma Scale score,GCS eye,724564678,1/4/22 12:00
Glascow Coma Scale score,GCS total,266742017,1/4/22 12:00
Glascow Coma Scale score,GCS verbal,236332404,1/4/22 12:00
External Authority Valuesets,[VSAC] Hemiplegia Symptoms,265687536,1/7/22 12:00
Critical visits,Inpatient or critical care visit,439506059,1/4/22 12:00
Critical visits,ECMO,415149730,1/4/22 12:00
Critical visits,IMV,469361388,1/4/22 12:00
Default,[HCUP] Acute myocardial infarction (v1),952598834,3/7/22 15:00
Default,[HCUP] Bipolar and related disorders (v1),405733646,3/7/22 15:00
Default,[HCUP] Chronic obstructive pulmonary disease and bronchiectasis (v1),15182496,3/7/22 15:00
Default,[HCUP] Complications of acute myocardial infarction (v1),749854788,3/7/22 15:00
Default,[HCUP] Depressive disorders (v1),537591858,3/7/22 15:00
Default,[HCUP] Diabetes mellitus with complication (v1),915573730,3/7/22 15:00
Default,[HCUP] Diabetes mellitus without complication (v1),617427516,3/7/22 15:00
Default,[HCUP] Diseases of white blood cells (v1),395987449,3/7/22 15:00
Default,"[HCUP] Disruptive, impulse-control and conduct disorders (v1)",953626701,3/7/22 15:00
Default,[HCUP] Gastroduodenal ulcer (v2),749303275,3/7/22 15:00
Default,[HCUP] Hepatic failure (v1),79164956,3/7/22 15:00
Default,[HCUP] Immunity disorders (v1),746266240,3/7/22 15:00
Default,[HCUP] Nephritis; nephrosis; renal sclerosis (v1),986085724,3/7/22 15:00
Default,[HCUP] Other specified and unspecified diseases of kidney and ureters (v1),739249986,3/7/22 15:00
Default,[HCUP] Other specified and unspecified liver disease (v1),102554893,3/7/22 15:00
Default,[HCUP] Other specified and unspecified lower respiratory disease (v1),841969278,3/7/22 15:00
Default,[HCUP] Other specified and unspecified mood disorders (v1),8133362,3/7/22 15:00
Default,[HCUP] Peripheral and visceral vascular disease (v2),590229661,5/16/22 15:00
Default,[HCUP] Proteinuria (v3),83982865,3/7/22 15:00
Default,[HCUP] Regional enteritis and ulcerative colitis (v1),168914400,3/7/22 15:00
Default,[HCUP] Schizophrenia spectrum and other psychotic disorders (v1),894827268,3/7/22 15:00
Default,[HCUP] Tobacco-related disorders (v1),144374255,3/7/22 15:00
Default,[VSAC] Veteran Status Procedures (v2),385390926,4/27/22 16:00
Default,[VSAC] Social Connection Diagnoses (v2),897240313,4/25/22 15:00
Default,[VSAC] Social Connection Goals (v2),855674119,4/25/22 15:00
Default,[VSAC] Social Connection Interventions (v2),777183738,4/25/22 15:00
Default,[VSAC] Social Determinants of Health General Interventions (V2),330538913,4/21/22 16:00
Default,[VSAC] Unemployment Procedures (v2),771404868,4/27/22 16:00
Default,[VSAC] Transportation Insecurity Procedures (v2),545210402,4/27/22 16:00
Default,[VSAC] Stress Diagnoses (v2),215396858,4/25/22 15:00
Default,[VSAC] Stress Procedures (v2),350566031,4/25/22 12:00
Default,[VSAC] Stress Goals (v2),16814325,4/25/22 12:00
Default,[VSAC] Stress Interventions (v2),967156356,4/25/22 12:00
Default,[VSAC] Social Determinants of Health Procedures (v2),687687213,4/27/22 15:00
Default,[VSAC] Social Determinants of Health Goals (v2),201986476,4/27/22 15:00
Default,[VSAC] Social Connection Procedures (v2),250470842,4/25/22 15:00
Default,[VSAC] Material Hardship Procedures (v2),319293562,4/25/22 15:00
Default,[VSAC] Material Hardship Goals (v2),457082396,4/25/22 15:00
Default,[VSAC] Less than high school education Goals (v2),271056968,4/25/22 12:00
Default,[VSAC] Less than high school education Procedures (v2),159470853,4/25/22 12:00
Default,[VSAC] Intimate Partner Violence Procedures (v2),436464307,4/25/22 15:00
Default,[VSAC] Intimate Partner Violence Diagnoses (v2),270414638,4/25/22 12:00
Default,[VSAC] Intimate Partner Violence Goals (v2),51267658,4/25/22 15:00
Default,[VSAC] Intimate Partner Violence Interventions (v2),989216425,4/25/22 12:00
Default,[VSAC] Inadequate Housing Goals (v2),396155663,4/22/22 15:00
Default,[VSAC] Inadequate Housing Procedures (v2),280734396,4/22/22 15:00
Default,[VSAC] Inadequate Housing Diagnoses (v2),597983430,4/22/22 12:00
Default,[VSAC] Inadequate Housing Interventions (v2),161425022,4/22/22 12:00
Default,[VSAC] Housing Instability Diagnoses (v2),829037839,4/23/22 12:00
Default,[VSAC] Housing Instability Procedures (v2),346277904,4/22/22 15:00
Default,[VSAC] Homelessness Diagnoses (v2),597460996,4/22/22 12:00
Default,[VSAC] Homelessness Procedures (v2),354899212,4/22/22 12:00
Default,[VSAC] Food Insecurity Diagnoses (v3),870007552,4/22/22 12:00
Default,[VSAC] Food Insecurity Interventions (v2),601354999,4/22/22 20:00
Default,[VSAC] Food Insecurity Procedures (v2),288235224,4/22/22 20:00
Default,[VSAC] Financial Insecurity Goals (v3),751745849,4/22/22 12:00
Default,[VSAC] Financial Insecurity Procedures (v2),378170869,4/22/22 12:00
Default,[VSAC] Elder Abuse Diagnoses (v3),256103397,4/22/22 12:00
Default,[VSAC] Elder Abuse Interventions (v2),450055230,4/22/22 12:00
Default,[VSAC] Elder Abuse Procedures (v2),988090927,4/22/22 12:00
Default,[VSAC] Elder Abuse Goals (v2),243655815,4/22/22 12:00
Default,[VSAC] Ischemic Stroke_Thrombosis (v2),590182092,4/27/22 15:00
Default,[VSAC] Ischemic Stroke_Other (v2),293213778,4/27/22 15:00
Default,[VSAC] Veteran Status Diagnoses (v2),991760905,4/27/22 16:00
Default,[VSAC] Unemployment Diagnoses (v2),910742250,4/27/22 12:00
Default,[VSAC] Unemployment Goals (v2),266069787,4/27/22 15:00
Default,[VSAC] Unemployment Interventions (v2),83471041,4/27/22 15:00
Default,[VSAC] Overweight or Obese (v3),616018686,4/20/22 20:00
Default,[VSAC] Obesity (v2),466714408,3/28/22 10:00
Default,"[VSAC] Bells Palsy, codes for AE reporting (v2)",41656421,4/20/22 20:00
Default,[VSAC] Encephalitis (Disorders) (v2),82523982,4/20/22 20:00
Default,[VSAC] Meningitis (v2),197886613,4/20/22 20:00
Default,[VSAC] Congenital heart disease (v2),892516138,4/21/22 15:00
Default,[VSAC] Parkinson's Disease (v2),251805604,4/20/22 20:00
Default,[VSAC] Multiple Sclerosis (v2),775793929,4/21/22 15:00
Default,[VSAC] epilepsy (active) (v2),948184761,3/10/22 16:00
Default,[VSAC] Headache (v3),730992420,3/16/22 16:00
Default,[VSAC] Brain Tumor (v3),758030341,3/16/22 16:00
Default,[VSAC] Pulmonary Embolism or DVT (v3),631335820,3/16/22 16:00
Default,[VSAC] Sickle Cell Disease (v3),765959200,3/10/22 16:00
Default,[VSAC] Pulmonary Hypertension (v3),86242485,3/10/22 16:00
Default,[VSAC] Hematopoietic stem cell transplant (v3),704911067,3/10/22 16:00
Default,[VSAC] Material Hardship Diagnoses (v2),377040058,4/25/22 16:00
Default,[VSAC] Mental Health Disorder (v3),603465835,3/10/22 16:00
Default,[VSAC] Eclampsia (v3),13453334,3/10/22 16:00
Default,[VSAC] Bleeding Disorder (v2),762981309,3/13/22 17:00
Default,[VSAC] Bariatric Surgery (v2),706169495,3/13/22 17:00
Default,[VSAC] Atrial Fibrillation/Flutter (v2),624950910,3/13/22 17:00
Default,[VSAC] Antithrombotic Therapy_Antiplatelet (v2),307887059,3/13/22 17:00
Default,[VSAC] Anemia (v2),575004035,3/13/22 17:00
Default,[VSAC] Air and Thrombotic Embolism (v1),214302145,3/13/22 17:00
Default,[VSAC] Acute Respiratory Distress Syndrome (v1),168521845,3/13/22 17:00
Default,[VSAC] Acute Renal Failure (v1),191950997,3/13/22 17:00
Default,[VSAC] Acute Myocardial Infarction (AMI) (v1),369143650,3/13/22 17:00
Default,[VSAC] Acute Heart Failure (v1),409804608,3/13/22 17:00
Default,[VSAC] Active Bleeding (v1),218099832,3/13/22 17:00
Default,[VSAC] Bronchiectasis (v1),555855275,3/13/22 17:00
Default,[VSAC] Interstitial lung disease (v1),158058925,3/13/22 17:00
Default,[VSAC] Immunodeficient Conditions (v1),777421664,3/13/22 17:00
Default,[VSAC] Guillain Barre Syndrome (v1),728213334,3/13/22 17:00
Default,[VSAC] Congenital Anomalies S (v1),773126867,3/13/22 17:00
Default,[VSAC] Tuberculosis (Disorders) (v1),942934994,3/13/22 17:00
Default,[VSAC] Thalassemia (v2),35406191,3/13/22 17:00
Default,[VSAC] Smoking or Cigarette Use (v2),727648761,3/13/22 17:00
Default,[VSAC] Chronic Kidney Disease All Stages (1 through 5) (v1),997506752,3/13/22 17:00
Default,[VSAC] Type II Diabetes (v2),484742674,3/13/22 17:00
Default,[VSAC] Substance Use Disorder (v3),509084611,3/13/22 17:00
Default,[VSAC] Major Transplant (v3),276258156,3/14/22 13:00
Default,[VSAC] Kidney Transplant (v2),751413170,3/14/22 13:00
Default,[VSAC] HIV 1 (v1),381333442,3/14/22 17:00
Default,[VSAC] Diabetes (v1),588957858,3/14/22 17:00
Default,[VSAC] Ischemic Stroke_Embolism (v1),192685592,3/14/22 17:00
Default,[VSAC] Asthma (v1),602584947,3/14/22 17:00
Default,[VSAC] Pregnancy (v2),446056982,3/14/22 17:00
Default,[VSAC] Myocardial Infarction (v1),673422393,3/14/22 17:00
Default,[VSAC] Hypothyroidism (v1),487697357,3/14/22 17:00
Default,[VSAC] Hypotension (v1),908202216,3/14/22 17:00
Default,[VSAC] Diastolic Heart Failure (v1),591383270,3/14/22 17:00
Default,[VSAC] Chronic Stable Angina (v1),521595143,3/14/22 17:00
Default,[VSAC] Cardiac Surgery (v1),416233275,3/14/22 17:00
Default,[VSAC] Bradycardia (v1),740983957,3/14/22 17:00
Default,[VSAC] Arrhythmia (v1),187292967,3/14/22 17:00
Default,[VSAC] Disseminated Intravascular Coagulation (v2),566478455,3/14/22 17:00
Default,[VSAC] Heart Failure (v2),142651859,3/15/22 15:00
Default,[VSAC] Coronary Artery Disease No MI (v2),866452655,3/15/22 15:00
Default,[VSAC] Chronic Liver Disease (v4),431382310,3/15/22 15:00
Default,[VSAC] Chronic Obstructive Pulmonary Disease (v4),864746330,3/15/22 15:00
Default,[VSAC] Cystic Fibrosis (v3),146508152,3/15/22 15:00
Default,[VSAC] Comorbid Conditions for Respiratory Conditions (v7),689333457,3/15/22 15:00
Default,[VSAC] Type 1 Diabetes (v4),673524081,3/15/22 15:00
Default,[VSAC] Inhaled Corticosteroids (v2),886083312,3/15/22 15:00
Default,[VSAC] Cardiomyopathy (v4),886099282,4/20/22 17:00
Default,[VSAC] Cardiac Disease (v2),402952445,3/15/22 15:00
Default,[VSAC] Brain Injury (v2),102561251,3/15/22 15:00
Default,[VSAC] Hypertension (v3),150036573,5/18/22 15:00
Default,[VSAC] Neuromuscular Disease (v2),881239411,3/15/22 15:00
Default,[VSAC] Sepsis (v2),782040890,3/15/22 15:00
Default,[VSAC] Immunosuppressant medications for inflammatory bowel disease or rheumatoid arthritis (v2),167015188,3/15/22 15:00
Default,"[VSAC] Corticosteroids, Systemic (v2)",983041019,3/15/22 15:00
Default,[VSAC] Dementia & Mental Degenerations (v2),100358347,3/16/22 10:00
Default,[VSAC] Cancer (v9),122145229,4/21/22 11:00
Default,[VSAC] Congenital Malformations (v3),568565870,3/16/22 11:00
Default,[VSAC] Mental Behavioral and Neurodevelopmental Disorders (v3),584855798,3/16/22 11:00
Default,[VSAC] Blood Transfusion (v2),800491580,3/16/22 15:00
Default,[VSAC] Alzheimer's Disease (ICD10CM) (v2),287931644,4/21/22 17:00
Default,[VSAC] Amyotrophic Lateral Sclerosis (ALS) and Related Motor Neuron Disease (ICD10CM) (v2),33647039,4/21/22 17:00
Default,[VSAC] Hemiplegia Symptoms (v3),798820225,4/27/22 17:00
Default,[VSAC] Cerebrovascular Diseases (v3),365765823,4/21/22 17:00
Default,[VSAC] Cerebrovascular Disease Stroke or TIA (v1),738973826,4/27/22 17:00
Default,[VSAC] COVID19 ICD10CM Value Set for Down's Syndrome (v2),527270407,4/21/22 17:00
Default,[VSAC] Rheumatoid Arthritis (v1),26562308,3/28/22 17:00
Default,[VSAC] Autoimmune Disease (v3),666627336,3/28/22 17:00
Default,[VSAC] Metastatic Cancer (v1),660401755,3/28/22 17:00
External Authority Valuesets,[HCUP] Acute myocardial infarction (v1),952598834,3/16/22 15:00
External Authority Valuesets,[HCUP] Bipolar and related disorders (v1),405733646,3/16/22 15:00
External Authority Valuesets,[HCUP] Chronic obstructive pulmonary disease and bronchiectasis (v1),15182496,3/16/22 15:00
External Authority Valuesets,[HCUP] Complications of acute myocardial infarction (v1),749854788,3/16/22 15:00
External Authority Valuesets,[HCUP] Depressive disorders (v1),537591858,3/16/22 15:00
External Authority Valuesets,[HCUP] Diabetes mellitus with complication (v1),915573730,3/16/22 15:00
External Authority Valuesets,[HCUP] Diabetes mellitus without complication (v1),617427516,3/16/22 15:00
External Authority Valuesets,[HCUP] Diseases of white blood cells (v1),395987449,3/16/22 15:00
External Authority Valuesets,"[HCUP] Disruptive, impulse-control and conduct disorders (v1)",953626701,3/16/22 15:00
External Authority Valuesets,[HCUP] Gastroduodenal ulcer (v2),749303275,3/16/22 15:00
External Authority Valuesets,[HCUP] Hepatic failure (v1),79164956,3/16/22 15:00
External Authority Valuesets,[HCUP] Immunity disorders (v1),746266240,3/16/22 15:00
External Authority Valuesets,[HCUP] Nephritis; nephrosis; renal sclerosis (v1),986085724,3/16/22 15:00
External Authority Valuesets,[HCUP] Other specified and unspecified diseases of kidney and ureters (v1),739249986,3/16/22 15:00
External Authority Valuesets,[HCUP] Other specified and unspecified liver disease (v1),102554893,3/16/22 15:00
External Authority Valuesets,[HCUP] Other specified and unspecified lower respiratory disease (v1),841969278,3/16/22 15:00
External Authority Valuesets,[HCUP] Other specified and unspecified mood disorders (v1),8133362,3/16/22 15:00
External Authority Valuesets,[HCUP] Peripheral and visceral vascular disease (v2),590229661,5/16/22 15:00
External Authority Valuesets,[HCUP] Proteinuria (v3),83982865,3/16/22 15:00
External Authority Valuesets,[HCUP] Regional enteritis and ulcerative colitis (v1),168914400,3/16/22 15:00
External Authority Valuesets,[HCUP] Schizophrenia spectrum and other psychotic disorders (v1),894827268,3/16/22 15:00
External Authority Valuesets,[HCUP] Tobacco-related disorders (v1),144374255,3/16/22 15:00
External Authority Valuesets,[VSAC] Active Bleeding (v1),218099832,3/16/22 15:00
External Authority Valuesets,[VSAC] Acute Heart Failure (v1),409804608,3/16/22 15:00
External Authority Valuesets,[VSAC] Acute Myocardial Infarction (AMI) (v1),369143650,3/16/22 15:00
External Authority Valuesets,[VSAC] Acute Renal Failure (v1),191950997,3/16/22 15:00
External Authority Valuesets,[VSAC] Acute Respiratory Distress Syndrome (v1),168521845,3/16/22 15:00
External Authority Valuesets,[VSAC] Air and Thrombotic Embolism (v1),214302145,3/16/22 15:00
External Authority Valuesets,[VSAC] Anemia (v2),575004035,3/16/22 15:00
External Authority Valuesets,[VSAC] Antithrombotic Therapy_Antiplatelet (v1),307887059,3/16/22 15:00
External Authority Valuesets,[VSAC] Arrhythmia (v1),187292967,3/16/22 15:00
External Authority Valuesets,[VSAC] Asthma (v1),602584947,3/16/22 15:00
External Authority Valuesets,[VSAC] Atrial Fibrillation/Flutter (v2),624950910,3/16/22 15:00
External Authority Valuesets,[VSAC] Autoimmune Disease (v3),666627336,3/28/22 17:00
External Authority Valuesets,[VSAC] Bariatric Surgery (v2),706169495,3/16/22 15:00
External Authority Valuesets,"[VSAC] Bells Palsy, codes for AE reporting (v2)",41656421,4/20/22 20:00
External Authority Valuesets,[VSAC] Bleeding Disorder (v2),762981309,3/16/22 15:00
External Authority Valuesets,[VSAC] Blood Transfusion (v2),800491580,3/16/22 15:00
External Authority Valuesets,[VSAC] Bradycardia (v1),740983957,3/16/22 15:00
External Authority Valuesets,[VSAC] Brain Injury (v2),102561251,3/16/22 15:00
External Authority Valuesets,[VSAC] Brain Tumor (v3),758030341,3/16/22 15:00
External Authority Valuesets,[VSAC] Bronchiectasis (v1),555855275,3/16/22 15:00
External Authority Valuesets,[VSAC] Cancer (v9),122145229,4/21/22 11:00
External Authority Valuesets,[VSAC] Cardiac Disease (v2),402952445,3/16/22 15:00
External Authority Valuesets,[VSAC] Cardiac Surgery (v1),416233275,3/16/22 15:00
External Authority Valuesets,[VSAC] Cardiomyopathy (v4),886099282,4/20/22 17:00
External Authority Valuesets,[VSAC] Cerebrovascular Diseases (v3),365765823,4/21/22 17:00
External Authority Valuesets,[VSAC] Cerebrovascular Disease Stroke or TIA (v1),738973826,4/27/22 17:00
External Authority Valuesets,[VSAC] Chronic Kidney Disease All Stages (1 through 5) (v1),997506752,3/16/22 15:00
External Authority Valuesets,[VSAC] Chronic Liver Disease (v4),431382310,3/16/22 15:00
External Authority Valuesets,[VSAC] Chronic Obstructive Pulmonary Disease (v4),864746330,3/16/22 15:00
External Authority Valuesets,[VSAC] Chronic Stable Angina (v1),521595143,3/16/22 15:00
External Authority Valuesets,[VSAC] Comorbid Conditions for Respiratory Conditions (v7),689333457,3/16/22 15:00
External Authority Valuesets,[VSAC] Congenital Anomalies S (v1),773126867,3/16/22 15:00
External Authority Valuesets,[VSAC] Congenital heart disease (v2),892516138,4/21/22 15:00
External Authority Valuesets,[VSAC] Congenital Malformations (v3),568565870,3/16/22 15:00
External Authority Valuesets,[VSAC] Coronary Artery Disease No MI (v2),866452655,3/16/22 15:00
External Authority Valuesets,"[VSAC] Corticosteroids, Systemic (v2)",983041019,3/16/22 15:00
External Authority Valuesets,[VSAC] COVID19 ICD10CM Value Set for Down's Syndrome (v2),527270407,4/21/22 17:00
External Authority Valuesets,[VSAC] Cystic Fibrosis (v3),146508152,3/16/22 15:00
External Authority Valuesets,[VSAC] Dementia & Mental Degenerations (v2),100358347,3/16/22 15:00
External Authority Valuesets,[VSAC] Diabetes (v1),588957858,3/16/22 15:00
External Authority Valuesets,[VSAC] Diastolic Heart Failure (v1),591383270,3/16/22 15:00
External Authority Valuesets,[VSAC] Disseminated Intravascular Coagulation (v2),566478455,3/16/22 15:00
External Authority Valuesets,[VSAC] Eclampsia (v3),13453334,3/16/22 15:00
External Authority Valuesets,[VSAC] Elder Abuse Diagnoses (v3),256103397,4/22/22 12:00
External Authority Valuesets,[VSAC] Elder Abuse Goals (v2),243655815,4/22/22 12:00
External Authority Valuesets,[VSAC] Elder Abuse Interventions (v2),450055230,4/22/22 12:00
External Authority Valuesets,[VSAC] Elder Abuse Procedures (v2),988090927,4/22/22 12:00
External Authority Valuesets,[VSAC] Encephalitis (Disorders) (v2),82523982,4/20/22 20:00
External Authority Valuesets,[VSAC] epilepsy (active) (v2),948184761,3/16/22 15:00
External Authority Valuesets,[VSAC] Financial Insecurity Goals (v3),751745849,4/22/22 12:00
External Authority Valuesets,[VSAC] Financial Insecurity Procedures (v2),378170869,4/22/22 12:00
External Authority Valuesets,[VSAC] Food Insecurity Diagnoses (v3),870007552,4/22/22 12:00
External Authority Valuesets,[VSAC] Food Insecurity Interventions (v2),601354999,4/22/22 20:00
External Authority Valuesets,[VSAC] Food Insecurity Procedures (v2),288235224,4/22/22 20:00
External Authority Valuesets,[VSAC] Guillain Barre Syndrome (v1),728213334,3/16/22 15:00
External Authority Valuesets,[VSAC] Headache (v3),730992420,3/16/22 15:00
External Authority Valuesets,[VSAC] Heart Failure (v2),142651859,3/16/22 15:00
External Authority Valuesets,[VSAC] Hematopoietic stem cell transplant (v3),704911067,3/16/22 15:00
External Authority Valuesets,[VSAC] Hemiplegia Symptoms (v3),798820225,4/27/22 17:00
External Authority Valuesets,[VSAC] HIV 1 (v1),381333442,3/16/22 15:00
External Authority Valuesets,[VSAC] Homelessness Diagnoses (v2),597460996,4/22/22 12:00
External Authority Valuesets,[VSAC] Homelessness Procedures (v2),354899212,4/22/22 12:00
External Authority Valuesets,[VSAC] Housing Instability Diagnoses (v2),829037839,4/23/22 12:00
External Authority Valuesets,[VSAC] Housing Instability Procedures (v2),346277904,4/22/22 15:00
External Authority Valuesets,[VSAC] Hypertension (v3),150036573,5/18/22 15:00
External Authority Valuesets,[VSAC] Hypotension (v1),908202216,3/16/22 15:00
External Authority Valuesets,[VSAC] Hypothyroidism (v1),487697357,3/16/22 15:00
External Authority Valuesets,[VSAC] Immunodeficient Conditions (v1),777421664,3/16/22 15:00
External Authority Valuesets,[VSAC] Immunosuppressant medications for inflammatory bowel disease or rheumatoid arthritis (v2),167015188,3/16/22 15:00
External Authority Valuesets,[VSAC] Inadequate Housing Diagnoses (v2),597983430,4/22/22 12:00
External Authority Valuesets,[VSAC] Inadequate Housing Goals (v2),396155663,4/22/22 15:00
External Authority Valuesets,[VSAC] Inadequate Housing Interventions (v2),161425022,4/22/22 12:00
External Authority Valuesets,[VSAC] Inadequate Housing Procedures (v2),280734396,4/22/22 15:00
External Authority Valuesets,[VSAC] Inhaled Corticosteroids (v2),886083312,3/16/22 15:00
External Authority Valuesets,[VSAC] Interstitial lung disease (v1),158058925,3/16/22 15:00
External Authority Valuesets,[VSAC] Intimate Partner Violence Diagnoses (v2),270414638,4/25/22 12:00
External Authority Valuesets,[VSAC] Intimate Partner Violence Goals (v2),51267658,4/25/22 15:00
External Authority Valuesets,[VSAC] Intimate Partner Violence Interventions (v2),989216425,4/25/22 12:00
External Authority Valuesets,[VSAC] Intimate Partner Violence Procedures (v2),436464307,4/25/22 15:00
External Authority Valuesets,[VSAC] Ischemic Stroke_Embolism (v1),192685592,3/16/22 15:00
External Authority Valuesets,[VSAC] Ischemic Stroke_Other (v2),293213778,4/27/22 15:00
External Authority Valuesets,[VSAC] Ischemic Stroke_Thrombosis (v2),590182092,4/27/22 15:00
External Authority Valuesets,[VSAC] Kidney Transplant (v2),751413170,3/16/22 15:00
External Authority Valuesets,[VSAC] Less than high school education Goals (v2),271056968,4/25/22 12:00
External Authority Valuesets,[VSAC] Less than high school education Procedures (v2),159470853,4/25/22 12:00
External Authority Valuesets,[VSAC] Major Transplant (v3),276258156,3/16/22 15:00
External Authority Valuesets,[VSAC] Material Hardship Diagnoses (v2),377040058,4/25/22 16:00
External Authority Valuesets,[VSAC] Material Hardship Goals (v2),457082396,4/25/22 15:00
External Authority Valuesets,[VSAC] Material Hardship Procedures (v2),319293562,4/25/22 15:00
External Authority Valuesets,[VSAC] Meningitis (v2),197886613,4/20/22 20:00
External Authority Valuesets,[VSAC] Mental Behavioral and Neurodevelopmental Disorders (v3),584855798,3/16/22 15:00
External Authority Valuesets,[VSAC] Mental Health Disorder (v3),603465835,3/16/22 15:00
External Authority Valuesets,[VSAC] Metastatic Cancer (v1),660401755,3/28/22 18:00
External Authority Valuesets,[VSAC] Multiple Sclerosis (v2),775793929,4/21/22 15:00
External Authority Valuesets,[VSAC] Myocardial Infarction (v1),673422393,3/16/22 15:00
External Authority Valuesets,[VSAC] Neuromuscular Disease (v2),881239411,3/16/22 15:00
External Authority Valuesets,[VSAC] Obesity (v2),466714408,3/28/22 10:00
External Authority Valuesets,[VSAC] Overweight or Obese (v3),616018686,4/20/22 20:00
External Authority Valuesets,[VSAC] Parkinson's Disease (v2),251805604,4/20/22 20:00
External Authority Valuesets,[VSAC] Pregnancy (v2),446056982,3/16/22 15:00
External Authority Valuesets,[VSAC] Pulmonary Embolism or DVT (v3),631335820,3/16/22 15:00
External Authority Valuesets,[VSAC] Pulmonary Hypertension (v3),86242485,3/16/22 15:00
External Authority Valuesets,[VSAC] Rheumatoid Arthritis (v1),26562308,3/28/22 17:00
External Authority Valuesets,[VSAC] Sepsis (v2),782040890,3/16/22 15:00
External Authority Valuesets,[VSAC] Sickle Cell Disease (v3),765959200,3/16/22 15:00
External Authority Valuesets,[VSAC] Smoking or Cigarette Use (v2),727648761,3/16/22 15:00
External Authority Valuesets,[VSAC] Social Connection Diagnoses (v2),897240313,4/25/22 15:00
External Authority Valuesets,[VSAC] Social Connection Goals (v2),855674119,4/25/22 15:00
External Authority Valuesets,[VSAC] Social Connection Interventions (v2),777183738,4/25/22 15:00
External Authority Valuesets,[VSAC] Social Connection Procedures (v2),250470842,4/25/22 15:00
External Authority Valuesets,[VSAC] Social Determinants of Health General Interventions (V2),330538913,4/21/22 16:00
External Authority Valuesets,[VSAC] Social Determinants of Health Goals (v2),201986476,4/27/22 15:00
External Authority Valuesets,[VSAC] Social Determinants of Health Procedures (v2),687687213,4/27/22 15:00
External Authority Valuesets,[VSAC] Stress Diagnoses (v2),215396858,4/25/22 15:00
External Authority Valuesets,[VSAC] Stress Goals (v2),16814325,4/25/22 12:00
External Authority Valuesets,[VSAC] Stress Interventions (v2),967156356,4/25/22 12:00
External Authority Valuesets,[VSAC] Stress Procedures (v2),350566031,4/25/22 12:00
External Authority Valuesets,[VSAC] Substance Use Disorder (v3),509084611,3/16/22 15:00
External Authority Valuesets,[VSAC] Thalassemia (v2),35406191,3/16/22 15:00
External Authority Valuesets,[VSAC] Transportation Insecurity Procedures (v2),545210402,4/27/22 16:00
External Authority Valuesets,[VSAC] Tuberculosis (Disorders) (v1),942934994,3/16/22 15:00
External Authority Valuesets,[VSAC] Type 1 Diabetes (v4),673524081,3/16/22 15:00
External Authority Valuesets,[VSAC] Type II Diabetes (v2),484742674,3/16/22 15:00
External Authority Valuesets,[VSAC] Unemployment Diagnoses (v2),910742250,4/27/22 12:00
External Authority Valuesets,[VSAC] Unemployment Goals (v2),266069787,4/27/22 15:00
External Authority Valuesets,[VSAC] Unemployment Interventions (v2),83471041,4/27/22 15:00
External Authority Valuesets,[VSAC] Unemployment Procedures (v2),771404868,4/27/22 16:00
External Authority Valuesets,[VSAC] Veteran Status Diagnoses (v2),991760905,4/27/22 16:00
External Authority Valuesets,[VSAC] Veteran Status Procedures (v2),385390926,4/27/22 16:00
External Authority Valuesets,[VSAC] Alzheimer's Disease (ICD10CM) (v2),287931644,4/21/22 17:00
External Authority Valuesets,[VSAC] Amyotrophic Lateral Sclerosis (ALS) and Related Motor Neuron Disease (ICD10CM) (v2),33647039,4/21/22 17:00
Harmonized Units,"Neutrophils (absolute), x10E3/uL",881397271,3/30/22 17:00
Harmonized Units,"Neutrophils (relative), %",304813348,3/30/22 17:00
Harmonized Units,"NT pro BNP, pg/mL",561166072,3/30/22 17:00
Harmonized Units,pH,845428728,3/30/22 17:00
Harmonized Units,"Platelet count, x10E3/uL",167697906,3/30/22 17:00
Harmonized Units,"Potassium, mmol/L",622316047,3/30/22 17:00
Harmonized Units,"Procalcitonin, ng/mL",610397248,3/30/22 17:00
Harmonized Units,Respiratory rate,286601963,3/30/22 17:00
Harmonized Units,"Sodium, mmol/L",887473517,3/30/22 17:00
Harmonized Units,SpO2,780678652,3/30/22 17:00
Harmonized Units,Systolic blood pressure,186465804,3/30/22 17:00
Harmonized Units,Temperature,656562966,3/30/22 17:00
Harmonized Units,"Troponin all types, ng/mL",623367088,3/30/22 17:00
Harmonized Units,"White blood cell count, x10E3/uL",138719030,3/30/22 17:00
Harmonized Units,Albumin (g/dL),104464584,3/30/22 17:00
Harmonized Units,"ALT (SGPT), IU/L",538737057,3/30/22 17:00
Harmonized Units,"AST (SGOT), IU/L",248480621,3/30/22 17:00
Harmonized Units,"Bilirubin (total), mg/dL",586434833,3/30/22 17:00
Harmonized Units,"Bilirubin (Conjugated/Direct), mg/dL",50252641,3/30/22 17:00
Harmonized Units,"Bilirubin - (Unconjugated/Indirect), mg/dL",59108731,3/30/22 17:00
Harmonized Units,BMI,65622096,3/30/22 17:00
Harmonized Units,"BNP, pg/mL",703853936,3/30/22 17:00
Harmonized Units,Body weight,776390058,3/30/22 17:00
Harmonized Units,"BUN, mg/dL",139231433,3/30/22 17:00
Harmonized Units,BUN/Creatinine ratio,165765962,3/30/22 17:00
Harmonized Units,"c-reactive protein CRP, mg/L",371622342,3/30/22 17:00
Harmonized Units,CD4 Cell Count (absolute),24299640,3/30/22 17:00
Harmonized Units,CD4 Cell Count (relative),254892302,3/30/22 17:00
Harmonized Units,CD8 Cell Count (absolute),301181605,3/30/22 17:00
Harmonized Units,CD8 Cell Count (relative),225952304,3/30/22 17:00
Harmonized Units,CD4/CD8 Ratio,410508887,3/30/22 17:00
Harmonized Units,"Chloride, mmol/L",733538531,3/30/22 17:00
Harmonized Units,"Creatinine, mg/dL",615348047,3/30/22 17:00
Harmonized Units,"D-Dimer, mg/L FEU",475972797,3/30/22 17:00
Harmonized Units,Diastolic blood pressure,573275931,3/30/22 17:00
Harmonized Units,"Erythrocyte Sed. Rate, mm/hr",905545362,3/30/22 17:00
Harmonized Units,"Ferritin, ng/mL",317388455,3/30/22 17:00
Harmonized Units,"Fibrinogen, pg/mL",27035875,3/30/22 17:00
Harmonized Units,GCS Eye,481397778,3/30/22 17:00
Harmonized Units,GCS Motor,706664851,3/30/22 17:00
Harmonized Units,GCS Verbal,660016502,3/30/22 17:00
Harmonized Units,"Glucose, mg/dL",59698832,3/30/22 17:00
Harmonized Units,Heart rate,596956209,3/30/22 17:00
Harmonized Units,Height,754731201,3/30/22 17:00
Harmonized Units,"Hemoglobin A1c, %",381434987,3/30/22 17:00
Harmonized Units,"Hemoglobin, g/dL",28177118,3/30/22 17:00
Harmonized Units,HIV Viral Load,776696816,3/30/22 17:00
Harmonized Units,"IL-6, pg/mL",928163892,3/30/22 17:00
Harmonized Units,"Lactate, U/L",400660753,3/30/22 17:00
Harmonized Units,"Lactate Dehydrogenase (LDH), units/L",10450810,3/30/22 17:00
Harmonized Units,"Lymphocytes (absolute), x10E3/uL",627523060,3/30/22 17:00
Harmonized Units,"Lymphocytes (relative), %",45416701,3/30/22 17:00
Harmonized Units,Mean arterial pressure,709075479,3/30/22 17:00
"""

    df = pd.read_csv(StringIO(csv_string))
    df = spark.createDataFrame(df).orderBy('tag_name', 'concept_set_name')

    return df