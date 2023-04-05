import json

from utils.execution_engine import ExecutionEngine
from input_schema import schema_dict

ee = ExecutionEngine(transforms_folder='./src', input_specs=json.load(open('./input_specs.json')), input_schema=schema_dict)
ee.execute_transform('per_person_info')
ee.execute_transform('person_labs')
ee.execute_transform('person_concept_features')
ee.execute_transform('per_person_info_test')
ee.execute_transform('person_labs_test')
ee.execute_transform('person_concept_features_test')