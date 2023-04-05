import json

from utils.execution_engine import ExecutionEngine
from input_schema import schema_dict

ee = ExecutionEngine(transforms_folder='./src', input_specs=json.load(open('./input_specs.json')), input_schema=schema_dict)
ee.execute_transform('train_test_model')