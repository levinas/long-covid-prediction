import json

from utils.execution_engine import ExecutionEngine
from input_schema import schema_dict

ee = ExecutionEngine(transforms_folder='./src', input_specs=json.load(open('./input_specs.json')), input_schema=schema_dict)
ee.execute_transform('Convalesco_predictions')
ee.export_to_csv('Convalesco_predictions', 'Convalesco_predictions.csv', index=False)
