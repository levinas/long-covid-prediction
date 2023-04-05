import os
import inspect
import pickle
from typing import Any
from importlib.machinery import SourceFileLoader

import pandas as pd
import pyspark as ps
from pyspark.sql import SparkSession
import networkx as nx
from pyspark.sql.types import *
from time import time
import atexit

import functools
import types
import copy
import json
import shutil


def copy_func(f, globals=None, module=None):
    "https://stackoverflow.com/questions/49076566/override-globals-in-function-imported-from-another-module"
    if globals is None:
        globals = f.__globals__
    g = types.FunctionType(f.__code__, globals, name=f.__name__,
                           argdefs=f.__defaults__, closure=f.__closure__)
    g = functools.update_wrapper(g, f)
    if module is not None:
        g.__module__ = module
    g.__kwdefaults__ = copy.copy(f.__kwdefaults__)
    return g


def dfs_directed(G, source, early_terminations=[]):
    subG = nx.DiGraph()
    stack = [source]
    while stack:
        v = stack.pop()
        if v not in early_terminations:
            for p_v in G.predecessors(v):
                stack.append(p_v)
                subG.add_edge(p_v, v)
    return subG


def process_annotations(transform_fn):
    """ If no annotations are provided, the argument should be a spark dataframe.

    Args:
        transform_fn (function_object): the transform function

    Returns:
        dict: A dictionary of the arguments and their types

    """
    annotations = transform_fn.__annotations__
    arguments = inspect.getfullargspec(transform_fn).args
    args_dict = {}
    for arg in arguments:
        if arg in annotations:
            args_dict[arg] = annotations[arg]
        else:
            args_dict[arg] = ps.sql.DataFrame
    return args_dict


def fix_csv_and_load(spark, csv_file, pyspark_schema):
    # remove double digits in csv files
    os.makedirs('tmp', exist_ok=True)
    filename = os.path.splitext(os.path.basename(csv_file))[0] + str(int(time()))
    fixed_csv_file = os.path.join('tmp', f'{filename}_fix.csv')
    if os.path.exists(fixed_csv_file):
        os.remove(fixed_csv_file)
    with open(csv_file, 'r') as f:
        with open(fixed_csv_file, 'w') as f_out:
            for line in f:
                f_out.write(line.replace('.0,', ',').replace('.0\n', '\n'))

    df = pd.read_csv(csv_file, nrows=1)    
    csv_available_columns = df.columns
    schema_columns = [s.name for s in pyspark_schema]
    schema_column_to_field = {s.name: s for s in pyspark_schema}
    reduced_schema_list = []
    for c in csv_available_columns:
        if c in schema_columns:
            reduced_schema_list.append(schema_column_to_field[c])
        else:
            reduced_schema_list.append(StructField(c, StringType(), True))
    reduced_schema = StructType(reduced_schema_list)
    result = spark.read.csv(fixed_csv_file, schema=reduced_schema, header=True)
    return result


class ExecutionEngine:
    def __init__(self,
                 transforms_folder,
                 input_specs,
                 input_schema,
                 output_folder='output',
                 load_from_output=True):
        """
        Args:
            transforms_folder (str): Path to the folder containing the transforms and global_code.py
            input_specs (dict): A dictionary of the input_nodes and their corresponding files
            input_schema (dict): A dictionary of the input_nodes and their corresponding schemas (StructType)
            output_folder (str, optional): Path to the folder where the output files should be saved. Defaults to 'output'.
            load_from_output (bool, optional): If True, intermediate transforms will be loaded from the output folder. Defaults to True.
            
        """
        atexit.register(self.cleanup)
        self.transforms_folder = transforms_folder
        self.input_specs = input_specs
        self.input_schema = input_schema
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)
        self.load_from_output = load_from_output
        self.transforms_files = [
            f for f in os.listdir(transforms_folder)
            if f.endswith('.py') and not f.startswith('_')
        ]
        self.transforms = {}
        self.workbook_graph = nx.DiGraph()
        self.spark = SparkSession.builder \
                    .master("local[*]") \
                    .config("spark.executor.memory", "64g") \
                    .config("spark.driver.memory", "64g") \
                    .appName("test_app") \
                    .getOrCreate()
        
        self._initialize_transforms_and_graph()
        self._load_input_files()
        self._check_input_ready(self.workbook_graph)


    def _initialize_transforms_and_graph(self):
        transforms_with_code = set()
        arguments = set()
        global_code_namespace = self._load_module_symbols_into_globals(os.path.join(
                    self.transforms_folder, 'global_code.py'), is_transform=False)
        global_code_namespace['spark'] = self.spark
        for f in self.transforms_files:
            if f == 'global_code.py':
                pass
            else:
                module_name, fn = self._load_module_symbols_into_globals(
                    os.path.join(self.transforms_folder, f))
                # add global code namespace to function namespace
                # fn.__globals__ = global_code_namespace | fn.__globals__
                fn_copy = copy_func(fn, global_code_namespace | fn.__globals__)
                annotations = process_annotations(fn)
                self.transforms[module_name] = {
                    'function': fn_copy,
                    'arguments': annotations,
                    'output': None,
                    'output_file': None
                }
                transforms_with_code.add(module_name)
                for arg in annotations.keys():
                    self.workbook_graph.add_edge(arg, module_name)
                    arguments.add(arg)
        for a in arguments:
            if a not in transforms_with_code:
                self.transforms[a] = {
                    'function': None,
                    'arguments': {},
                    'output': None,
                    'output_file': None
                }


    def _load_module_symbols_into_globals(self, full_path, is_transform=True):
        module_name = os.path.splitext(os.path.basename(full_path))[0]
        module = SourceFileLoader(module_name, full_path).load_module()
        if is_transform:
            fn = None
            for k, v in module.__dict__.items():
                if k == module_name:
                    fn = v
            if fn is None:
                raise ValueError(
                    f'Malformed transform: no function called {module_name} in {full_path}'
                )
            return module_name, fn
        else:
            return module.__dict__


    def _load_input_files(self):
        for k, v in self.input_specs.items():
            if k not in self.transforms:
                raise ValueError(
                    f'Input {k} does not correspond to a transform')
            if v.endswith('.csv'):
                print(f'Loading {v} with schema {k}...')
                self.transforms[k]['output'] = fix_csv_and_load(self.spark, v, self.input_schema[k])
                self.transforms[k]['output_file'] = v
            elif v.endswith('.pkl'):
                with open(v, 'rb') as f:
                    self.transforms[k]['output'] = pickle.load(f)
                    self.transforms[k]['output_file'] = v
            else:
                raise ValueError(f'Cannot load file {v}')
        if self.load_from_output:
            output_files = os.listdir(self.output_folder)
            available_transform_dict = {}
            for f in output_files:
                file_name, ext = os.path.splitext(f)
                if ext == '.json':
                    available_transform_dict[file_name] = 'pyspark'
                elif ext == '.csv':
                    available_transform_dict[file_name] = 'pandas'
                elif ext == '.pkl':
                    available_transform_dict[file_name] = 'pickle'
            for transform_name, transform_type in available_transform_dict.items():
                if transform_name in self.transforms.keys():
                    if transform_type == 'pandas':
                        file_full_path = os.path.join(self.output_folder, f'{transform_name}.csv')
                        self.transforms[transform_name]['output'] = pd.read_csv(file_full_path)
                        self.transforms[transform_name]['output_file'] = file_full_path
                        print(f'Loaded pandas dataframe for {transform_name} from {file_full_path}')
                    elif transform_type == 'pyspark':
                        schema_json_full_path = os.path.join(self.output_folder, f'{transform_name}.json')
                        csv_folder_full_path = os.path.join(self.output_folder, f'{transform_name}')
                        schema = StructType.fromJson(json.load(open(schema_json_full_path)))
                        self.transforms[transform_name]['output'] = self.spark.read.csv(csv_folder_full_path, schema=schema)
                        self.transforms[transform_name]['output_file'] = csv_folder_full_path
                        print(f'Loaded pyspark dataframe for {transform_name} from {csv_folder_full_path}')
                    elif transform_type == 'pickle':
                        file_full_path = os.path.join(self.output_folder, f'{transform_name}.pkl')
                        with open(file_full_path, 'rb') as f:
                            self.transforms[transform_name]['output'] = pickle.load(f)
                        self.transforms[transform_name]['output_file'] = file_full_path
                        print(f'Loaded pickled object for {transform_name} from {file_full_path}')
                    else:
                        raise ValueError(f'Cannot load file {v}')


    def _check_input_ready(self, G):
        input_nodes = [x for x in G.nodes() if G.in_degree(x) == 0]
        for node in input_nodes:
            if self.transforms[node]['output'] is None:
                if self.transforms[node]['function']:
                    self.transforms[node]['output'] = self.transforms[node][
                        'function']()
                else:
                    raise ValueError(
                        f'Input {node} is not ready and does not have a method to populate. Please provide a file or load from output.'
                    )
                

    def save_node(self, t):
        """
        Save the output of a node to the output folder.
        Args:
            t (str): The name of the node to save.
        
        """
        if self.transforms[t]['output'] is None:
            raise ValueError(f'ERROR: Cannot save, output for {t} is None.')
        output_type = type(self.transforms[t]['output'])
        if output_type == pd.DataFrame:
            output_file = os.path.join(self.output_folder, f'{t}.csv')
            if os.path.exists(output_file):
                return
            self.transforms[t]['output'].to_csv(output_file)
        elif output_type == ps.sql.DataFrame:
            output_file = os.path.join(self.output_folder, f'{t}')
            if os.path.exists(output_file):
                return
            self.transforms[t]['output'].write.csv(output_file)
            with open(output_file + '.json', 'w') as f:
                f.write(self.transforms[t]['output'].schema.json())
        else:
            output_file = os.path.join(self.output_folder, f'{t}.pkl')
            if os.path.exists(output_file):
                return
            with open(output_file, 'wb') as f:
                pickle.dump(self.transforms[t]['output'], f)

        self.transforms[t]['output_file'] = output_file

    
    def export_to_csv(self, t, csv_save_location, index=True):
        """
        Export the output of a node to a csv file. Produces a single-file csv with headers.
        Args:
            t (str): The name of the node to export.
            csv_save_location (str): The location to save the csv file.
        """
        if self.transforms[t]['output'] is None:
            raise ValueError(f'ERROR: Cannot export, output for {t} is None.')
        output_type = type(self.transforms[t]['output'])
        if output_type == pd.DataFrame:
            self.transforms[t]['output'].to_csv(csv_save_location, index=index)
        elif output_type == ps.sql.DataFrame:
            self.transforms[t]['output'].toPandas().to_csv(csv_save_location, index=index)
        else:
            raise ValueError(f'ERROR: Cannot export, output for {t} is not a dataframe.')


    def _execute_graph(self, G, save_to_file=True, dry_run=False):
        exec_order = nx.topological_sort(G)
        if not dry_run:
            self._check_input_ready(G)
        if dry_run:
            print(f'Preview execution order: {list(exec_order)}')
            return
        for t in exec_order:
            print(f'Executing {t}...')
            arg_values = {}
            for arg, required_type in self.transforms[t]['arguments'].items():
                arg_value = self.transforms[arg]['output']
                if arg_value is None:
                    raise ValueError(
                        f'Argument {arg} for transform {t} is not ready')
                current_type = type(arg_value)
                if required_type == Any or required_type == current_type:
                    pass
                elif required_type == ps.sql.DataFrame and current_type == pd.DataFrame:
                    print(f'WARNING: Converting arg:{arg} for {t} from pandas to pyspark')
                    arg_value = self.spark.createDataFrame(arg_value)
                elif required_type == pd.DataFrame and current_type == ps.sql.DataFrame:
                    print(f'WARNING: Converting arg:{arg} for {t} from pyspark to pandas')
                    arg_value = arg_value.toPandas()
                else:
                    raise ValueError(
                        f'Cannot transform {current_type} to {required_type}')
                arg_values[arg] = arg_value
            if self.transforms[t]['output'] is None:
                self.transforms[t]['output'] = self.transforms[t]['function'](
                    **arg_values)
            if save_to_file and (self.transforms[t]['output'] is not None):
                self.save_node(t)


    def execute_all(self, save_to_file=True, dry_run=False):
        """
        Execute all transforms in the workbook.
        Args:
            save_to_file (bool): Whether to save the output of each transform to a file.
            dry_run (bool): Whether to execute the transforms or just print the execution order.
        """
        self._execute_graph(self.workbook_graph, save_to_file, dry_run)


    def execute_transform(self,
                          transform_name,
                          save_to_file=True,
                          dry_run=False):
        """
        Execute a single transform and its dependencies in the workbook.
        Args:
            transform_name (str): The name of the transform to execute.
            save_to_file (bool): Whether to save the output of each transform to a file.
            dry_run (bool): Whether to execute the transforms or just print the execution order.
        """
        early_terminations = [
            k for k, v in self.transforms.items() if v['output'] is not None
        ]
        sub_graph = dfs_directed(self.workbook_graph, transform_name,
                                 early_terminations)
        self._execute_graph(sub_graph, save_to_file, dry_run)


    def cleanup(self):
        """
        Clean up the temporary files created by the workbook and stop the spark session.
        """
        if os.path.exists('tmp'):
            shutil.rmtree('tmp')
        self.spark.stop()
