#!/bin/bash

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
python3 data_preparation.py
python3 train.py
python3 infer.py
