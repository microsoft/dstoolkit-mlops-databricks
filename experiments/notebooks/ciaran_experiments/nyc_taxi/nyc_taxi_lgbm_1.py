# Databricks notebook source

from featurization import run_feature_store_refresh
run_feature_store_refresh()

# COMMAND ----------
from training import run_training 

run_training(
    experiment_name = "ciaran_experiment_nyc_taxi",
    model_name = "taxi_example_fare_packaged",
    model_params = {
        "objective": "regression",
        "metric": "rmse",
        "num_leaves": 32,
        "learning_rate": 0.2,
        "bagging_fraction": 0.9,
        "feature_fraction": 0.9,
        "bagging_seed": 42,
        "verbosity": -1,
        "seed": 42
    }
)
# COMMAND ----------
from registration import run_registration
run_registration(
    model_name = "taxi_example_fare_packaged"
)