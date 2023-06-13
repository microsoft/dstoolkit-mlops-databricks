# Databricks notebook source
from featurization import run_feature_store_refresh
run_feature_store_refresh()

# COMMAND ----------

from training import run_training 

num_rounds_arr = [20,40,60,80,100,120,160]

for num_rounds in num_rounds_arr:
    run_training(
        experiment_name = "ciaran_experiment_nyc_taxi",
        model_name = "taxi_example_fare_packaged",
        model_params = {
            "objective": "regression",
            "metric": "rmse",
            "num_leaves": 25,
            "learning_rate": 0.2,
            "bagging_fraction": 0.9,
            "feature_fraction": 0.9,
            "bagging_seed": 42,
            "verbosity": -1,
            "seed": 42,
            "num_rounds": num_rounds
        }
    )
    from registration import run_registration
    run_registration(
        model_name = "taxi_example_fare_packaged"
    )
