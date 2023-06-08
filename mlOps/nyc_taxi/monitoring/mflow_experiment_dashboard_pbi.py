# Databricks notebook source


import mlflow
from mlflow.tracking import MlflowClient

mlflow_client = MlflowClient()
experiment = mlflow_client.get_experiment_by_name("/Shared/ciaran_experiment_nyc_taxi")
experiment_id = experiment.experiment_id


df = mlflow.search_runs(
    experiment_ids=experiment_id
)

display(df)

df = df.rename(columns={"metrics.r2": "r2"})
display(df)
df = df[df.end_time.notnull()]
df = df[df.r2.notnull()]

display(df)

df2 = df.drop(df[df['status'] == "FINISHED"].index, inplace = True)

display(df2)
