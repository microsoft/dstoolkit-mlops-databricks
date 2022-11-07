# Databricks notebook source
"""Example of framework usage"""
# If setting up scope with Keyvault --> Ensure you have RBAC to KV Admin From 'AzureDatabrics' to Key Vault. 
# Test....Demo Merge Delete- 07/10/2022 @ 22:49
import random
from acai_ml.core import Engine
import pandas as pd
from pydataset import data
from pathlib import Path
from dbkcore.core import trace
from dbkcore.core import Log
from dbkdev.data_steps import DataStep, DataStepDataframe
from dbkdev.data_steps import apply_test
from sklearn.model_selection import ParameterSampler
from sklearn.utils.fixes import loguniform
from pyspark.sql import functions as F
import numpy as np
from sklearn.model_selection import cross_val_score
from sklearn import svm


class Step_loadData(DataStep):
    """Load the defined dataset."""

    def test(self):
        """Apply data tests."""
        self.test_is_dataframe_empty(df=self.output_data.dataframe)
        self.test_null_values(
            cols=['Sepal.Length', 'Sepal.Width'],
            df=self.output_data.dataframe
        )

    @apply_test
    @trace
    def initialize(self, name_dataset: str):
        """
        Initialize the DataStep.

        Parameters
        ----------
        name_dataset : str
            Name of the dataset to load from pydataset package
        """
        p_df = data(name_dataset)
        p_df.columns = [c.replace('.', '') for c in p_df.columns]
        dt = self.spark.createDataFrame(p_df)
        self.set_output_data(dt)


class Step_crossValidate(DataStep):
    """Run multiple models in parallel."""

    def test(self):
        pass

    @trace(attrs_refact=['appi_ik'])
    def initialize(
        self,
        dt: DataStepDataframe,
        pipeline_name: str,
        appi_ik: str,
        n_iter: int
    ):
        param_grid = {
            'C': loguniform(1e0, 1e3),
            'kernel': ['linear', 'rbf'],
            'class_weight': ['balanced', None]
        }
        rng = np.random.RandomState(0)
        param_list = list(
            ParameterSampler(
                param_grid,
                n_iter=n_iter,
                random_state=rng
            )
        )
        # p_dt = Engine.get_instance().spark().createDataFrame(pd.DataFrame(param_list)).\
        #     withColumn('id', F.monotonically_increasing_id())
        p_dt = self.spark.createDataFrame(pd.DataFrame(param_list)).\
            withColumn('id', F.monotonically_increasing_id())
        dt_train = dt.dataframe.crossJoin(
            p_dt
        )

        udf_schema = dt_train.select(
            'id',
            F.lit(0.0).alias('score')
        ).schema

        def pudf_train(dt_model):
            param_id = dt_model['id'].unique()[0]
            param_c = dt_model['C'].unique()[0]
            param_class_weight = dt_model['class_weight'].unique()[0]
            param_kernel = dt_model['kernel'].unique()[0]

            logging_custom_dimensions = {
                'id': str(param_id),
                'C': str(param_c),
                'class_weight': param_class_weight,
                'kernel': param_kernel
            }

            Log(pipeline_name, appi_ik)

            try:

                # Raising randomly exception
                if random.randint(0, 20) > 15:
                    raise 'Random exception'

                dt_x = dt_model[
                    [
                        'SepalLength',
                        'SepalWidth',
                        'PetalLength',
                        'PetalWidth'
                    ]
                ]
                y = dt_model['Species']
                clf = svm.SVC(
                    kernel=param_kernel,
                    C=param_c,
                    class_weight=param_class_weight,
                    random_state=42
                )
                scores = cross_val_score(clf, dt_x, y, cv=5, scoring='f1_macro')
                score = scores.mean()
                dt_out = pd.DataFrame(
                    {
                        'id': [param_id],
                        'score': [score]
                    }
                )
                Log.get_instance().log_info("Training:success", custom_dimension=logging_custom_dimensions)
            except Exception:
                Log.get_instance().log_error("Training:failed", custom_dimension=logging_custom_dimensions)
                dt_out = pd.DataFrame(
                    {
                        'id': [param_id],
                        'score': [-1]
                    }
                )
            return dt_out

        '''
        dt_model = dt_train.where(F.col('id') == 17179869184).toPandas()
        '''
        dt_cross_evals = dt_train.\
            groupBy(['id']).\
            applyInPandas(pudf_train, schema=udf_schema).\
            cache()
        dt_cross_evals.count()
        self.set_output_data(dt_cross_evals)


Engine()
Engine().get_instance().initialize_env()
# pipeline_name = Path(__file__).stem
pipeline_name = "Remote Testing"

Engine().get_instance().initialize_logger(pipeline_name=pipeline_name, appi_ik_scope="AzureResourceSecrets", appi_ik_secret="appi_ik")
# Engine().get_instance().spark().conf.set("spark.sql.execution.arrow.enabled", "true")

run_id = 'test_run_id'

step_loadData = Step_loadData(
    spark=Engine.get_instance().spark(),
    run_id=run_id
)

step_loadData.initialize(
    name_dataset='iris'
)

step_crossValidate = Step_crossValidate(
    spark=Engine.get_instance().spark(),
    run_id=run_id
)

step_crossValidate.initialize(
    dt=step_loadData.output_data,
    pipeline_name=pipeline_name,
    appi_ik=Engine().get_instance().appi_ik,
    n_iter=1000
)

step_crossValidate.output_data.dataframe.toPandas()

print(step_crossValidate.output_data.dataframe.toPandas())
