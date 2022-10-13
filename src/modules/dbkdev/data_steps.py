from dbkcore.core import BaseObject, trace, Log
from pyspark.sql.dataframe import DataFrame as PyDataFrame
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from enum import Enum
# from abc import ABC, abstractmethod
from abc import abstractmethod
import pandas as pd
from pandas import DataFrame as PdDataFrame
from pandas.api.types import is_numeric_dtype
from typing import Union, List
from typeguard import typechecked
from pathlib import Path
import functools as _functools




class DataDirection(str, Enum):
    IN = "in"
    OUT = "out"


class DataStepDataframe(BaseObject):

    # TODO: Change name to name_or_path

    def __init__(self, name: str, dataframe: Union[PyDataFrame, PdDataFrame], cache=False):
        self.name = name
        self.dataframe = dataframe
        # Note: it has been removed for spark since the dataframe has to be read
        self.__rows = None
        self.columns_count = len(dataframe.columns) if isinstance(dataframe, PyDataFrame) else dataframe.shape[1]
        self.columns_names = dataframe.columns
        self.cache = cache
        self.__columns_negative = None
        self.__columns_null = None

    def to_pandas(self) -> PdDataFrame:
        if self.is_pandas:
            return self.dataframe
        elif self.is_pyspark:
            return self.dataframe.toPandas()

    @property
    def rows(self):
        if not self.__rows:
            # self.__rows = self.dataframe.cache().count() if self.cache else dataframe.count()
            # TODO: improve me
            if self.cache:
                self.__rows = self.dataframe.cache().count() if isinstance(self.dataframe, PyDataFrame) else self.dataframe.shape[0]    
            else:
                self.__rows = self.dataframe.count() if isinstance(self.dataframe, PyDataFrame) else self.dataframe.shape[0]
        return self.__rows

    @trace
    def columns_negative(self) -> List[str]:
        """
        Identifies the columns with negative values

        Returns
        -------
        List[str]
            Column names
        """
        columns = []

        if not self.__columns_negative:
            if isinstance(self.dataframe, PyDataFrame):
                for column in self.columns_names:
                    count = 0
                    try:
                        count = self.dataframe.filter((F.col(column) < 0)).count()
                    except AnalysisException:
                        pass
                    if count > 0:
                        columns.append(column)
            elif isinstance(self.dataframe, pd.DataFrame):
                for column in self.columns_names:
                    if is_numeric_dtype(self.dataframe[column]):
                        dt_filtered = self.dataframe[self.dataframe[column] < 0]
                        count = dt_filtered.shape[0]
                        if count > 0:
                            columns.append(column)
            self.__columns_negative = columns
        return self.__columns_negative

    @trace
    def columns_null(self) -> List[str]:
        """
        Identifies the columns with null values

        Returns
        -------
        List[str]
            Column names
        """
        if not self.__columns_null:
            columns = []
            if isinstance(self.dataframe, PyDataFrame):
                for column in self.columns_names:
                    count = self.dataframe.filter(F.col(column).isNull()).count()
                    if count > 0:
                        columns.append(column)
            elif isinstance(self.dataframe, pd.DataFrame):
                nan_cols = self.dataframe.columns[self.dataframe.isna().any()].tolist()
                columns.extend(nan_cols)
            self.__columns_null = columns
        return self.__columns_null

    @property
    def is_pandas(self) -> bool:
        return isinstance(self.dataframe, PdDataFrame)

    @property
    def is_pyspark(self) -> bool:
        return isinstance(self.dataframe, PyDataFrame)

    def log_in(self):
        self.log(direction=DataDirection.IN)

    def log_out(self):
        self.log(direction=DataDirection.OUT)

    def log(self, direction: DataDirection):
        dt_name = self.name

        # dt_tag_prefix = "DT:{}".format(direction.upper(), dt_name)
        # dt_name_tag = "{}:NAME".format(dt_tag_prefix)

        # dt_rows_tag = "{}:ROWS:COUNT".format(dt_tag_prefix)
        # if isinstance(self.dataframe, PyDataFrame):
        #     dt_rows = self.dataframe.count()
        # elif isinstance(self.dataframe, PdDataFrame):
        #     dt_rows = self.dataframe.shape[0]

        # dt_columns_tag = "{}:COLUMNS:COUNT".format(dt_tag_prefix)
        # if isinstance(self.dataframe, PyDataFrame):
        #     dt_columns = len(self.dataframe.columns)
        # elif isinstance(self.dataframe, PdDataFrame):
        #     dt_columns = self.dataframe.shape[1]

        # dt_columns_names_tag = "{}:COLUMNS:NAMES".format(dt_tag_prefix)
        # dt_columns_names = ', '.join(self.dataframe.columns)

        # Log.get_instance().log_info(f"{dt_name_tag}:{dt_name}", prefix=direction, custom_dimension=dimensions)
        # Log.get_instance().log_info(f"{dt_rows_tag}:{dt_rows}", prefix=direction)
        # Log.get_instance().log_info(f"{dt_columns_tag}:{dt_columns}", prefix=direction)
        # Log.get_instance().log_info(f"{dt_columns_names_tag}:{dt_columns_names}", prefix=direction)
        
        dimensions = {
            'dataset_name': dt_name,
            'rows_count': self.rows,
            'columns_count': self.columns_count,
            'columns_name': self.columns_names,
            'direction': direction
        }
        Log.get_instance().log_info(f"Processed dataset {dt_name} with {direction.upper()} direction", custom_dimension=dimensions)


def apply_test(func):
    """
    Execute test function after the initialize.

    Notes
    -----
    [Example](https://stackoverflow.com/a/15196410)
    """
    @_functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        res = func(self, *args, **kwargs)
        self.tests()
        return res
    return wrapper


def pre_apply_test(func):
    """
    Execute test function before the initialize.

    Notes
    -----
    [Example](https://stackoverflow.com/a/15196410)
    """
    @_functools.wraps(func)
    def wrapper(self, *args, **kwargs):        
        self.tests()
        res = func(self, *args, **kwargs)
        return res
    return wrapper


def log_output(func):
    """
    Decorator for executing test in sequence

    Notes
    -----
    [Example](https://stackoverflow.com/a/15196410)
    """
    @_functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        res = func(self, *args, **kwargs)
        self.tests()
        return res
    return wrapper


# TODO: if works, remove ABC class
# class DataStep(ABC):
@typechecked
class DataStep():
    """
    Creates a datastep to be used in a pipeline

    Parameters
    ----------
    metaclass : [type], optional
        [description], by default abc.ABCMeta

    Raises
    ------
    Exception
        [description]
    """

    @trace
    def __init__(
        self,
        spark: SparkSession,
        run_id: str
    ):
        self.spark = spark
        self.run_id = run_id

    @property
    def display_name(self) -> str:
        res = type(self).__name__
        return res

    @trace
    def spark_read_table(self, name: str) -> DataStepDataframe:
        dt = self.spark.read.table(name)
        datastep_dataframe = DataStepDataframe(name=name, dataframe=dt)
        datastep_dataframe.log(DataDirection.IN)
        return datastep_dataframe

    @trace
    def spark_read_temp_table(self, name: str) -> DataStepDataframe:
        global_temp_db = self.spark.conf.get(f"spark.sql.globalTempDatabase")
        dt = self.spark.read.table(f'{global_temp_db}.{name}')
        datastep_dataframe = DataStepDataframe(name=name, dataframe=dt)
        datastep_dataframe.log_in()
        return datastep_dataframe

    @trace
    def spark_read_parquet_path(self, path: Path, cache=False) -> DataStepDataframe:
        path_str = str(path)
        dt = self.spark.read.parquet(path_str)
        datastep_dataframe = DataStepDataframe(name=path_str, dataframe=dt, cache=cache)
        datastep_dataframe.log_in()
        return datastep_dataframe

    @trace
    def pandas_read_csv(self, path: Path) -> DataStepDataframe:
        datastep_dataframe = self.spark_read_csv(path)
        datastep_dataframe.dataframe = datastep_dataframe.dataframe.toPandas()
        datastep_dataframe.log_in()
        return datastep_dataframe

    @trace
    def spark_read_csv(self, path: Path) -> DataStepDataframe:
        path_str = str(path)
        dt = self.spark.read.format("csv").\
            option("inferSchema", "true").\
            option("header", "true").\
            option("delimiter", ",").\
            option("charset", "utf-8").load(path_str)
        datastep_dataframe = DataStepDataframe(name=path_str, dataframe=dt)
        datastep_dataframe.log_in()
        return datastep_dataframe

    @trace
    def test_rows_leq(self, dt_1: DataStepDataframe, dt_2: DataStepDataframe):
        assert dt_1.rows < dt_2.rows,\
            "ROWS CHECK: {dt_1_name} ({dt_1_rows}) < {dt_2_name} ({dt_2_rows})".format(
                dt_1_name=dt_1.name,
                dt_1_rows=dt_1.rows,
                dt_2_name=dt_2.name,
                dt_2_rows=dt_2.rows)
        print("Asserted")

    @trace
    def test_rows_eq(self, dt_1: DataStepDataframe, dt_2: DataStepDataframe):
        assert dt_1.rows == dt_2.rows,\
            "ROWS CHECK: {dt_1_name} ({dt_1_rows}) == {dt_2_name} ({dt_2_rows})".format(
                dt_1_name=dt_1.name,
                dt_1_rows=dt_1.rows,
                dt_2_name=dt_2.name,
                dt_2_rows=dt_2.rows)
        print("Asserted")

    @trace
    def test_rows_geq(self, dt_1: DataStepDataframe, dt_2: DataStepDataframe):
        assert dt_1.rows >= dt_2.rows,\
            "ROWS CHECK: {dt_1_name} ({dt_1_rows}) >= {dt_2_name} ({dt_2_rows})".format(
                dt_1_name=dt_1.name,
                dt_1_rows=dt_1.rows,
                dt_2_name=dt_2.name,
                dt_2_rows=dt_2.rows)
        print("Asserted")

    @trace
    def test_rows_diff(self, dt_1: DataStepDataframe, dt_2: DataStepDataframe):
        assert dt_1.rows != dt_2.rows,\
            "ROWS CHECK: {dt_1_name} ({dt_1_rows}) >= {dt_2_name} ({dt_2_rows})".format(
                dt_1_name=dt_1.name,
                dt_1_rows=dt_1.rows,
                dt_2_name=dt_2.name,
                dt_2_rows=dt_2.rows)
        print("Asserted")

    @trace
    def test_negative_values(self, cols: List[str], dt: DataStepDataframe):
        for col in cols:
            assert col not in dt.columns_negative(), f"Dataset {dt.name} has columns with negative values -> {col}"

    @trace
    def test_null_values(self, cols: List[str], dt: DataStepDataframe):
        for col in cols:
            assert col not in dt.columns_null(), f"Dataset {dt.name} has columns with null values -> {col}"

    @trace
    def test_is_dataframe_empty(self, df: PyDataFrame):
        count = df.count()
        assert count > 0, "the dataframe count is zero"

    @property
    def output_data(self) -> DataStepDataframe:
        return self.__output_data

    @trace
    def check_output(self, **kwargs):
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, (PyDataFrame, PdDataFrame)):
                    msg = "Output Pandas or PySpark dataframe must be encapsulated into DataStepDataframe"
                    Log.get_instance().log_error(msg)
                    raise ValueError(msg)
                elif isinstance(value, DataStepDataframe):
                    value.log(direction=DataDirection.OUT)
                else:
                    Log.get_instance().log_info(f'{key}:{value}')
                # setattr(self, key, value)

    @trace
    def set_output_data(self, dataframe: Union[PyDataFrame, PdDataFrame], name='', cache: bool = False):
        name = self.display_name if not name else name
        self.__output_data = DataStepDataframe(
            name=name,
            dataframe=dataframe,
            cache=cache)
        self.__output_data.log_out()

    @trace
    @abstractmethod
    def initialize(self):
        """
        Define the DataStep logic.
        """
        pass

    @trace
    @abstractmethod
    def tests(self):
        """
        Define all the the tests that this step must pass
        """
        pass
