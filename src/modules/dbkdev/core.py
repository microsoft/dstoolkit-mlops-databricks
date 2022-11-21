# Databricks notebook source

from pyspark.sql import DataFrame, SparkSession
from dbkcore.core import trace, Log
import re
from pathlib import Path
from typing import Any, List
from typeguard import typechecked
from dbkcore.core import Singleton
from enum import Enum
import pkg_resources





class IdeEnvironment(str, Enum):
    LOCAL = "local"
    DATABRICKS = "databricks"


@typechecked
class DevelopmentClient(metaclass=Singleton):
    """
    Client to use for local Databricks' local development
    """

    # @trace
    def __init__(
        self,
        dbutils,
        spark: SparkSession,
        ide_environment: IdeEnvironment
        # deployment_environment: DeploymentEnvironment
    ):
        """
        Instantiates this object

        Parameters
        ----------
        dbutils : Dbutils
            The Dbutils instance to use
        spark : SparkSession
            The SparkSession to use
        ide_environment : IdeEnvironment
            The environment used
        deployment_environment : DeploymentEnvironment
            The deployment environment
        """
        self.__spark = spark
        self.__dbutils = dbutils
        self.__ide_environment = ide_environment

    @property
    def spark(self) -> SparkSession:
        return self.__spark

    @property
    def dbutils(self) -> Any:
        return self.__dbutils

    @property
    def __storage_account_name(self) -> str:
        res = self.dbutils.secrets.get(
            scope="storage",
            key="name"
        )
        return res

    @property
    def __blob_container_name(self) -> str:
        res = self.dbutils.secrets.get(
            scope="storage",
            key="container"
        )
        return res

    @property
    def __storage_account_access_key(self) -> str:
        res = self.dbutils.secrets.get(
            scope="storage",
            key="key"
        )
        return res

    @property
    def ide_environment(self) -> IdeEnvironment:
        return self.__ide_environment

    @trace(attrs_refact=['dbkea_token'])
    def set_dbkea(self, dbkea_token: str):
        """
        To use when the environment is LOCAL for using the dbutils secrets

        Parameters
        ----------
        dbkea_token : str
            The token
        """
        self.__dbkea_token = dbkea_token
        self.dbutils.secrets.setToken(dbkea_token)

    @property
    def mount_name(self) -> str:
        """
        Standard name of the root mount for the configured storage account and container

        Returns
        -------
        str
            [description]
        """
        res = "{}_{}".format(self.__storage_account_name, self.__blob_container_name)
        return res

    @property
    def mount_path(self) -> str:
        """
        Standard mount path

        Returns
        -------
        str
            The path
        """
        res = 'dbfs:/mnt/{}/'.format(self.mount_name)
        return res

    @trace
    def read_csv(self, file_path: str) -> DataFrame:
        # blob_path = self.__blob_path(file_path)
        blob_df = self.spark.read.format("csv").\
            option("inferSchema", "true").\
            option("header", "true").\
            option("delimiter", ",").\
            option("charset", "utf-8").load(file_path)
        return blob_df

    @trace
    def read_parquet(self, file_path: str) -> DataFrame:
        return self.spark.read.parquet(file_path)

    @trace
    def save_temp_table(
        self,
        dataframe: DataFrame,
        # schema: str,
        table_name: str,
        cache=True
    ):
        # TODO: Documentation

        # self.create_schema(schema)
        # dbk_table_name = f"{schema}_{table_name}"
        Log.get_instance().log_info(f"Creating temp table: {table_name}")
        if cache:
            dataframe.cache().createOrReplaceGlobalTempView(table_name)
        else:
            dataframe.createOrReplaceGlobalTempView(table_name)

    @trace
    def load_temp_table(
        self,
        # schema: str,
        table_name: str
    ) -> DataFrame:
        # TODO: Documentation

        Log.get_instance().log_info(f"Loading temp table: {table_name}")
        # self.create_schema(schema)
        # dbk_table_name = f"{schema}.{table_name}"
        global_temp_db = self.spark.conf.get("spark.sql.globalTempDatabase")
        # dt = self.spark.conf.get(f"spark.sql.{dbk_table_name}")
        dt = self.spark.read.table(f"{global_temp_db}.{table_name}")
        return dt

    @trace
    def save_delta_table(
        self,
        dataframe: DataFrame,
        schema: str,
        table_name: str,
        output_path: Path,
        partition_columns: List[str] = None,
        mode: str = 'overwrite',
        overwrite_schema: bool = False
    ):
        """
        Saves the dataframe as a delta table in an external location
        Parameters
        ----------
        dataframe : DataFrame
            The dataframe
        schema : str
            Destination schema
        table_name : str
            Destination schema
        output_path : Path
            Folder where to save the dataframe
        partition_columns: List[str]
            Columns to use for partitioning, default is None
        mode: str
           e.g. append, overwrite, passed to dataframe.write.saveAsTable
        """
        self.create_schema(schema)
        dbk_table_name = f"{schema}.{table_name}"
        # mnt_path = str(Path('mnt', mount_name, root_path, schema, table_name))
        # path = f"dbfs:/{mnt_path}"
        path = output_path.joinpath(dbk_table_name.replace('.', '_'))
        self._save_table(
            dataframe=dataframe,
            table_name=dbk_table_name,
            path=str(path),
            format='delta',
            mode=mode,
            partition_columns=partition_columns,
            overwrite_schema=overwrite_schema
        )

    @trace
    def _save_table(
        self,
        dataframe: DataFrame,
        table_name: str,
        path: str,
        format: str,
        mode: str,
        partition_columns: List[str] = None,
        overwrite_schema=False
    ):
        """
        Saves the given dataframe into a delta table

        Parameters
        ----------
        dataframe : DataFrame
            Dataframe to save
        schema : str
            Schema into save
        table_name : str
            Name of the table
        """
        # TODO: Update documentation
        if table_name is None or "":
            raise Exception("Table name missing")
        if not path:
            raise Exception("Path missing")
        # Create hive table from a dataframe (for the final ETL process)
        # self.spark.sql("DROP TABLE IF EXISTS {}".format(table_name))
        dataframe.write.saveAsTable(table_name, mode=mode, format=format, path=path, partitionBy=partition_columns, overwriteSchema=overwrite_schema)

    @trace
    def table_exists(
        self,
        schema_name: str,
        table_name: str
    ):
        return table_name in [t.name for t in self.spark.catalog.listTables(schema_name)]

    @trace
    def list_tables(self, schema: str) -> List[str]:
        """
        List the tables in the given schema

        Parameters
        ----------
        schema : str
            The Databricks schema

        Returns
        -------
        List[str]
            List of tables
        """
        df = self.spark.sql("show tables in {}".format(schema)).toPandas()
        table_name = df["tableName"]  # ! Could be wrong
        return list(table_name)

    @trace
    def list_databases(self) -> List[str]:
        """
        Gets the list of Databricks databases (a.k.a. schemas)

        Returns
        -------
        List[str]
            List of schemas
        """
        df = self.spark.sql("show schemas").toPandas()
        databases = df["databaseName"].tolist()
        return databases

    @trace
    def create_schema(self, schema_databricks: str):
        """
        Creates a schema in Databricks

        Parameters
        ----------
        schema_databricks : str
            Name of the schema
        """
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS {}".format(schema_databricks))

    @trace
    def mount_exists(self, mount_name: str) -> bool:
        mounts = self.list_mounts()
        names = [m.name.replace('/', '') for m in mounts]
        res = True if mount_name in names else False
        return res

    @trace
    def list_mounts(self) -> list:
        mounts = self.files('/mnt')
        return mounts

    @trace
    def files(self, path: str) -> list:
        files = self.dbutils.fs.ls(path)
        return files

    @classmethod
    def get_instance(cls):
        return DevelopmentClient()


class DevelopmentEngine(metaclass=Singleton):

    def __init__(self):
        self.spark = self.__get_spark()
        dbutils, ide_environment = self.__get_dbutils(self.spark)
        self.dbutils = dbutils
        self.ide_environment = ide_environment

    def __get_spark(self) -> SparkSession:
        MAX_MEMORY = "5g"
        spark = SparkSession.builder.\
            config("spark.executor.memory", MAX_MEMORY).\
            config("spark.driver.memory", MAX_MEMORY).\
            config("spark.driver.maxResultSize", MAX_MEMORY).\
            getOrCreate()
        return spark
        # import IPython
        # user_ns = IPython.get_ipython().user_ns
        # if "spark" in user_ns:
        #     return user_ns["spark"]
        # else:
        #     from pyspark.sql import SparkSession
        #     user_ns["spark"] = SparkSession.builder.getOrCreate()
        #     return user_ns["spark"]

    def __get_dbutils(self, spark: SparkSession):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            env = IdeEnvironment.LOCAL if "databricks-connect" in [i.key for i in pkg_resources.working_set] else IdeEnvironment.DATABRICKS
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
            env = IdeEnvironment.DATABRICKS
        return (dbutils, env)

    @classmethod
    def get_instance(cls):
        return DevelopmentEngine()
