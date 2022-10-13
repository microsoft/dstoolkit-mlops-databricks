from dbkdev.core import DevelopmentClient
from dbkdev.core import DevelopmentEngine
from dbkcore.core import Log
from dbkcore.core import trace
from dbkcore.core import Singleton
from dbkdev.core import IdeEnvironment
from pyspark.sql import SparkSession
from typing import Dict


class Engine(metaclass=Singleton):

    """
    This is the core of the framework.
    It configures the environment to interact with the remote Databricks.
    """

    def __init__(self):
        """
        Instantiate the current object

        """
        self.__ide_environment = DevelopmentEngine().get_instance().ide_environment
        self.appi_ik = None

    def initialize_env(self):
        """
        Initializes the DevelopmentClient.
        That is, sets the dbutils and spark context accordingly if the code is runt on cluster or locally.
        """
        DevelopmentClient(
            dbutils=DevelopmentEngine().get_instance().dbutils,
            spark=DevelopmentEngine().get_instance().spark,
            ide_environment=self.__ide_environment
        )

    # def initialize_import(self):
    #     # Setting pipeline module path
    #     if self.__ide_environment == IdeEnvironment.DATABRICKS:
    #         import sys
    #         sys.path.append(str(self.pipelines_lib_path))

    def initialize_logger(
        self,
        pipeline_name: str,
        appi_ik_scope: str = 'appinsight',
        appi_ik_secret: str = 'appsecret'
    ):
        """
        Initializes the logger

        Parameters
        ----------
        pipeline_name : str
            Name to use with the logger. It will be the base name used for all the upcoming logs and tracing
        appi_ik_scope : str, optional
            Databricks secret scope where the Application Insight key is stored, by default "dds"
        appi_ik_secret : str, optional
            Databricks secret name where the Application Insight key is stored, by default "appiik"

        Raises
        ------
        ValueError
            Unknown Ide Environment used
        """
        # Configuring application insight key
        if self.__ide_environment == IdeEnvironment.LOCAL:
            from dbkenv.core import Configuration
            configurations = Configuration()
            self.appi_ik = configurations.APPINSIGHT_CONNECTIONSTRING
        elif self.__ide_environment == IdeEnvironment.DATABRICKS:
            self.appi_ik = DevelopmentEngine().get_instance().dbutils.secrets.get(appi_ik_scope, appi_ik_secret)
        else:
            raise ValueError(f'ide_environment unknown: {self.__ide_environment}')
        # Instantiating logger
        Log(pipeline_name, self.appi_ik)

    def spark(self) -> SparkSession:
        """
        Current spark context

        Returns
        -------
        SparkSession
            Spark context
        """
        return DevelopmentClient().get_instance().spark

    def dbutils(self):
        """
        Current dbutils

        Returns
        -------
        DBUtils
            The DBUtils
        """
        return DevelopmentClient().get_instance().dbutils

    @classmethod
    def get_instance(cls):
        """
        Current singleton Engine

        Returns
        -------
        Engine
            The Engine
        """
        return Engine()

    @staticmethod
    def ide_environment() -> IdeEnvironment:
        """
        Current Ide Environment

        Returns
        -------
        IdeEnvironment
            The Ide Environment
        """
        return DevelopmentClient().get_instance().ide_environment

    @staticmethod
    def is_ide_dataricks() -> bool:
        """
        Checks if the current environment is Databricks

        Returns
        -------
        bool
            Check result
        """
        return DevelopmentClient().get_instance().ide_environment == IdeEnvironment.DATABRICKS

    @staticmethod
    def is_ide_local() -> bool:
        """
        Checks if the current environment is Local

        Returns
        -------
        bool
            Check result
        """
        return DevelopmentClient().get_instance().ide_environment == IdeEnvironment.LOCAL

    def run_notebook_with_retry(self, notebook: str, args: Dict, timeout=86400, max_retries=3):
        """
        Runs the specified notebook through dbutils

        Parameters
        ----------
        notebook : str
            Name or path of the notebook
        args : Dict
            [description]
        timeout : int, optional
            [description], by default 86400
        max_retries : int, optional
            [description], by default 3

        Returns
        -------
        [type]
            [description]

        Raises
        ------
        e
            [description]
        """
        num_retries = 0
        while True:
            try:
                return DevelopmentClient().get_instance().dbutils.notebook.run(notebook, timeout, args)
            except Exception as e:
                if num_retries > max_retries:
                    raise e
                else:
                    print("Retrying error"), e
                    num_retries += 1

    @trace
    # TODO: rename check
    def run_notebook(self, notebook: str, args: Dict, timeout=86400, error_raise=True):
        try:
            res = DevelopmentClient().get_instance().dbutils.notebook.run(notebook, timeout, args)
        except Exception as e:
            res = f"Notebook {notebook} failed"
            Log().get_instance().log_error(res)
            if error_raise:
                raise e
        return res
