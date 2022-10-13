import databricks_cli.sdk as _dbkcli
import databricks_cli.sdk.service as _dss

from databricks_api import DatabricksAPI
from dbkcore.core import trace
from dbkcore.core import Log
# from dbkcore.core import Log
import os as _os
import base64 as _base64
import tempfile as _tempfile
import time as _time
import enum as _enum
import typing as _typing
from dotenv import load_dotenv





class Configuration():
    """Retrieve the keys used from the package from the local environment."""

    def __init__(self, file_load: bool = False):
        """
        Initialize the configuration class.

        Parameters
        ----------
        file_load : bool, optional
            Search .env file and loads it, by default False
        """

        if file_load:
            load_dotenv(override=True)
        # self._APPINSIGHT_CONNECTIONSTRING = _os.getenv("APPI_IK")
        # self.DATABRICKS_HOST = _os.getenv('DATABRICKS_HOST')
        # self.DATABRICKS_TOKEN = _os.getenv('DATABRICKS_TOKEN')
        # self.DATABRICKS_ORDGID = _os.getenv('DATABRICKS_ORDGID')
        # self.AZURE_SUBSCRIPTIONID = _os.getenv('AZURE_SUBSCRIPTIONID')
        # self.RESOURCEGROUP_NAME = _os.getenv('RESOURCEGROUP_NAME')
        # self.RESOURCEGROUP_REGION = _os.getenv('RESOURCEGROUP_REGION')

    @property
    def APPINSIGHT_CONNECTIONSTRING(self) -> str:
        """
        Application insight connection string.

        Returns
        -------
        str
            The connection string
        """
        res = _os.environ["APPI_IK"]


        # Test Setting These As Environment Variables in The PipeLine And See If you Can Print Them Here.
        return res

    @property
    def DATABRICKS_HOST(self) -> str:
        """
        Databricks host url.

        Returns
        -------
        str
            The host url
        """
        ###Amended DATABRICKS_HOST --> param_DATABRICKS_HOST
        res = _os.environ["DATABRICKS_HOST"]
        print("Databricks Host")
        print(res)
        ############################################
        return res

    @property
    def DATABRICKS_TOKEN(self) -> str:
        """
        Databricks personal roken.

        Returns
        -------
        str
            The token
        """
        ##Amended DATABRICKS_TOKEN --> param_DATABRICKS_TOKEN
        res = _os.environ["DATABRICKS_TOKEN"]
        print("Databricks Token")
        print(res)
        ######################################################
        return res

    @property
    def DATABRICKS_ORDGID(self) -> str:
        """
        Databricks organization id.

        Returns
        -------
        str
            The id
        """
        res = _os.environ["DATABRICKS_ORDGID"]
        print("Databricks ORG ID")
        print(res)
        return res


class DatabricksEnvironment(str, _enum.Enum):
    """
    Describes the type of environment used
    """
    LOCAL = 'local'
    DATABRICKS = 'databricks'


class Package():
    """
    Rapresents the python package installed in databricks

    Reference
    ---------

    Documentation [link](https://docs.databricks.com/dev-tools/api/latest/libraries.html#example-response)
    """

    @trace
    def __init__(self, origin: str, package: str, repo: str):
        """
        Creates this object

        Parameters
        ----------
        origin : str
            Origin of the package
        package : str
            Name and version of the package
        repo : str
            The repository of the package
        """
        super().__init__()
        self.origin = origin
        self.package = package
        self.repo = repo

    @trace
    def to_api_json(self):
        return {
            self.origin: {
                'package': self.package,
                'repo': self.repo
            }
        }


class ResourceClient():
    """
    Client used to interact with a Databricks cluster
    """

    @trace(attrs_refact=['personal_token'])
    def __init__(self, host: str, personal_token: str):
        """
        Instantiates this object

        Parameters
        ----------
        host : str
            Host of the cluster
        personal_token : str
            Databricks personal token
        """
        super().__init__()
        self.host = host
        self.personal_token = personal_token

        # This is client
        self.__api_client = None

    @property
    def apiClient(self) -> _dbkcli.ApiClient:
        """
        Creates the Databricks API client

        Returns
        -------
        ApiClient
            The client
        """

        if not self.__api_client:
            self.__api_client = DatabricksAPI(host=self.host, token=self.personal_token)
    
            print("API CLIENT")
            print(self.__api_client)

        return self.__api_client


class Cluster():
    """
    Manages a Databricks cluster
    """

    @trace
    def __init__(
        self,
        client: ResourceClient,
        cluster_name: str,
        cluster_configuration: dict
    ):
        """
        Instantiates this object

        Parameters
        ----------
        client : ResourceClient
            A ResourceClient object
        cluster_name : str
            Name of the cluster
        cluster_configuration : dict
            Dictionary that contains the cluster configuration
        appinsight_instrumentation_key : str
            Application Insights' instrumentation key
        """
        self.client = client
        self.__cluster_name = cluster_name
        self.cluster_configuration = cluster_configuration
        # self.appinsight_instrumentation_key = appinsight_instrumentation_key
        self.__cluster_service = None

    @property
    def cluster_service(self) -> _dss.ClusterService:
        if not self.__cluster_service:
            # This step is when we specify 'Cluster'
            #Previous Step was: db = client.apiClient
            # In THIS step we create the Cluster Service: cs = db.cluster
            # In the Next Steo, we can use the Cluster Service to Do things: clusterID = cs.list_clusters
            
            
            #HEEEEEEREEEEEEE
            
            self.__cluster_service = self.client.apiClient
        # This will return cluster service when Cluster() (Above) is called
        return self.__cluster_service

    @trace
    def create_cluster_and_wait(self, redeploy=False) -> bool:
        """
        Creates the cluster and waits for its done

        Parameters
        ----------
        redeploy: bool
            Redeploy the cluster
        """
        if self.cluster_configuration:
            deploy = False
            if self.cluster_exists():
                if redeploy:
                    print("Cluster {} exists. Dropping and recreating".format(self.cluster_name))
                    self.delete_cluster()
                    deploy = True
                else:
                    deploy = False
            else:
                deploy = True

            if deploy:
                spark_env_vars = self.cluster_configuration["spark_env_vars"]
                # spark_env_vars["APPINSIGHT_CONNECTIONSTRING"] = self.appinsight_instrumentation_key
                spark_env_vars["EXECUTER"] = f"DATABRICKS_{self.cluster_name}"
                self.cluster_service.create_cluster(
                    num_workers=self.cluster_configuration.get("num_workers"),
                    autoscale=self.cluster_configuration.get("autoscale"),
                    cluster_name=self.cluster_configuration.get("cluster_name"),
                    spark_version=self.cluster_configuration.get("spark_version"),
                    spark_conf=self.cluster_configuration.get("spark_conf"),
                    node_type_id=self.cluster_configuration.get("node_type_id"),
                    driver_node_type_id=self.cluster_configuration.get("driver_node_type_id"),
                    spark_env_vars=spark_env_vars,
                    autotermination_minutes=self.cluster_configuration.get("autotermination_minutes"),
                    enable_elastic_disk=self.cluster_configuration.get("enable_elastic_disk")
                )

                searched_times = 0
                while not self.cluster_exists():
                    searched_times += 1
                    _time.sleep(10)
                    if searched_times > 10:
                        raise Exception("Cluster failed to deploy")
                self.start_cluster_and_wait()
        else:
            print("Can't deploy since cluster configuration is missing")
        return True

    @trace
    def databricks_list_clusters(self) -> _typing.List[str]:
        """
        List clusters in Databricks

        Returns
        -------
        List[str]
            List of clusters
        """
        return self.cluster_service.list_clusters()

    @property
    def cluster_name(self) -> str:
        if self.cluster_configuration:
            return self.cluster_configuration.get("cluster_name")
        else:
            return self.__cluster_name

    @trace
    def cluster_started(self) -> bool:
        """
        Checks if the cluster is started

        Returns
        -------
        bool
            True if started
        """
        started = False
        if self.cluster_state() == "RUNNING":
            started = True
        return started

    @trace
    def cluster_exists(self) -> bool:
        """
        Checks is the cluster exists

        Returns
        -------
        bool
            True if exists
        """
        exists = False
        if self.cluster_id:
            exists = True
        return exists

    @trace
    def install_package(self, packages: _typing.List[Package]) -> str:
        """
        Installs the given packages

        Parameters
        ----------
        packages : List[Package]
            The packages to install

        Returns
        -------
        str
            Result from Databricks API call
        """
        mls = _dss.ManagedLibraryService(self.client.apiClient)
        pkgs = [p.to_api_json() for p in packages]
        res = mls.install_libraries(self.cluster_id, pkgs)
        return res

    @property
    def cluster_id(self) -> str:
        """
        Retrieves cluster's id

        Returns
        -------
        str
            Id of the cluster
        """
        print("Cluster Service")
        cs = self.cluster_service                                               # FOLLOW
        #print("Cluster Service")
        #print(cs)

        print("Cluster List")
        cluster_list = cs.cluster.list_clusters(headers=None)                   #FOLLOW
        print(cluster_list)

        id = None
        if cluster_list:
            matches = [c['cluster_id'] for c in cluster_list["clusters"] if c['cluster_name'] == self.cluster_name]
            if matches:
                id = matches[0]
        self.__cluster_id = id
        return self.__cluster_id

    @trace
    def delete_cluster_and_wait(self) -> bool:
        """
        Deletes the cluster and waits for completion

        """
        cs = self.cluster_service
        id = self.cluster_id

        is_deleted = False if id else True

        if not is_deleted:
            cs.cluster.permanent_delete_cluster(id)

            requests = 0
            seconds_interval = 20
            timeout_requests = 20

            is_deleted = True
            while self.cluster_exists():
                requests += 1
                _time.sleep(seconds_interval)
                message = "Waiting from {} seconds. Timeout at {}".format(
                    seconds_interval * requests,
                    timeout_requests * seconds_interval
                )

                Log.get_instance().log_info(message=message)

                if requests > 20:
                    is_deleted = False
        return is_deleted

    @trace
    def cluster_state(self) -> str:
        """
        Checks cluster state

        Returns
        -------
        str
            State of the cluster
        """
        cs = self.cluster_service
        cluster_state = cs.cluster.get_cluster(self.cluster_id)["state"]           #######
        return cluster_state

    @trace
    def start_cluster_and_wait(self) -> bool:
        """
        Starts the cluster and wait for it completion

        Returns
        -------
        bool
            True if started
        """
        cluster_id = self.cluster_id
        cs = self.cluster_service

        if self.cluster_state() == "RUNNING":
            return "Already running"
        elif self.cluster_state() == "PENDING":
            _time.sleep(20)
            # Waiting cluster to start
            requests = 0
            seconds_interval = 20
            timeout_requests = 20
            while self.cluster_state() == "PENDING":
                requests += 1
                _time.sleep(seconds_interval)
                message = "Waiting from {} seconds. Timeout at {}".format(
                    seconds_interval * requests,
                    timeout_requests * seconds_interval
                )
                Log.get_instance().log_info(message=message)
                if requests > 20:
                    raise Exception("Cluster not started")
            return True
        elif self.cluster_state() in ["TERMINATED", "TERMINATING"]:
            cs.cluster.start_cluster(cluster_id)                                  ###################### CHECK HERE ##############
            _time.sleep(20)
            # Waiting cluster to start
            requests = 0
            seconds_interval = 20
            timeout_requests = 20
            while self.cluster_state() == "PENDING":
                requests += 1
                _time.sleep(seconds_interval)
                message = "Waiting from {} seconds. Timeout at {}".format(
                    seconds_interval * requests,
                    timeout_requests * seconds_interval
                )
                Log.get_instance().log_info(message=message)
                if requests > 20:
                    raise Exception("Cluster not started")
            return True
        else:
            raise Exception("Unmanaged state: {}".format(self.cluster_state()))


class Jobs():
    """
    Rapresents a Databricks Job
    """
    @trace
    def __init__(
        self,
        client: ResourceClient
    ):
        """
        Instantiates this object

        Parameters
        ----------
        client : ResourceClient
            A client
        """
        self.client = client
        self.__jobs_service = _dss.JobsService(self.client.apiClient)

    @trace
    def run_notebook_and_wait(
        self,
        destination_path: str,
        cluster_id: str,
        delete_run=False,
    ) -> str:
        """
        Run a notebooks and waits for its completion

        Parameters
        ----------
        destination_path : str
            Notebooks path
        cluster_id : str
            Cluster's id
        delete_run : bool, optional
            Deletes the run onces it's completed, by default False

        Returns
        -------
        str
            Result from Databricks API call
        """
        djs = self.__jobs_service
        destination_path_dbfs = destination_path
        base = _os.path.basename(destination_path)
        filename = _os.path.splitext(base)[0]

        job = djs.create_job(
            name=filename,
            existing_cluster_id=cluster_id,
            notebook_task={"notebook_path": destination_path_dbfs}
        )

        job_id = job["job_id"]
        run = djs.run_now(job_id=job_id)
        run_id = run['run_id']
        run_status = djs.get_run(run_id)
        # run_state = run_status['state']

        while run_status['state']["life_cycle_state"] != "TERMINATED":
            _time.sleep(10)
            run_status = djs.get_run(run_id)
            if run_status['state']["life_cycle_state"] == 'INTERNAL_ERROR':
                raise Exception(run_status['state']["life_cycle_state"])

        output = None

        if run_status['state']['result_state'] == 'SUCCESS':
            output = djs.get_run_output(run_id).get('notebook_output').get('result')
        elif run_status['state']['result_state'] == 'FAILED':
            output = "FAILED"

        if delete_run:
            djs.delete_job(job_id)
            djs.delete_run(run_id)
        return output


class Secret():
    """
    Manages Databricks' secrets
    """

    @trace
    def __init__(
        self,
        client: ResourceClient
    ):
        """
        Instantiates this object.

        Parameters
        ----------
        client : ResourceClient
            A ResourceClient object
        """
        self.client = client
        self.__secret_service = None

    @property
    def secret_service(self):
        if not self.__secret_service:
            self.__secret_service = _dss.SecretService(self.client.apiClient)
        return self.__secret_service

    @trace
    def delete_scope(self, scope: str) -> str:
        """
        Deletes the given scope

        Parameters
        ----------
        scope : str
            Scope's name

        Returns
        -------
        str
            Result from Databricks API call
        """
        dbs = self.secret_service
        res = dbs.delete_scope(scope)
        return res

    @trace
    def add_scope(self, scope: str) -> str:
        """
        Creates the scope

        Parameters
        ----------
        scope : str
            Scope's name

        Returns
        -------
        str
            Result from Databricks API call
        """
        res = self.secret_service.create_scope(
            scope,
            initial_manage_principal='users'
        )
        return res

    @trace(attrs_refact=['secret_value'])
    def add_secret(self, scope: str, secret_name: str, secret_value: str) -> str:
        """
        Adds a secret to the given scope.
        If a secret already exists with the same name, it will be overwritten.

        Note
        ----
        The server encrypts the secret using the secret scope’s encryption settings before storing it. You must have WRITE or MANAGE permission on the secret scope.

        Parameters
        ----------
        scope : str
            Name of the scope
        secret_name : str
            Name of the secret
        secret_value : str
            Value of the secret

        Returns
        -------
        str
            Result from Databricks API call
        """
        dbs = self.secret_service
        res = dbs.put_secret(
            scope=scope,
            key=secret_name,
            string_value=secret_value,
            bytes_value=None
        )
        return res

    def scopes(self) -> _typing.List[str]:
        """
        Retrieve list of scopes.

        Returns
        -------
        List[str]
            List of scopes
        """
        dbs = self.secret_service
        sc = dbs.list_scopes()
        if sc:
            scopes = [s["name"] for s in sc["scopes"]]
        else:
            scopes = []
        return scopes

    @trace(attrs_refact=['secrets'])
    def create_scope_secrets(self, scope: str, secrets: dict):
        """
        Insert a secret under the provided scope with the given name.
        If the scope already exists, it will be dropped and recreated.
        If a secret already exists with the same name, it will be overwritten.

        Notes
        -----
        The server encrypts the secret using the secret scope’s encryption settings before storing it. You must have WRITE or MANAGE permission on the secret scope.

        Parameters
        ----------
        scope : str
            Name of the scope
        secret_names : [str]
            Secrets names that must be searched in the Key Vault
        """

        scopes = self.scopes()
        dbs = self.secret_service
        # The only way to update a secret is to delete it and reload it
        if scope in scopes:
            dbs.delete_scope(scope)

        self.add_scope(scope, dbs)
        for name, value in secrets.items():
            self.add_secret(
                scope=scope,
                name=name,
                value=value
            )

    @trace
    def list_scopes_secrets(self) -> _typing.Dict:
        """
        Returns all the scopes and their secrets

        Returns
        -------
        dict
            The secrets {"secret": ...}
        """
        dbs = self.secret_service
        scopes = dbs.list_scopes()["scopes"]
        sc_names = [sc["name"] for sc in scopes]
        res = {}
        for name in sc_names:
            sc_secrets = dbs.list_secrets(name)
            secrets = None
            if sc_secrets:
                secrets = [k["key"] for k in sc_secrets["secrets"]]
            res[name] = secrets
        return res


class Workspace():
    """
    Manages a Databricks Workspace
    """

    def __init__(
        self,
        client: ResourceClient
    ):
        """
        Instantiates this object

        Parameters
        ----------
        client : ResourceClient
            A ResourceClient object
        """
        self.client = client
        self.__workspace_service = None

    @property
    def workspace_service(self) -> _dss.WorkspaceService:
        """
        The Databricks Workspace Service

        Returns
        -------
        WorkspaceService
            Workspace service from Databricks API
        """
        if not self.__workspace_service:
            self.__workspace_service = _dss.WorkspaceService(self.client.apiClient)
        return self.__workspace_service

    @trace
    def upload_content(
        self,
        destination_path: str,
        content: str,
        format="SOURCE",  # TODO: Rename to content_format
        language="PYTHON",
        overwrite=True
    ) -> str:
        """
        Uploads content to the workspace

        Parameters
        ----------
        destination_path : str
            Destination of the file
        content : str
            File's content as string
        format : str, optional
            Databricks file format, by default "SOURCE"
        overwrite : bool, optional
            Overwrite the file if exists, by default True

        Returns
        -------
        str
            Result from Databricks API call
        """
        base64_bytes = _base64.b64encode(content.encode("utf-8"))
        base64_string = base64_bytes.decode('utf-8')
        dws = self.workspace_service
        file_dir = _os.path.dirname(destination_path)
        dws.mkdirs(file_dir)
        res = dws.import_workspace(
            path=destination_path,
            content=base64_string,
            format=format,
            language=language,
            overwrite=overwrite
        )
        return res

    @trace
    def list_content(self, destination_folder: str) -> _typing.List[str]:
        """
        Lists the content in the given folder

        Parameters
        ----------
        destination_folder : str
            Folder to check

        Returns
        -------
        List[str]
            List of files
        """
        dws = self.workspace_service
        content = dws.list(destination_folder)
        return content

    @trace
    def delete_content(self, destination_path: str) -> str:
        """
        Deletes the content at the given path

        Parameters
        ----------
        destination_path : str
            Path of the file

        Returns
        -------
        str
            Result from Databricks API call
        """
        dws = self.workspace_service
        res = dws.delete(path=destination_path)
        return res

    @trace
    def make_dir(self, path_dir: str) -> str:
        """
        Makes a directory in the given folder

        Parameters
        ----------
        path_dir : str
            Base directory to which add the folder

        Returns
        -------
        str
            Result from Databricks API call
        """
        dws = self.workspace_service
        res = dws.mkdirs(path_dir)
        return res


class DatabricksResourceManager():
    """
    The orchestrator for managing the Databricks resources with ease
    """

    @trace
    def __init__(
        self,
        client: ResourceClient,
        cluster_name: str,
        cluster_configuration: dict,
        log_to_appi: bool = False
    ):
        """
        Instantiates this object

        Parameters
        ----------
        client : ResourceClient
            A ResourceClient object
        cluster_name : str
            Name of the cluster
        cluster_configuration : Dict, optional
            The configuration of the cluster, by default None
        log_to_appi: bool
            Log to application insights
        """
        self.client = client


        self.cluster = Cluster(
            client=client,
            cluster_name=cluster_name,
            cluster_configuration=cluster_configuration
            # appinsight_instrumentation_key="",
        )


        #Here #############                                                                #####################
        self.jobs = Jobs(client=self.client)
        self.secret = Secret(client=self.client)
        self.workspace = Workspace(client=self.client)
        #self.log_to_appi = log_to_appi

    @trace
    def __dkea_token(self) -> str:
        """
        Returns the DKEA token needed for using dbutils secrets locally.

        Returns
        -------
        str
            The token
        """
        code = '''
        v = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        k = v.replace("Some(", "").replace(")", "")
        key = k[:4] + '-' + k[4:]

        dbutils.notebook.exit(key)
        '''
        output_notebook = self.run_python_code_on_notebook(code)
        dkea = output_notebook.replace('-', '')
        return dkea

    @trace
    def run_python_code_on_notebook(self, code: str) -> str:
        """
        Runs python code as a notebook

        Parameters
        ----------
        code : str
            The code to execute

        Returns
        -------
        str
            Results from Databricks API call
        """
        # TODO: Add _and_wait in the name
        temp_name = "{}.py".format(next(_tempfile._get_candidate_names()))
        defult_tmp_dir = _tempfile._get_default_tempdir()
        temp_file_path = _os.path.join(defult_tmp_dir, temp_name)
        self.workspace.upload_content(temp_file_path, code)
        output = self.jobs.run_notebook_and_wait(
            destination_path=temp_file_path,
            cluster_id=self.cluster.cluster_id,
            delete_run=True
        )
        self.workspace.delete_content(temp_file_path)
        return output

    @trace
    def run_notebook_and_wait(
        self,
        destination_path: str,
        delete_run=False
    ):
        return self.jobs.run_notebook_and_wait(
            destination_path=destination_path,
            cluster_id=self.cluster.cluster_id,
            delete_run=delete_run
        )
