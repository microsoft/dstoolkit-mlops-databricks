# Module `dbkcore`

## Sub-modules

  - [dbkcore.core](#module-`dbkcore.core`)
  - [dbkcore.helpers](#module-`dbkcore.helpers`)

----
# Module `dbkcore.core`

## Functions

### Function `trace`

> 
> 
>	 def trace(
>		 original_function: Any = None,
>		 *,
>		 attrs_refact: List[str] = None
>	 )

Log the function call.

###### Parameters

  - **`original_function`** : <code>Any</code>, optional  
	Function to trace, by default None
  - **`attrs_refact`** : <code>List\[str\]</code>, optional  
	List of parameters to hide from logging, by default None

## Classes

### Class `BaseObject`

> 
> 
>	 class BaseObject

Base class to use with any object new object. It implements the method
log which will be used for logging

#### Static methods

##### `Method class_name`

> 
> 
>	 def class_name(
>		 cls
>	 ) ‑> str

#### Methods

##### Method `log`

> 
> 
>	 def log(
>		 self,
>		 prefix='',
>		 suffix=''
>	 )

Specifices how to log the object

### Class `Log`

> 
> 
>	 class Log(
>		 name: str,
>		 connection_string: str = None
>	 )

Helper class for Application Insight Logger.

Create a new Log object.

#### Parameters

  - **`name`** : <code>str</code>  
	Name used by the logger for tracing
  - **`connection_string`** : <code>\[type\]</code>, optional  
	Application Insight’s connection string

#### Instance variables

##### Variable `logger`

Type: `logging.Logger`

Logger that will be used.

###### Returns

  - <code>Logger</code>  
	This logger

##### Variable `tracer`

Type: `opencensus.trace.tracer.Tracer`

Tracer that will be used.

###### Returns

  - <code>Tracer</code>  
	The tracer

#### Static methods

##### `Method get_instance`

> 
> 
>	 def get_instance()

Current instance

#### Methods

##### Method `log_critical`

> 
> 
>	 def log_critical(
>		 self,
>		 message: str,
>		 prefix='',
>		 custom_dimension: dict = None
>	 )

Log a message as critical.

###### Parameters

  - **`message`** : <code>str</code>  
	The message

##### Method `log_debug`

> 
> 
>	 def log_debug(
>		 self,
>		 message: str,
>		 prefix='',
>		 custom_dimension: dict = None
>	 )

Log a message as debug.

###### Parameters

  - **`message`** : <code>str</code>  
	The message

##### Method `log_error`

> 
> 
>	 def log_error(
>		 self,
>		 message: str,
>		 include_stack=True,
>		 prefix='',
>		 custom_dimension: dict = None
>	 )

Log a message as error.

###### Parameters

  - **`message`** : <code>str</code>  
	The message

##### Method `log_info`

> 
> 
>	 def log_info(
>		 self,
>		 message: str,
>		 prefix='',
>		 custom_dimension: dict = None
>	 )

Log a message as info.

###### Parameters

  - **`message`** : <code>str</code>  
	The message

##### Method `log_warning`

> 
> 
>	 def log_warning(
>		 self,
>		 message: str,
>		 prefix='',
>		 custom_dimension: dict = None
>	 )

Log a message as warning.

###### Parameters

  - **`message`** : <code>str</code>  
	The message

##### Method `trace_function`

> 
> 
>	 def trace_function(
>		 self,
>		 name: str,
>		 kwargs: dict
>	 ) ‑> Optional[opencensus.trace.span.Span]

Traces a function

###### Parameters

  - **`name`** : <code>str</code>  
	Name of the function used for tracing
  - **`name`** : <code>kwargs</code>  
	The parameters of the function

###### Returns

  - <code>Span</code>  
	A Span that can be used for customizing logging

### Class `Singleton`

> 
> 
>	 class Singleton(
>		 *args,
>		 **kwargs
>	 )

Create a singleton.

#### Ancestors (in MRO)

  - [builtins.type](#module-`builtins.type`)

----
# Module `dbkcore.helpers`

Various utilities for speed up development

## Functions

### Function `add_folder_in_current_directory`

> 
> 
>	 def add_folder_in_current_directory(
>		 folder_name: str
>	 ) ‑> bool

Add a folder in the current directory.

###### Parameters

  - **`folder_name`** : <code>str</code>  
	New folder name

###### Returns

  - <code>bool</code>  
	True if success

### Function `current_directory`

> 
> 
>	 def current_directory() ‑> str

Get current directory.

###### Returns

  - <code>str</code>  
	The current directory path

### Function `is_json_serializable`

> 
> 
>	 def is_json_serializable(
>		 x: Any
>	 ) ‑> bool

Check if the object is serializable.

###### Parameters

  - **`x`** : <code>Any</code>  
	Object to validate

###### Returns

  - <code>bool</code>  
	True if success

----


# Module `dbkdev`

## Sub-modules

  - [dbkdev.core](#module-`dbkdev.core`)
  - [dbkdev.data\_steps](#module-`dbkdev.data_steps`)

----
# Module `dbkdev.core`

## Classes

### Class `DevelopmentClient`

> 
> 
>	 class DevelopmentClient(
>		 dbutils,
>		 spark: pyspark.sql.session.SparkSession,
>		 ide_environment: dbkdev.core.IdeEnvironment
>	 )

Client to use for local Databricks’ local development

Instantiates this object

#### Parameters

  - **`dbutils`** : <code>Dbutils</code>  
	The Dbutils instance to use
  - **`spark`** : <code>SparkSession</code>  
	The SparkSession to use
  - **`ide_environment`**
	: <code>[IdeEnvironment](#module-`dbkdev.core.IdeEnvironment "dbkdev.core.IdeEnvironment"`)</code>  
	The environment used
  - **`deployment_environment`** : <code>DeploymentEnvironment</code>  
	The deployment environment

#### Instance variables

##### Variable `dbutils`

Type: `Any`

##### Variable `ide_environment`

Type: `dbkdev.core.IdeEnvironment`

##### Variable `mount_name`

Type: `str`

Standard name of the root mount for the configured storage account and
container

###### Returns

  - <code>str</code>  
	\[description\]

##### Variable `mount_path`

Type: `str`

Standard mount path

###### Returns

  - <code>str</code>  
	The path

##### Variable `spark`

Type: `pyspark.sql.session.SparkSession`

#### Static methods

##### `Method get_instance`

> 
> 
>	 def get_instance()

#### Methods

##### Method `create_schema`

> 
> 
>	 def create_schema(
>		 self,
>		 schema_databricks: str
>	 )

Creates a schema in Databricks

###### Parameters

  - **`schema_databricks`** : <code>str</code>  
	Name of the schema

##### Method `files`

> 
> 
>	 def files(
>		 self,
>		 path: str
>	 ) ‑> list

##### Method `list_databases`

> 
> 
>	 def list_databases(
>		 self
>	 ) ‑> List[str]

Gets the list of Databricks databases (a.k.a. schemas)

###### Returns

  - <code>List\[str\]</code>  
	List of schemas

##### Method `list_mounts`

> 
> 
>	 def list_mounts(
>		 self
>	 ) ‑> list

##### Method `list_tables`

> 
> 
>	 def list_tables(
>		 self,
>		 schema: str
>	 ) ‑> List[str]

List the tables in the given schema

###### Parameters

  - **`schema`** : <code>str</code>  
	The Databricks schema

###### Returns

  - <code>List\[str\]</code>  
	List of tables

##### Method `load_temp_table`

> 
> 
>	 def load_temp_table(
>		 self,
>		 table_name: str
>	 ) ‑> pyspark.sql.dataframe.DataFrame

##### Method `mount_exists`

> 
> 
>	 def mount_exists(
>		 self,
>		 mount_name: str
>	 ) ‑> bool

##### Method `read_csv`

> 
> 
>	 def read_csv(
>		 self,
>		 file_path: str
>	 ) ‑> pyspark.sql.dataframe.DataFrame

##### Method `read_parquet`

> 
> 
>	 def read_parquet(
>		 self,
>		 file_path: str
>	 ) ‑> pyspark.sql.dataframe.DataFrame

##### Method `save_delta_table`

> 
> 
>	 def save_delta_table(
>		 self,
>		 dataframe: pyspark.sql.dataframe.DataFrame,
>		 schema: str,
>		 table_name: str,
>		 output_path: pathlib.Path,
>		 partition_columns: List[str] = None,
>		 mode: str = 'overwrite',
>		 overwrite_schema: bool = False
>	 )

Saves the dataframe as a delta table in an external location
\#\#\#\#\#\# Parameters

  - **`dataframe`** : <code>DataFrame</code>  
	The dataframe
  - **`schema`** : <code>str</code>  
	Destination schema
  - **`table_name`** : <code>str</code>  
	Destination schema
  - **`output_path`** : <code>Path</code>  
	Folder where to save the dataframe
  - **`partition_columns`** : <code>List\[str\]</code>  
	Columns to use for partitioning, default is None
  - **`mode`** : <code>str</code>  
	 

e.g. append, overwrite, passed to dataframe.write.saveAsTable

##### Method `save_temp_table`

> 
> 
>	 def save_temp_table(
>		 self,
>		 dataframe: pyspark.sql.dataframe.DataFrame,
>		 table_name: str,
>		 cache=True
>	 )

##### Method `set_dbkea`

> 
> 
>	 def set_dbkea(
>		 self,
>		 dbkea_token: str
>	 )

To use when the environment is LOCAL for using the dbutils secrets

###### Parameters

  - **`dbkea_token`** : <code>str</code>  
	The token

##### Method `table_exists`

> 
> 
>	 def table_exists(
>		 self,
>		 schema_name: str,
>		 table_name: str
>	 )

### Class `DevelopmentEngine`

> 
> 
>	 class DevelopmentEngine

#### Static methods

##### `Method get_instance`

> 
> 
>	 def get_instance()

### Class `IdeEnvironment`

> 
> 
>	 class IdeEnvironment(
>		 value,
>		 names=None,
>		 *,
>		 module=None,
>		 qualname=None,
>		 type=None,
>		 start=1
>	 )

An enumeration.

#### Ancestors (in MRO)

  - [builtins.str](#module-`builtins.str`)
  - [enum.Enum](#module-`enum.Enum`)

#### Class variables

##### Variable `DATABRICKS`

##### Variable `LOCAL`

----
# Module `dbkdev.data_steps`

## Functions

### Function `apply_test`

> 
> 
>	 def apply_test(
>		 func
>	 )

Execute test function after the initialize.

###### Notes

[Example](https://stackoverflow.com/a/15196410)

### Function `log_output`

> 
> 
>	 def log_output(
>		 func
>	 )

Decorator for executing test in sequence

###### Notes

[Example](https://stackoverflow.com/a/15196410)

### Function `pre_apply_test`

> 
> 
>	 def pre_apply_test(
>		 func
>	 )

Execute test function before the initialize.

###### Notes

[Example](https://stackoverflow.com/a/15196410)

## Classes

### Class `DataDirection`

> 
> 
>	 class DataDirection(
>		 value,
>		 names=None,
>		 *,
>		 module=None,
>		 qualname=None,
>		 type=None,
>		 start=1
>	 )

An enumeration.

#### Ancestors (in MRO)

  - [builtins.str](#module-`builtins.str`)
  - [enum.Enum](#module-`enum.Enum`)

#### Class variables

##### Variable `IN`

##### Variable `OUT`

### Class `DataStep`

> 
> 
>	 class DataStep(
>		 spark: pyspark.sql.session.SparkSession,
>		 run_id: str
>	 )

Creates a datastep to be used in a pipeline

#### Parameters

  - **`metaclass`** : <code>\[type\]</code>, optional  
	\[description\], by default abc.ABCMeta

#### Raises

  - <code>Exception</code>  
	\[description\]

#### Instance variables

##### Variable `display_name`

Type: `str`

##### Variable `output_data`

Type: `dbkdev.data_steps.DataStepDataframe`

#### Methods

##### Method `check_output`

> 
> 
>	 def check_output(
>		 self,
>		 **kwargs
>	 )

##### Method `initialize`

> 
> 
>	 def initialize(
>		 self
>	 )

Define the DataStep logic.

##### Method `pandas_read_csv`

> 
> 
>	 def pandas_read_csv(
>		 self,
>		 path: pathlib.Path
>	 ) ‑> dbkdev.data_steps.DataStepDataframe

##### Method `set_output_data`

> 
> 
>	 def set_output_data(
>		 self,
>		 dataframe: Union[pyspark.sql.dataframe.DataFrame, pandas.core.frame.DataFrame],
>		 name='',
>		 cache: bool = False
>	 )

##### Method `spark_read_csv`

> 
> 
>	 def spark_read_csv(
>		 self,
>		 path: pathlib.Path
>	 ) ‑> dbkdev.data_steps.DataStepDataframe

##### Method `spark_read_parquet_path`

> 
> 
>	 def spark_read_parquet_path(
>		 self,
>		 path: pathlib.Path,
>		 cache=False
>	 ) ‑> dbkdev.data_steps.DataStepDataframe

##### Method `spark_read_table`

> 
> 
>	 def spark_read_table(
>		 self,
>		 name: str
>	 ) ‑> dbkdev.data_steps.DataStepDataframe

##### Method `spark_read_temp_table`

> 
> 
>	 def spark_read_temp_table(
>		 self,
>		 name: str
>	 ) ‑> dbkdev.data_steps.DataStepDataframe

##### Method `test_is_dataframe_empty`

> 
> 
>	 def test_is_dataframe_empty(
>		 self,
>		 df: pyspark.sql.dataframe.DataFrame
>	 )

##### Method `test_negative_values`

> 
> 
>	 def test_negative_values(
>		 self,
>		 cols: List[str],
>		 dt: dbkdev.data_steps.DataStepDataframe
>	 )

##### Method `test_null_values`

> 
> 
>	 def test_null_values(
>		 self,
>		 cols: List[str],
>		 dt: dbkdev.data_steps.DataStepDataframe
>	 )

##### Method `test_rows_diff`

> 
> 
>	 def test_rows_diff(
>		 self,
>		 dt_1: dbkdev.data_steps.DataStepDataframe,
>		 dt_2: dbkdev.data_steps.DataStepDataframe
>	 )

##### Method `test_rows_eq`

> 
> 
>	 def test_rows_eq(
>		 self,
>		 dt_1: dbkdev.data_steps.DataStepDataframe,
>		 dt_2: dbkdev.data_steps.DataStepDataframe
>	 )

##### Method `test_rows_geq`

> 
> 
>	 def test_rows_geq(
>		 self,
>		 dt_1: dbkdev.data_steps.DataStepDataframe,
>		 dt_2: dbkdev.data_steps.DataStepDataframe
>	 )

##### Method `test_rows_leq`

> 
> 
>	 def test_rows_leq(
>		 self,
>		 dt_1: dbkdev.data_steps.DataStepDataframe,
>		 dt_2: dbkdev.data_steps.DataStepDataframe
>	 )

##### Method `tests`

> 
> 
>	 def tests(
>		 self
>	 )

Define all the the tests that this step must pass

### Class `DataStepDataframe`

> 
> 
>	 class DataStepDataframe(
>		 name: str,
>		 dataframe: Union[pyspark.sql.dataframe.DataFrame, pandas.core.frame.DataFrame],
>		 cache=False
>	 )

Base class to use with any object new object. It implements the method
log which will be used for logging

#### Ancestors (in MRO)

  - [dbkcore.core.BaseObject](#module-`dbkcore.core.BaseObject`)

#### Instance variables

##### Variable `is_pandas`

Type: `bool`

##### Variable `is_pyspark`

Type: `bool`

##### Variable `rows`

#### Methods

##### Method `columns_negative`

> 
> 
>	 def columns_negative(
>		 self
>	 ) ‑> List[str]

Identifies the columns with negative values

###### Returns

  - <code>List\[str\]</code>  
	Column names

##### Method `columns_null`

> 
> 
>	 def columns_null(
>		 self
>	 ) ‑> List[str]

Identifies the columns with null values

###### Returns

  - <code>List\[str\]</code>  
	Column names

##### Method `log`

> 
> 
>	 def log(
>		 self,
>		 direction: dbkdev.data_steps.DataDirection
>	 )

Specifices how to log the object

##### Method `log_in`

> 
> 
>	 def log_in(
>		 self
>	 )

##### Method `log_out`

> 
> 
>	 def log_out(
>		 self
>	 )

##### Method `to_pandas`

> 
> 
>	 def to_pandas(
>		 self
>	 ) ‑> pandas.core.frame.DataFrame

----


# Module `acai_ml`

## Sub-modules

  - [acai\_ml.core](#module-`acai_ml.core`)

----
# Module `acai_ml.core`

## Classes

### Class `Engine`

> 
> 
>	 class Engine

This is the core of the framework. It configures the environment to
interact with the remote Databricks.

Instantiate the current object

#### Static methods

##### `Method get_instance`

> 
> 
>	 def get_instance()

Current singleton Engine

###### Returns

  - <code>[Engine](#module-`acai_ml.core.Engine "acai_ml.core.Engine"`)</code>  
	The Engine

##### `Method ide_environment`

> 
> 
>	 def ide_environment() ‑> dbkdev.core.IdeEnvironment

Current Ide Environment

###### Returns

  - <code>IdeEnvironment</code>  
	The Ide Environment

##### `Method is_ide_dataricks`

> 
> 
>	 def is_ide_dataricks() ‑> bool

Checks if the current environment is Databricks

###### Returns

  - <code>bool</code>  
	Check result

##### `Method is_ide_local`

> 
> 
>	 def is_ide_local() ‑> bool

Checks if the current environment is Local

###### Returns

  - <code>bool</code>  
	Check result

#### Methods

##### Method `dbutils`

> 
> 
>	 def dbutils(
>		 self
>	 )

Current dbutils

###### Returns

  - <code>DBUtils</code>  
	The DBUtils

##### Method `initialize_env`

> 
> 
>	 def initialize_env(
>		 self
>	 )

Initializes the DevelopmentClient. That is, sets the dbutils and spark
context accordingly if the code is runt on cluster or locally.

##### Method `initialize_logger`

> 
> 
>	 def initialize_logger(
>		 self,
>		 pipeline_name: str,
>		 appi_ik_scope: str = 'appinsight',
>		 appi_ik_secret: str = 'appsecret'
>	 )

Initializes the logger

###### Parameters

  - **`pipeline_name`** : <code>str</code>  
	Name to use with the logger. It will be the base name used for all
	the upcoming logs and tracing
  - **`appi_ik_scope`** : <code>str</code>, optional  
	Databricks secret scope where the Application Insight key is stored,
	by default “dds”
  - **`appi_ik_secret`** : <code>str</code>, optional  
	Databricks secret name where the Application Insight key is stored,
	by default “appiik”

###### Raises

  - <code>ValueError</code>  
	Unknown Ide Environment used

##### Method `run_notebook`

> 
> 
>	 def run_notebook(
>		 self,
>		 notebook: str,
>		 args: Dict[~KT, ~VT],
>		 timeout=86400,
>		 error_raise=True
>	 )

##### Method `run_notebook_with_retry`

> 
> 
>	 def run_notebook_with_retry(
>		 self,
>		 notebook: str,
>		 args: Dict[~KT, ~VT],
>		 timeout=86400,
>		 max_retries=3
>	 )

Runs the specified notebook through dbutils

###### Parameters

  - **`notebook`** : <code>str</code>  
	Name or path of the notebook
  - **`args`** : <code>Dict</code>  
	\[description\]
  - **`timeout`** : <code>int</code>, optional  
	\[description\], by default 86400
  - **`max_retries`** : <code>int</code>, optional  
	\[description\], by default 3

###### Returns

\[type\] \[description\]

###### Raises

  - <code>e</code>  
	\[description\]

##### Method `spark`

> 
> 
>	 def spark(
>		 self
>	 ) ‑> pyspark.sql.session.SparkSession

Current spark context

###### Returns

  - <code>SparkSession</code>  
	Spark context

----
