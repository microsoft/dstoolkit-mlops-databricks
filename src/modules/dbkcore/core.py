from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.tracer import Tracer
from opencensus.trace.span import Span
from opencensus.ext.azure.log_exporter import AzureLogHandler
import logging
from logging import Logger
from abc import abstractmethod
from typeguard import typechecked
from .helpers import is_json_serializable
from datetime import datetime
import functools as _functools
from typing import Any, List, Union
from collections import OrderedDict
import json





class Singleton(type):
    """Create a singleton."""

    _instances = OrderedDict()

    def __call__(cls, *args, **kwargs):
        """
        Instantiate the singleton.

        Returns
        -------
        any
            Parameters of the singleton
        """
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Log(metaclass=Singleton):
    """Helper class for Application Insight Logger."""

    def __init__(self, name: str, connection_string: str = None):
        """
        Create a new Log object.

        Parameters
        ----------
        name : str
            Name used by the logger for tracing
        connection_string : [type], optional
            Application Insight's connection string
        """
        self.name = name
        self.__connection_string = connection_string

        # config_integration.trace_integrations(['logging'])
        # [Documentation](https://docs.microsoft.com/it-it/azure/azure-monitor/app/opencensus-python#logs)
        # [Documentation](https://docs.microsoft.com/it-it/azure/azure-monitor/app/opencensus-python#trace)
        self.__logger = self._get_logger()
        self.__tracer = self._get_tracer()

    def _get_logger(self) -> Logger:
        """
        Create the logger with an Azure Handler for Application Insight.

        Returns
        -------
        Logger
            Current logger
        """
        logger = logging.getLogger(name=self.name)
        logger.setLevel(logging.DEBUG)
        if self.__connection_string:
            handler = AzureLogHandler(connection_string=self.__connection_string)
            # handler.export_interval = 1
            # handler.max_batch_size = 1
            # handler.setFormatter(logging.Formatter('%(traceId)s:%(spanId)s:%(message)s'))
            logger.addHandler(handler)
        return logger

    def _get_tracer(self) -> Tracer:
        """
        Create the Opencencus Tracer with Azure Exporter.

        Returns
        -------
        Tracer
            Opencencus Tracer
        """
        if self.__connection_string:
            tracer = Tracer(
                exporter=AzureExporter(connection_string=self.__connection_string),
                sampler=AlwaysOnSampler()
            )
        else:
            tracer = None
        return tracer

    @classmethod
    def get_instance(cls):
        """Current instance"""
        return Log()

    @typechecked
    def trace_function(self, name: str, kwargs: dict) -> Union[Span, None]:
        """
        Traces a function

        Parameters
        ----------
        name : str
            Name of the function used for tracing

        name : kwargs
            The parameters of the function

        Returns
        -------
        Span
            A Span that can be used for customizing logging
        """
        tracer = self.__tracer
        if tracer:
            span = self.__tracer.span(name=name)
            if kwargs:
                for key, value in kwargs.items():
                    # if hasattr(value, 'to_json_logger'):
                    #     value = value.to_json_logger()
                    if not is_json_serializable(value):
                        value = str(value)
                    span.add_attribute(key, value)
                    # self.log_info(f"TRACING:{key}:{value}")
        else:
            span = None
        return span

    @property
    def tracer(self) -> Tracer:
        """
        Tracer that will be used.

        Returns
        -------
        Tracer
            The tracer
        """
        return self.__tracer

    @property
    def logger(self) -> Logger:
        """
        Logger that will be used.

        Returns
        -------
        Logger
            This logger
        """
        return self.__logger

    def __log_message(self, message: str, prefix: str) -> str:
        if prefix:
            msg = f'{prefix}:{message}'
        else:
            msg = f'{message}'
        # res = f"{self.name}:{msg}"
        return msg

    def log_info(self, message: str, prefix="", custom_dimension: dict = None):
        """
        Log a message as info.

        Parameters
        ----------
        message : str
            The message
        """
        msg = self.__log_message(message=message, prefix=prefix)
        local_msg = f'{msg}\nDetails: {json.dumps(custom_dimension, indent=4)}' if custom_dimension else msg
        print(f'INFO:{local_msg}')
        properties = {'custom_dimensions': custom_dimension}
        self.__logger.info(msg, extra=properties)

    def log_debug(self, message: str, prefix="", custom_dimension: dict = None):
        """
        Log a message as debug.

        Parameters
        ----------
        message : str
            The message
        """
        msg = self.__log_message(message=message, prefix=prefix)
        local_msg = f'{msg}|Custom dimensions: {json.dumps(custom_dimension)}' if custom_dimension else msg
        print(f'DEBUG:{local_msg}')
        # logging.debug(msg=f'{msg}| Custom dimensions: {json.dumps(custom_dimension)}' if custom_dimension else msg)
        properties = {'custom_dimensions': custom_dimension if custom_dimension else {}}
        self.__logger.debug(msg=msg, extra=properties)

    def log_warning(self, message: str, prefix="", custom_dimension: dict = None):
        """
        Log a message as warning.

        Parameters
        ----------
        message : str
            The message
        """
        msg = self.__log_message(message=message, prefix=prefix)
        local_msg = f'{msg}|Custom dimensions: {json.dumps(custom_dimension)}' if custom_dimension else msg
        print(f'WARNING:{local_msg}')
        # logging.warning(msg=f'{msg}| Custom dimensions: {json.dumps(custom_dimension)}' if custom_dimension else msg)
        properties = {'custom_dimensions': custom_dimension if custom_dimension else {}}
        self.__logger.warning(msg=msg, extra=properties)

    def log_error(self, message: str, include_stack=True, prefix="", custom_dimension: dict = None):
        """
        Log a message as error.

        Parameters
        ----------
        message : str
            The message
        """
        msg = self.__log_message(message=message, prefix=prefix)
        local_msg = f'{msg}|Custom dimensions: {json.dumps(custom_dimension)}' if custom_dimension else msg
        print(f'ERROR:{local_msg}')
        # logging.error(msg=f'{msg}| Custom dimensions: {json.dumps(custom_dimension)}' if custom_dimension else msg, exc_info=include_stack)
        properties = {'custom_dimensions': custom_dimension if custom_dimension else {}}
        self.__logger.error(msg=msg, exc_info=include_stack, extra=properties)

    def log_critical(self, message: str, prefix="", custom_dimension: dict = None):
        """
        Log a message as critical.

        Parameters
        ----------
        message : str
            The message
        """
        msg = self.__log_message(message=message, prefix=prefix)
        local_msg = f'{msg}|Custom dimensions: {json.dumps(custom_dimension)}' if custom_dimension else msg
        print(f'CRITICAL:{local_msg}')
        properties = {'custom_dimensions': custom_dimension if custom_dimension else {}}
        logging.critical(msg=f'{msg}| Custom dimensions: {json.dumps(custom_dimension)}' if custom_dimension else msg)
        self.__logger.critical(msg=msg, extra=properties)


@typechecked
def trace(original_function: Any = None, *, attrs_refact: List[str] = None):
    """
    Log the function call.

    Parameters
    ----------
    original_function : Any, optional
        Function to trace, by default None
    attrs_refact : List[str], optional
        List of parameters to hide from logging, by default None
    """

    def __log(func, fn_k, *args, **kwargs):
        start = datetime.utcnow()
        # Log.get_instance().log_info(f"MODULE:{func.__module__}:FN:{func.__qualname__}:START @ {start}", custom_dimension=fn_k)
        res = func(*args, **kwargs)
        end = datetime.utcnow()
        elapsed = end - start
        # Log.get_instance().log_info(f"MODULE:{func.__module__}:FN:{func.__qualname__}:COMPLETE @ {end}:ELAPSED {elapsed}", custom_dimension=fn_k)
        fn_k['elapsed'] = str(elapsed)
        fn_k['module'] = str(func.__module__)
        fn_k['qualname'] = str(func.__qualname__)
        Log.get_instance().log_info(f"Executed function {func.__module__}.{func.__qualname__}", custom_dimension=fn_k)
        return res

    """Decorator for tracing functions (link)[https://stackoverflow.com/a/24617244]"""
    def _decorate(func):
        @_functools.wraps(func)
        def wrapper(*args, **kwargs):
            fn_k = {}
            # if not attrs_refact:
            #     fn_k = kwargs
            # else:
            for key, value in kwargs.items():
                v = value if is_json_serializable(value) else 'not serializable'
                if attrs_refact:
                    if key in attrs_refact:
                        v = '***'
                fn_k[key] = v
                # if key not in attrs_refact:
                #     fn_k[key] = value
                # else:
                #     fn_k[key] = '***'
            if Log.get_instance().tracer:
                with Log.get_instance().trace_function(
                    name=func.__name__,
                    kwargs=fn_k
                ):
                    return __log(func, fn_k, *args, **kwargs)
                    # start = datetime.utcnow()
                    # # Log.get_instance().log_info(f"MODULE:{func.__module__}:FN:{func.__qualname__}:START @ {start}", custom_dimension=fn_k)
                    # res = func(*args, **kwargs)
                    # end = datetime.utcnow()
                    # elapsed = end - start
                    # # Log.get_instance().log_info(f"MODULE:{func.__module__}:FN:{func.__qualname__}:COMPLETE @ {end}:ELAPSED {elapsed}", custom_dimension=fn_k)
                    # fn_k['elapsed'] = str(elapsed)
                    # fn_k['module'] = str(func.__module__)
                    # fn_k['qualname'] = str(func.__qualname__)
                    # Log.get_instance().log_info(f"Executed function {func.__module__}.{func.__qualname__}", custom_dimension=fn_k)
                    # return res
            else:
                return __log(func, fn_k, *args, **kwargs)
                # start = datetime.utcnow()
                # # Log.get_instance().log_info(f"MODULE:{func.__module__}:FN:{func.__qualname__}:START @ {start}", custom_dimension=fn_k)
                # res = func(*args, **kwargs)
                # end = datetime.utcnow()
                # elapsed = end - start
                # # Log.get_instance().log_info(f"MODULE:{func.__module__}:FN:{func.__qualname__}:COMPLETE @ {end}:ELAPSED {elapsed}", custom_dimension=fn_k)
                # fn_k['elapsed'] = str(elapsed)
                # fn_k['module'] = str(func.__module__)
                # fn_k['qualname'] = str(func.__qualname__)
                # Log.get_instance().log_info(f"Executed function {func.__module__}.{func.__qualname__}", custom_dimension=fn_k)
        return wrapper

    if original_function:
        return _decorate(original_function)

    return _decorate


@typechecked
# class BaseObject(ABC):
# TODO: if works, remove ABC class
class BaseObject():
    """
    Base class to use with any object new object.
    It implements the method log which will be used for logging

    """

    @abstractmethod
    def log(self, prefix="", suffix=""):
        """
        Specifices how to log the object
        """
        pass

    @classmethod
    def class_name(cls) -> str:
        return cls.__name__.lower()
