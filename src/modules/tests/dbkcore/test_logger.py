"""Test script."""

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

import os
from dotenv import load_dotenv
import pytest
load_dotenv(override=True)


from dbkcore.core import trace
from dbkcore.core import Log
Log("Unit Tests", os.environ['APPI_IK'])


@trace
def func_to_test_div(a, b):
    res = a / b
    return res


def test_log_info():
    Log.get_instance().log_info("Test info")


def test_log_debug():
    Log.get_instance().log_debug("Test debug")


def test_log_critical():
    Log.get_instance().log_critical("Test critical")


def test_log_warning():
    Log.get_instance().log_warning("Test warning")


def test_log_error():
    Log.get_instance().log_error("Test error")


def test_trace():
    func_to_test_div(a=3, b=2)


def test_trace_error():
    with pytest.raises(ZeroDivisionError):
        try:
            func_to_test_div(a=1, b=0)
        except ZeroDivisionError as e:
            Log.get_instance().log_error(f"Function call failed: {e}")
            raise e
