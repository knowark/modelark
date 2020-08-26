import inspect
from pytest import fixture
from modelark.connector import Connector


def test_connector_definition():
    functions = [item[0] for item in inspect.getmembers(
        Connector, predicate=inspect.isfunction)]
    assert 'get' in functions
    assert 'put' in functions
