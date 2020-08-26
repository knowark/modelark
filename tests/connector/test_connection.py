import inspect
from pytest import fixture
from modelark.connector import Connection


def test_connection_definition():
    functions = [item[0] for item in inspect.getmembers(
        Connection, predicate=inspect.isfunction)]
    assert 'execute' in functions
    assert 'fetch' in functions
