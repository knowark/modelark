import inspect
from pytest import fixture
from modelark.common import Filterer, DefaultFilterer


def test_filterer_definition():
    functions = [item[0] for item in inspect.getmembers(
        Filterer, predicate=inspect.isfunction)]
    assert 'parse' in functions


def test_default_filterer_parse():
    filterer = DefaultFilterer()

    filter = filterer.parse([])
    assert filter({}) is True


def test_default_conditioner_parse_equality():
    filterer = DefaultFilterer()

    filter = filterer.parse([('field', '=', '5')])

    class FooModel:
        field = '3'

    assert filter(FooModel()) is False

    class BarModel:
        field = '5'

    assert filter(BarModel()) is True
