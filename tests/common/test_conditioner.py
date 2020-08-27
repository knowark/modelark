import inspect
from pytest import fixture
from modelark.common import Conditioner, DefaultConditioner


def test_conditioner_definition():
    functions = [item[0] for item in inspect.getmembers(
        Conditioner, predicate=inspect.isfunction)]
    assert 'parse' in functions


def test_default_conditioner_parse():
    conditioner = DefaultConditioner()

    assert conditioner.parse([]) == ('1 = 1', ())


def test_default_conditioner_parse_equality():
    conditioner = DefaultConditioner()

    assert conditioner.parse([('field', '=', '5')]) == (
        "field = ANY(ARRAY['5'])", ('5',))
    assert conditioner.parse([('field', 'in', ['5', '6'])]) == (
        "field = ANY(ARRAY['5', '6'])", ('5', '6'))
