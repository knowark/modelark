import inspect
from pytest import fixture
from modelark.common import Locator, DefaultLocator


def test_locator_definition():
    properties = [item[0] for item in inspect.getmembers(
        Locator, lambda o: isinstance(o, property))]
    assert 'location' in properties
    assert 'zone' in properties


def test_default_locator():
    locator = DefaultLocator('servagro', 'southwest')
    assert locator.location == 'servagro'
    assert locator.zone == 'southwest'
