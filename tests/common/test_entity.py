from pytest import fixture
from modelark.common import Entity


@fixture
def entity():
    return Entity(
        id='1'
    )


def test_entity_instantiation(entity):
    assert entity is not None


def test_entity_attributes(entity):
    assert entity.id == '1'
    assert entity.created_at == 0
    assert entity.updated_at == 0
    assert entity.created_by == ''
    assert entity.updated_by == ''
