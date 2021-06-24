from asyncio import sleep
from typing import Callable
from pytest import fixture, mark, raises
from modelark.common import Entity
from modelark.filterer import Domain
from modelark.repository import Repository, MemoryRepository


pytestmark = mark.asyncio


class Alpha(Entity):
    def __init__(self, **attributes) -> None:
        super().__init__(**attributes)
        self.field_1 = attributes.get('field_1', "")


class Beta(Entity):
    def __init__(self, **attributes) -> None:
        super().__init__(**attributes)
        self.alpha_id = attributes.get('alpha_id', "")


class Gamma(Entity):
    def __init__(self, **attributes) -> None:
        super().__init__(**attributes)


class Delta(Entity):
    def __init__(self, **attributes) -> None:
        super().__init__(**attributes)
        self.alpha_id = attributes.get('alpha_id', "")
        self.gamma_id = attributes.get('gamma_id', "")


def test_memory_repository_implementation() -> None:
    assert issubclass(MemoryRepository, Repository)


@fixture
def alpha_memory_repository() -> MemoryRepository[Alpha]:
    class AlphaMemoryRepository(MemoryRepository[Alpha]):
        model = Alpha

    return AlphaMemoryRepository().load({
        "default": {
            "1": Alpha(id='1', field_1='value_1'),
            "2": Alpha(id='2', field_1='value_2'),
            "3": Alpha(id='3', field_1='value_3')
        }
    })


@fixture
def beta_memory_repository() -> MemoryRepository[Beta]:
    class BetaMemoryRepository(MemoryRepository[Beta]):
        model = Beta

    return BetaMemoryRepository().load({
        "default": {
            "1": Beta(id='1', alpha_id='1'),
            "2": Beta(id='2', alpha_id='1'),
            "3": Beta(id='3', alpha_id='2')
        }
    })


@fixture
def gamma_memory_repository() -> MemoryRepository[Gamma]:
    class GammaMemoryRepository(MemoryRepository[Gamma]):
        model = Gamma

    return GammaMemoryRepository().load({
        "default": {
            "1": Gamma(id='1'),
            "2": Gamma(id='2'),
            "3": Gamma(id='3')
        }
    })


@fixture
def delta_memory_repository() -> MemoryRepository[Delta]:
    class DeltaMemoryRepository(MemoryRepository[Delta]):
        model = Delta

    return DeltaMemoryRepository().load({
        "default": {
            "1": Delta(id='1', alpha_id='1', gamma_id='1'),
            "2": Delta(id='2', alpha_id='1', gamma_id='2'),
            "3": Delta(id='3', alpha_id='2', gamma_id='3'),
            "4": Delta(id='3', alpha_id='3', gamma_id='3')
        }
    })


def test_memory_repository_model(alpha_memory_repository) -> None:
    assert alpha_memory_repository.model is Alpha


def test_memory_repository_not_implemented_model() -> None:
    repository = MemoryRepository[Alpha]()
    with raises(NotImplementedError):
        repository.model


async def test_memory_repository_search_limit(alpha_memory_repository):
    items = await alpha_memory_repository.search([], limit=2)

    assert len(items) == 2


async def test_memory_repository_search_limit_none(alpha_memory_repository):
    items = await alpha_memory_repository.search([], limit=None, offset=None)

    assert len(items) == 3


async def test_memory_repository_search_offset(alpha_memory_repository):
    items = await alpha_memory_repository.search([], offset=2)

    assert len(items) == 1


async def test_memory_repository_order(alpha_memory_repository):
    items = await alpha_memory_repository.search([], order='field_1')

    assert len(items) == 3
    assert items[0].id == '1'
    assert items[1].id == '2'
    assert items[2].id == '3'


async def test_memory_repository_order_desc(alpha_memory_repository):
    items = await alpha_memory_repository.search([], order='field_1 DESC')

    assert len(items) == 3
    assert items[0].id == '3'
    assert items[1].id == '2'
    assert items[2].id == '1'


async def test_memory_repository_order_multiple(alpha_memory_repository):
    items = await alpha_memory_repository.search(
        [], order='field_1 DESC, id ASC')

    assert len(items) == 3
    assert items[0].id == '3'
    assert items[1].id == '2'
    assert items[2].id == '1'


async def test_memory_repository_add(alpha_memory_repository) -> None:
    item = Alpha(id="4", field_1="value_1")

    await alpha_memory_repository.add(item)

    assert len(alpha_memory_repository.data['default']) == 4
    assert "4" in alpha_memory_repository.data['default'].keys()
    assert item in alpha_memory_repository.data['default'].values()


async def test_memory_repository_add_update(alpha_memory_repository) -> None:
    created_entity = Alpha(id="1", field_1="value_1")
    created_entity, *_ = await alpha_memory_repository.add(created_entity)

    await sleep(1)

    updated_entity = Alpha(id="1", field_1="New Value")
    updated_entity, *_ = await alpha_memory_repository.add(updated_entity)

    assert created_entity.created_at < updated_entity.created_at

    items = alpha_memory_repository.data['default']
    assert len(items) == 3
    assert "1" in items.keys()
    assert updated_entity in items.values()
    assert "New Value" in items['1'].field_1


async def test_memory_repository_add_no_id(alpha_memory_repository) -> None:
    item = Alpha(field_1="value_1")

    await alpha_memory_repository.add(item)

    items = alpha_memory_repository.data['default']
    assert len(items) == 4
    assert len(list(items.keys())[0]) > 0
    assert item in items.values()


async def test_memory_repository_add_multiple(alpha_memory_repository):
    items = [
        Alpha(field_1="value_1"),
        Alpha(field_1="value_2")
    ]

    returned_items = await alpha_memory_repository.add(items)

    items = alpha_memory_repository.data['default']
    assert len(returned_items) == 2
    assert returned_items[0].field_1 == 'value_1'
    assert returned_items[1].field_1 == 'value_2'


async def test_memory_repository_search(alpha_memory_repository):
    domain = [('field_1', '=', "value_3")]

    items = await alpha_memory_repository.search(domain)

    assert len(items) == 1
    for item in items:
        assert item.id == '3'
    assert item.field_1 == "value_3"


async def test_memory_repository_search_all(alpha_memory_repository):
    items = await alpha_memory_repository.search([])

    assert len(items) == 3


async def test_memory_repository_search_limit_zero(alpha_memory_repository):
    items = await alpha_memory_repository.search([], limit=0)

    assert len(items) == 0


async def test_memory_repository_join_one_to_many(
        alpha_memory_repository, beta_memory_repository):

    for parent, children in await alpha_memory_repository.join(
            [('id', '=', '1')], join=beta_memory_repository):
        assert isinstance(parent, Alpha)
        assert len(children) == 2
        assert all(isinstance(beta, Beta) for beta in children)


async def test_memory_repository_join_many_to_one(
        alpha_memory_repository, beta_memory_repository):

    for element, siblings in await beta_memory_repository.join(
        [('id', '=', '1')], join=alpha_memory_repository,
            link=beta_memory_repository):

        assert isinstance(element, Beta)
        assert len(siblings) == 1
        assert isinstance(next(iter(siblings)), Alpha)


async def test_memory_repository_join_many_to_many(
        alpha_memory_repository, gamma_memory_repository,
        delta_memory_repository):

    for alpha, gammas in await alpha_memory_repository.join(
        [('id', '=', '1')], join=gamma_memory_repository,
            link=delta_memory_repository):

        assert isinstance(alpha, Alpha)
        assert len(gammas) == 2
        assert gammas[0].id == '1'
        assert gammas[1].id == '2'


async def test_memory_repository_remove_true(alpha_memory_repository):
    item = alpha_memory_repository.data['default']["2"]
    deleted = await alpha_memory_repository.remove(item)

    items = alpha_memory_repository.data['default']
    assert deleted is True
    assert len(items) == 2
    assert "2" not in items


async def test_memory_repository_remove_false(alpha_memory_repository):
    item = Alpha(**{'id': '6', 'field_1': 'MISSING'})
    deleted = await alpha_memory_repository.remove(item)

    items = alpha_memory_repository.data['default']
    assert deleted is False
    assert len(items) == 3


async def test_memory_repository_remove_idempotent(alpha_memory_repository):
    existing_item = item = alpha_memory_repository.data['default']["2"]
    missing_item = Alpha(**{'id': '6', 'field_1': 'MISSING'})

    items = alpha_memory_repository.data['default']

    deleted = await alpha_memory_repository.remove(
        [existing_item, missing_item])

    assert deleted is True
    assert len(items) == 2

    deleted = await alpha_memory_repository.remove(
        [existing_item, missing_item])

    assert deleted is False
    assert len(items) == 2


async def test_memory_repository_count(alpha_memory_repository):
    count = await alpha_memory_repository.count()

    assert count == 3


async def test_memory_repository_count_domain(alpha_memory_repository):
    domain = [('field_1', '=', "value_3")]
    count = await alpha_memory_repository.count(domain)

    assert count == 1
