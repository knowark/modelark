import contextvars
from pytest import fixture, mark
from modelark.common import Entity
from modelark.repository import Repository


pytestmark = mark.asyncio


def test_repository_interface_methods():
    methods = Repository.__abstractmethods__  # type: ignore
    assert 'add' in methods
    assert 'search' in methods
    assert 'remove' in methods
    assert 'count' in methods


def test_repository_support_methods():
    assert 'join' in dir(Repository)


class ConcreteEntity(Entity):
    def __init__(self, **attributes) -> None:
        super().__init__(**attributes)
        self.name = attributes.get('name', '')


class ConcreteRepository(Repository):
    def __init__(self, **kwargs) -> None:
        self.search_result = kwargs.get('search_result', [])

    async def add(self, item):
        pass

    async def remove(self, item):
        pass

    async def count(self, domain):
        pass

    async def search(
            self, domain, limit=None, offset=None, order=None):
        self.search_arguments = [domain, limit, offset, order]
        return self.search_result


def test_repository_context_definition():
    concrete_repository = ConcreteRepository()
    assert isinstance(
        concrete_repository.context, contextvars.ContextVar)

    token = concrete_repository.context.set({'meta': 'data'})

    value = concrete_repository.context.get()
    assert value == {'meta': 'data'}

    concrete_repository.context.reset(token)

    value = concrete_repository.context.get()
    assert value == {}


async def test_repository_meta():
    concrete_repository = ConcreteRepository()

    with concrete_repository.meta({'extra': 'data'}) as context:
        assert context.get() == {'extra': 'data'}

    assert context.get() == {}


async def test_repository_find_list_scalar():
    items = [
        ConcreteEntity(id='C001'),
        ConcreteEntity(id='C002'),
        ConcreteEntity(id='C003')
    ]
    concrete_repository = ConcreteRepository(search_result=items)
    records = ['C002', 'C003']

    found = await concrete_repository.find(records)

    assert found == items[1:]
    assert concrete_repository.search_arguments == [
        [('id', 'in', ['C002', 'C003'])], None, None, None]


async def test_repository_find_list_of_dicts():
    items = [
        ConcreteEntity(id='C001'),
        ConcreteEntity(id='C002'),
        ConcreteEntity(id='C003')
    ]
    concrete_repository = ConcreteRepository(search_result=items)
    records = [{'id': 'C002'}, {'id': 'C003'}]

    found = await concrete_repository.find(records)

    assert found == items[1:]
    assert concrete_repository.search_arguments == [
        [('id', 'in', ['C002', 'C003'])], None, None, None]


async def test_repository_find_list_of_dicts_missing():
    items = [
        ConcreteEntity(id='C001'),
        ConcreteEntity(id='C002'),
        ConcreteEntity(id='C003')
    ]
    concrete_repository = ConcreteRepository(search_result=items)
    records = [{'id': 'C999'}, {'id': 'C003'}, {'name': 'John'}]

    found = await concrete_repository.find(records)

    assert found == [None, items[2], None]
    assert concrete_repository.search_arguments == [
        [('id', 'in', ['C999', 'C003', None])], None, None, None]


async def test_repository_find_by_field():
    items = [
        ConcreteEntity(id='C001', name='John'),
        ConcreteEntity(id='C002', name='Bob'),
        ConcreteEntity(id='C003')
    ]
    concrete_repository = ConcreteRepository(search_result=items)
    records = [{'id': 'C999'}, {'id': 'C003'}, {'name': 'John'}]

    found = await concrete_repository.find(records, 'name')

    assert found == [None, None, items[0]]
    assert concrete_repository.search_arguments == [
        [('name', 'in', [None, None, 'John'])], None, None, None]
