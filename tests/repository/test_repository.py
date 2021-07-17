import contextvars
from pytest import fixture, mark
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


class ConcreteRepository(Repository):
    async def add(self, item):
        pass

    async def remove(self, item):
        pass

    async def count(self, domain):
        pass

    async def search(
            self, domain, limit=None, offset=None, order=None):
        pass


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
