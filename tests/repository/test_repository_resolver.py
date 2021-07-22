from pytest import fixture, raises
from modelark.common import Entity
from modelark.repository import Repository, RepositoryResolver


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


class Alpha(Entity):
    """Alpha Entity"""


class Beta(Entity):
    """Beta Entity"""


class AlphaRepository(ConcreteRepository):
    model = Alpha


class BetaRepository(ConcreteRepository):
    model = Beta


@fixture
def repository_resolver():
    repositories = [
        AlphaRepository(),
        BetaRepository()
    ]
    return RepositoryResolver(repositories)


def test_repository_resolver_instantiation(repository_resolver):
    assert repository_resolver is not None


def test_repository_resolver_resolve(repository_resolver):
    alpha_repository = repository_resolver.resolve('Alpha')
    beta_repository = repository_resolver.resolve('Beta')

    assert isinstance(alpha_repository, AlphaRepository)
    assert isinstance(beta_repository, BetaRepository)


def test_repository_resolver_missing(repository_resolver):
    with raises(KeyError):
        repository_resolver.resolve('Missing')
