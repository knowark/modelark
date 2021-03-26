import json
from asyncio import sleep
from inspect import cleandoc
from typing import Callable, List, Tuple, Dict, Mapping, Any
from pytest import fixture, mark, raises
from modelark.common import Entity, Domain
from modelark.connector import Connector, Connection
from modelark.repository import Repository, RestRepository


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


def test_rest_repository_implementation() -> None:
    assert issubclass(RestRepository, Repository)


@fixture
def mock_connector():
    class MockConnection:
        def __init__(self) -> None:
            self.execute_query = ''
            self.execute_args: Tuple = ()
            self.execute_kwargs: Dict = {}
            self.execute_result = ''
            self.fetch_query = ''
            self.fetch_args: Tuple = ()
            self.fetch_kwargs: Dict = {}
            self.fetch_result: List[Any] = []
            self.data: Dict = {}

        async def execute(self, query: str, *args, **kwargs) -> str:
            self.execute_query = query
            self.execute_args = args
            self.execute_kwargs = kwargs
            return self.execute_result

        async def fetch(self, query: str, *args, **kwargs) -> List[Mapping]:
            self.fetch_query = query
            self.fetch_args = args
            self.fetch_kwargs = kwargs
            return self.fetch_result

        def load(self, data) -> None:
            self.data = data

    class MockConnector:
        def __init__(self) -> None:
            self.connection = MockConnection()
            self.pool = [self.connection]

        async def get(self, zone='') -> Connection:
            return self.pool.pop()

        async def put(self, connection, zone='') -> None:
            self.pool.append(connection)

        def load(self, data) -> None:
            self.connection.load(data)

    return MockConnector()


@fixture
def alpha_rest_repository(mock_connector) -> RestRepository[Alpha]:
    class AlphaRestRepository(RestRepository[Alpha]):
        model = Alpha

    repository = AlphaRestRepository(
        'https://service.example.com/alphas', mock_connector, Alpha)
    mock_connector.load({
        "default": {
            "1": Alpha(id='1', field_1='value_1'),
            "2": Alpha(id='2', field_1='value_2'),
            "3": Alpha(id='3', field_1='value_3')
        }
    })
    return repository


# @fixture
# def beta_rest_repository(mock_connector) -> RestRepository[Beta]:
    # class BetaRestRepository(RestRepository[Beta]):
    # model = Beta

    # repository = BetaRestRepository(
    # 'betas', Beta, mock_connector
    # )
    # mock_connector.load({
    # "default": {
    # "1": Beta(id='1', alpha_id='1'),
    # "2": Beta(id='2', alpha_id='1'),
    # "3": Beta(id='3', alpha_id='2')
    # }
    # })
    # return repository


# @fixture
# def gamma_rest_repository(mock_connector) -> RestRepository[Gamma]:
    # class GammaRestRepository(RestRepository[Gamma]):
    # model = Gamma

    # repository = GammaRestRepository(
    # 'gammas', Gamma, mock_connector
    # )
    # mock_connector.load({
    # "default": {
    # "1": Gamma(id='1'),
    # "2": Gamma(id='2'),
    # "3": Gamma(id='3')
    # }
    # })
    # return repository


# @fixture
# def delta_rest_repository(mock_connector) -> RestRepository[Delta]:
    # class DeltaRestRepository(RestRepository[Delta]):
    # model = Delta

    # repository = DeltaRestRepository(
    # 'deltas', Delta, mock_connector
    # )
    # mock_connector.load({
    # "default": {
    # "1": Delta(id='1', alpha_id='1', gamma_id='1'),
    # "2": Delta(id='2', alpha_id='1', gamma_id='2'),
    # "3": Delta(id='3', alpha_id='2', gamma_id='3'),
    # "4": Delta(id='3', alpha_id='3', gamma_id='3')
    # }
    # })
    # return repository


def test_rest_repository_model(alpha_rest_repository) -> None:
    assert alpha_rest_repository.model is Alpha


async def test_rest_repository_search_all(alpha_rest_repository):
    items = await alpha_rest_repository.search([])

    connection = alpha_rest_repository.connector.connection

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'GET'


async def test_rest_repository_search_domain(alpha_rest_repository):
    domain = [('field_1', '=', "value_3")]
    items = await alpha_rest_repository.search(domain)

    connection = alpha_rest_repository.connector.connection

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'GET'
    assert kwargs['query_params'] == {
        "filter": '[["field_1", "=", "value_3"]]'
    }


async def test_rest_repository_search_no_constructor(alpha_rest_repository):
    alpha_rest_repository.constructor = None
    items = await alpha_rest_repository.search([])

    connection = alpha_rest_repository.connector.connection

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'GET'


async def test_rest_repository_search_limit(alpha_rest_repository):
    items = await alpha_rest_repository.search([], limit=2)

    connection = alpha_rest_repository.connector.connection

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'GET'
    assert kwargs['query_params']['limit'] == '2'


async def test_rest_repository_search_limit_none(alpha_rest_repository):
    items = await alpha_rest_repository.search([], limit=None, offset=None)

    connection = alpha_rest_repository.connector.connection

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs.get('limit') is None
    assert kwargs.get('offset') is None


async def test_rest_repository_search_offset(alpha_rest_repository):
    items = await alpha_rest_repository.search([], offset=2)

    connection = alpha_rest_repository.connector.connection

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs.get('limit') is None
    assert kwargs['query_params']['offset'] == '2'


async def test_rest_repository_add(alpha_rest_repository) -> None:
    item = Alpha(id="4", field_1="value_1")

    await alpha_rest_repository.add(item)

    connection = alpha_rest_repository.connector.connection

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'PUT'
    assert kwargs['payload'][0]['id'] == "4"
    assert kwargs['payload'][0]['field_1'] == "value_1"


async def test_rest_repository_add_multiple(alpha_rest_repository):
    items = [
        Alpha(id='1', field_1="value_1"),
        Alpha(id='2', field_1="value_2")
    ]

    await alpha_rest_repository.add(items)

    connection = alpha_rest_repository.connector.connection

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'PUT'
    assert kwargs['payload'][0]['id'] == "1"
    assert kwargs['payload'][0]['field_1'] == "value_1"
    assert kwargs['payload'][1]['id'] == "2"
    assert kwargs['payload'][1]['field_1'] == "value_2"


async def test_rest_repository_remove_true(alpha_rest_repository):
    item = Alpha(id="5", field_1="value_5")

    connection = alpha_rest_repository.connector.connection

    deleted = await alpha_rest_repository.remove(item)

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'DELETE'
    assert kwargs['path'] == '/5'

    assert deleted is True


async def test_rest_repository_remove_true_multiple(alpha_rest_repository):
    items = [
        Alpha(id="5", field_1="value_5"),
        Alpha(id="6", field_1="value_6")
    ]

    connection = alpha_rest_repository.connector.connection

    deleted = await alpha_rest_repository.remove(items)

    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'DELETE'
    assert kwargs['payload'] == ["5", "6"]
    assert kwargs.get('path') is None

    assert deleted is True


async def test_rest_repository_remove_false(alpha_rest_repository):
    items = []

    connection = alpha_rest_repository.connector.connection

    deleted = await alpha_rest_repository.remove(items)

    assert connection.fetch_query == ""

    kwargs = connection.fetch_kwargs
    assert kwargs == {}

    assert deleted is False


async def test_rest_repository_count(alpha_rest_repository):
    connection = alpha_rest_repository.connector.connection
    connection.fetch_result = [{'Total-Count': 5}]

    count = await alpha_rest_repository.count()

    assert count == 5
    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'HEAD'


async def test_rest_repository_count_specific_header(alpha_rest_repository):
    connection = alpha_rest_repository.connector.connection
    connection.fetch_result = [{'Count': 5}]

    alpha_rest_repository.settings['count_header'] = 'Count'
    count = await alpha_rest_repository.count()

    assert count == 5
    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'HEAD'


async def test_rest_repository_count_domain(alpha_rest_repository):
    connection = alpha_rest_repository.connector.connection
    connection.fetch_result = [{'Total-Count': 1}]

    domain = [('field_1', '=', "value_3")]
    count = await alpha_rest_repository.count(domain)

    assert count == 1
    assert connection.fetch_query == (
        "https://service.example.com/alphas")

    kwargs = connection.fetch_kwargs
    assert kwargs['method'] == 'HEAD'
    assert kwargs['query_params'] == (
        {"filter": '[["field_1", "=", "value_3"]]'})
