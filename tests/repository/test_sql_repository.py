import json
from asyncio import sleep
from inspect import cleandoc
from typing import Callable, List, Tuple, Dict, Mapping, Any
from pytest import fixture, mark, raises
from modelark.common import Entity, Domain
from modelark.connector import Connector, Connection
from modelark.repository import Repository, SqlRepository


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


def test_sql_repository_implementation() -> None:
    assert issubclass(SqlRepository, Repository)


@fixture
def mock_connector():
    class MockConnection:
        def __init__(self) -> None:
            self.execute_query = ''
            self.execute_args: Tuple = ()
            self.execute_result = ''
            self.fetch_query = ''
            self.fetch_args: Tuple = ()
            self.fetch_result: List[Any] = []
            self.data: Dict = {}

        async def execute(self, query: str, *args) -> str:
            self.execute_query = query
            self.execute_args = args
            return self.execute_result

        async def fetch(self, query: str, *args) -> List[Any]:
            self.fetch_query = query
            self.fetch_args = args
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
def alpha_sql_repository(mock_connector) -> SqlRepository[Alpha]:
    class AlphaSqlRepository(SqlRepository[Alpha]):
        model = Alpha

    repository = AlphaSqlRepository(
        'alphas', Alpha, mock_connector
    )
    mock_connector.load({
        "default": {
            "1": Alpha(id='1', field_1='value_1'),
            "2": Alpha(id='2', field_1='value_2'),
            "3": Alpha(id='3', field_1='value_3')
        }
    })
    return repository


@fixture
def beta_sql_repository(mock_connector) -> SqlRepository[Beta]:
    class BetaSqlRepository(SqlRepository[Beta]):
        model = Beta

    repository = BetaSqlRepository(
        'betas', Beta, mock_connector
    )
    mock_connector.load({
        "default": {
            "1": Beta(id='1', alpha_id='1'),
            "2": Beta(id='2', alpha_id='1'),
            "3": Beta(id='3', alpha_id='2')
        }
    })
    return repository


@fixture
def gamma_sql_repository(mock_connector) -> SqlRepository[Gamma]:
    class GammaSqlRepository(SqlRepository[Gamma]):
        model = Gamma

    repository = GammaSqlRepository(
        'gammas', Gamma, mock_connector
    )
    mock_connector.load({
        "default": {
            "1": Gamma(id='1'),
            "2": Gamma(id='2'),
            "3": Gamma(id='3')
        }
    })
    return repository


@fixture
def delta_sql_repository(mock_connector) -> SqlRepository[Delta]:
    class DeltaSqlRepository(SqlRepository[Delta]):
        model = Delta

    repository = DeltaSqlRepository(
        'deltas', Delta, mock_connector
    )
    mock_connector.load({
        "default": {
            "1": Delta(id='1', alpha_id='1', gamma_id='1'),
            "2": Delta(id='2', alpha_id='1', gamma_id='2'),
            "3": Delta(id='3', alpha_id='2', gamma_id='3'),
            "4": Delta(id='3', alpha_id='3', gamma_id='3')
        }
    })
    return repository


def test_sql_repository_model(alpha_sql_repository) -> None:
    assert alpha_sql_repository.model is Alpha


async def test_sql_repository_search_all(alpha_sql_repository):
    items = await alpha_sql_repository.search([])

    connection = alpha_sql_repository.connector.connection

    assert cleandoc(connection.fetch_query) == cleandoc(
        """
        SELECT data
        FROM public.alphas
        WHERE 1 = 1

        ORDER BY data->>'created_at' DESC NULLS LAST
        """)
    args = connection.fetch_args
    assert args == ()


async def test_sql_repository_search_limit(alpha_sql_repository):
    items = await alpha_sql_repository.search([], limit=2)

    connection = alpha_sql_repository.connector.connection

    assert cleandoc(connection.fetch_query) == cleandoc(
        """
        SELECT data
        FROM public.alphas
        WHERE 1 = 1

        ORDER BY data->>'created_at' DESC NULLS LAST
        LIMIT 2
        """)
    args = connection.fetch_args
    assert args == ()


async def test_sql_repository_search_limit_none(alpha_sql_repository):
    items = await alpha_sql_repository.search([], limit=None, offset=None)

    connection = alpha_sql_repository.connector.connection

    assert cleandoc(connection.fetch_query) == cleandoc(
        """
        SELECT data
        FROM public.alphas
        WHERE 1 = 1

        ORDER BY data->>'created_at' DESC NULLS LAST
        """)
    args = connection.fetch_args
    assert args == ()


async def test_sql_repository_search_offset(alpha_sql_repository):
    items = await alpha_sql_repository.search([], offset=2)

    connection = alpha_sql_repository.connector.connection

    assert cleandoc(connection.fetch_query) == cleandoc(
        """
        SELECT data
        FROM public.alphas
        WHERE 1 = 1

        ORDER BY data->>'created_at' DESC NULLS LAST

        OFFSET 2
        """)


async def test_sql_repository_add(alpha_sql_repository) -> None:
    item = Alpha(id="4", field_1="value_1")

    await alpha_sql_repository.add(item)

    connection = alpha_sql_repository.connector.connection

    assert cleandoc(connection.fetch_query) == cleandoc(
        """
        INSERT INTO public.alphas(data) (
            SELECT *
            FROM unnest($1::public.alphas[]) AS d
        )
        ON CONFLICT ((data->>'id'))
        DO UPDATE
            SET data = public.alphas.data ||
            EXCLUDED.data - 'created_at' - 'created_by'
        RETURNING *;
        """)
    args = connection.fetch_args
    assert isinstance(args, tuple)
    assert json.loads(args[0][0][0])['id'] == '4'
    assert json.loads(args[0][0][0])['field_1'] == 'value_1'


# async def test_sql_repository_add_update(alpha_sql_repository) -> None:
    # created_entity = Alpha(id="1", field_1="value_1")
    # created_entity, *_ = await alpha_sql_repository.add(created_entity)

    # await sleep(1)

    # updated_entity = Alpha(id="1", field_1="New Value")
    # updated_entity, *_ = await alpha_sql_repository.add(updated_entity)

    # assert created_entity.created_at == updated_entity.created_at


# async def test_sql_repository_add_no_id(alpha_sql_repository) -> None:
    # item = Alpha(field_1="value_1")

    # await alpha_sql_repository.add(item)


async def test_sql_repository_add_multiple(alpha_sql_repository):
    items = [
        Alpha(id='1', field_1="value_1"),
        Alpha(id='2', field_1="value_2")
    ]

    connection = alpha_sql_repository.connector.connection

    returned_items = await alpha_sql_repository.add(items)

    assert cleandoc(connection.fetch_query) == cleandoc(
        """
        INSERT INTO public.alphas(data) (
            SELECT *
            FROM unnest($1::public.alphas[]) AS d
        )
        ON CONFLICT ((data->>'id'))
        DO UPDATE
            SET data = public.alphas.data ||
            EXCLUDED.data - 'created_at' - 'created_by'
        RETURNING *;
        """)
    args = connection.fetch_args
    assert isinstance(args, tuple)
    assert json.loads(args[0][0][0])['id'] == '1'
    assert json.loads(args[0][0][0])['field_1'] == 'value_1'
    assert json.loads(args[0][1][0])['id'] == '2'
    assert json.loads(args[0][1][0])['field_1'] == 'value_2'


async def test_sql_repository_search_join_one_to_many(
        alpha_sql_repository, beta_sql_repository):
    alpha_sql_repository.connector.connection.fetch_result = [
        {'data': '{"id": "1", "field_1": "value_1"}',
         'array_agg': ['{"id": "1", "alpha_id": "1"}',
                       '{"id": "2", "alpha_id": "1"}']}
    ]

    connection = alpha_sql_repository.connector.connection

    for parent, children in await alpha_sql_repository.search(
            [('id', '=', '1')], join=beta_sql_repository):
        assert isinstance(parent, Alpha)
        assert len(children) == 2
        assert all(isinstance(beta, Beta) for beta in children)

    assert cleandoc(connection.fetch_query) == cleandoc("""\
        SELECT alphas.data, array_agg(betas.data)
        FROM public.alphas LEFT JOIN public.betas
        ON betas.data->>'alpha_id' = alphas.data->>'id'

        WHERE id = ANY(ARRAY['1'])
        GROUP BY alphas.data
        ORDER BY data->>'created_at' DESC NULLS LAST""")

# async def test_sql_repository_search_join_many_to_one(
    # alpha_sql_repository, beta_sql_repository):

    # for element, siblings in await beta_sql_repository.search(
    # [('id', '=', '1')], join=alpha_sql_repository,
    # link=beta_sql_repository):


# async def test_sql_repository_search_join_many_to_many(
    # alpha_sql_repository, gamma_sql_repository,
    # delta_sql_repository):

    # for alpha, gammas in await alpha_sql_repository.search(
    # [('id', '=', '1')], join=gamma_sql_repository,
    # link=delta_sql_repository):

    # assert isinstance(alpha, Alpha)
    # assert len(gammas) == 2
    # assert gammas[0].id == '1'
    # assert gammas[1].id == '2'


async def test_sql_repository_remove_true(alpha_sql_repository):
    item = Alpha(id="5", field_1="value_5")

    connection = alpha_sql_repository.connector.connection
    connection.execute_result = 'DELETE 1'

    deleted = await alpha_sql_repository.remove(item)

    assert deleted is True
    assert cleandoc(connection.execute_query) == cleandoc(
        """
        DELETE FROM public.alphas
        WHERE (data->>'id') IN ($1)
        """)
    args = connection.execute_args
    assert args == ('5',)


async def test_sql_repository_remove_false(alpha_sql_repository):
    item = Alpha(**{'id': '6', 'field_1': 'MISSING'})

    connection = alpha_sql_repository.connector.connection
    connection.execute_result = 'DELETE 0'

    deleted = await alpha_sql_repository.remove(item)

    assert deleted is False
    assert cleandoc(connection.execute_query) == cleandoc(
        """
        DELETE FROM public.alphas
        WHERE (data->>'id') IN ($1)
        """)
    args = connection.execute_args
    assert args == ('6',)


async def test_sql_repository_remove_empty(alpha_sql_repository):
    deleted = await alpha_sql_repository.remove([])
    assert deleted is False


async def test_sql_repository_count(alpha_sql_repository):
    connection = alpha_sql_repository.connector.connection
    connection.fetch_result = [{'count': 5}]

    count = await alpha_sql_repository.count()

    assert count == 5
    assert cleandoc(connection.fetch_query) == cleandoc(
        """
        SELECT count(*) as count
        FROM public.alphas
        WHERE 1 = 1
        """)
    args = connection.fetch_args
    assert args == ()


async def test_sql_repository_count_domain(alpha_sql_repository):
    connection = alpha_sql_repository.connector.connection
    connection.fetch_result = [{'count': 1}]
    domain = [('field_1', '=', "value_3")]

    count = await alpha_sql_repository.count(domain)

    assert count == 1
    assert cleandoc(connection.fetch_query) == cleandoc(
        """
        SELECT count(*) as count
        FROM public.alphas
        WHERE field_1 = ANY(ARRAY['value_3'])
        """)
    args = connection.fetch_args
    assert args == ("value_3",)
