from json import dumps, loads
from pytest import fixture, mark
from modelark.common import Entity, DefaultLocator
from modelark.repository import Repository, JsonRepository


pytestmark = mark.asyncio


class Alpha(Entity):
    def __init__(self, **attributes) -> None:
        super().__init__(**attributes)
        self.field_1 = attributes.get('field_1', '')


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


def test_json_repository_implementation() -> None:
    assert issubclass(JsonRepository, Repository)


@fixture
def alpha_json_repository(tmp_path) -> JsonRepository[Alpha]:
    item_dict = {
        "1": vars(Alpha(id='1', field_1='value_1')),
        "2": vars(Alpha(id='2', field_1='value_2')),
        "3": vars(Alpha(id='3', field_1='value_3'))
    }

    tenant_directory = tmp_path / "origin"
    tenant_directory.mkdir(parents=True, exist_ok=True)
    collection = 'alphas'

    file_path = str(tenant_directory / f'{collection}.json')

    with open(file_path, 'w') as f:
        data = dumps({collection: item_dict})
        f.write(data)

    class AlphaJsonRepository(JsonRepository[Alpha]):
        model = Alpha

    json_repository = AlphaJsonRepository(
        data_path=str(tmp_path),
        collection=collection,
        item_class=Alpha,
        locator=DefaultLocator('origin')
    )

    return json_repository


@fixture
def beta_json_repository(tmp_path) -> JsonRepository[Beta]:
    item_dict = {
        "1": vars(Beta(id='1', alpha_id='1')),
        "2": vars(Beta(id='2', alpha_id='1')),
        "3": vars(Beta(id='3', alpha_id='2'))
    }

    tenant_directory = tmp_path / "origin"
    tenant_directory.mkdir(parents=True, exist_ok=True)
    collection = 'betas'

    file_path = str(tenant_directory / f'{collection}.json')

    with open(file_path, 'w') as f:
        data = dumps({collection: item_dict})
        f.write(data)

    class BetaJsonRepository(JsonRepository[Beta]):
        model = Beta

    json_repository = BetaJsonRepository(
        data_path=str(tmp_path),
        collection=collection,
        item_class=Beta,
        locator=DefaultLocator('origin')
    )

    return json_repository


@fixture
def gamma_json_repository(tmp_path) -> JsonRepository[Gamma]:
    item_dict = {
        "1": vars(Gamma(id='1')),
        "2": vars(Gamma(id='2')),
        "3": vars(Gamma(id='3'))
    }

    tenant_directory = tmp_path / "origin"
    tenant_directory.mkdir(parents=True, exist_ok=True)
    collection = 'gammas'

    file_path = str(tenant_directory / f'{collection}.json')

    with open(file_path, 'w') as f:
        data = dumps({collection: item_dict})
        f.write(data)

    class GammaJsonRepository(JsonRepository[Gamma]):
        model = Gamma

    json_repository = GammaJsonRepository(
        data_path=str(tmp_path),
        collection=collection,
        item_class=Gamma,
        locator=DefaultLocator('origin')
    )

    return json_repository


@fixture
def delta_json_repository(tmp_path) -> JsonRepository[Delta]:
    item_dict = {
        "1": vars(Delta(id='1', alpha_id='1', gamma_id='1')),
        "2": vars(Delta(id='2', alpha_id='1', gamma_id='2')),
        "3": vars(Delta(id='3', alpha_id='2', gamma_id='3')),
        "4": vars(Delta(id='3', alpha_id='3', gamma_id='3'))
    }

    tenant_directory = tmp_path / "origin"
    tenant_directory.mkdir(parents=True, exist_ok=True)
    collection = 'deltas'

    file_path = str(tenant_directory / f'{collection}.json')

    with open(file_path, 'w') as f:
        data = dumps({collection: item_dict})
        f.write(data)

    class DeltaJsonRepository(JsonRepository[Delta]):
        model = Delta

    json_repository = DeltaJsonRepository(
        data_path=str(tmp_path),
        collection=collection,
        item_class=Delta,
        locator=DefaultLocator('origin')
    )

    return json_repository


async def test_json_repository_add(alpha_json_repository):
    item = Alpha(id='5', field_1='value_5')

    await alpha_json_repository.add(item)

    file_path = alpha_json_repository.file_path
    with open(file_path) as f:
        data = loads(f.read())
    items = data.get("alphas")

    item_dict = items.get('5')

    assert item_dict.get('field_1') == item.field_1


async def test_json_repository_add_no_id(alpha_json_repository) -> None:
    item = Alpha(field_1='value_5')
    item = await alpha_json_repository.add(item)

    file_path = alpha_json_repository.file_path
    with open(file_path) as f:
        data = loads(f.read())
    items = data.get("alphas")
    for key in items:
        assert len(key) > 0


async def test_json_repository_add_update(alpha_json_repository) -> None:
    update = Alpha(id='1', field_1='New Value')

    await alpha_json_repository.add(update)

    file_path = alpha_json_repository.file_path
    with open(file_path) as f:
        data = loads(f.read())
    items = data.get("alphas")

    assert len(items) == 3
    assert "New Value" in items['1']['field_1']


async def test_json_repository_add_non_existent_file(
        tmp_path, alpha_json_repository):
    alpha_json_repository.data_path = str(tmp_path / '.non_existent_file')
    item = Alpha(**{'id': '6', 'field_1': 'value_6'})
    items = await alpha_json_repository.add(item)
    assert len(items) == 1


async def test_json_repository_search(alpha_json_repository):
    domain = [('field_1', '=', "value_3")]
    items = await alpha_json_repository.search(domain)

    assert len(items) == 1
    for item in items:
        assert item.id == '3'
    assert item.field_1 == "value_3"


async def test_json_repository_search_all(alpha_json_repository):
    items = await alpha_json_repository.search([])
    assert len(items) == 3


async def test_json_repository_search_limit(alpha_json_repository):
    domain = []
    items = await alpha_json_repository.search(domain, limit=2)
    assert len(items) == 2


async def test_json_repository_search_limit_zero(alpha_json_repository):
    items = await alpha_json_repository.search([], limit=0)
    assert len(items) == 0


async def test_json_repository_search_offset(alpha_json_repository):
    domain = []
    items = await alpha_json_repository.search(domain, offset=2)

    assert len(items) == 1


async def test_json_repository_search_limit_and_offset_none(
        alpha_json_repository):
    items = await alpha_json_repository.search([], limit=None, offset=None)
    assert len(items) == 3


async def test_json_repository_search_non_existent_file(
        tmp_path, alpha_json_repository):
    alpha_json_repository.data_path = str(tmp_path / '.non_existent_file')
    items = await alpha_json_repository.search([])
    assert len(items) == 0


async def test_json_repository_search_join_one_to_many(
        alpha_json_repository, beta_json_repository):

    for parent, children in await alpha_json_repository.search(
            [('id', '=', '1')], join=beta_json_repository):

        assert isinstance(parent, Alpha)
        assert all(isinstance(beta, Beta) for beta in children)
        assert len(children) == 2


async def test_json_repository_search_join_many_to_one(
        alpha_json_repository, beta_json_repository):

    for element, siblings in await beta_json_repository.search(
        [('id', '=', '1')], join=alpha_json_repository,
            link=beta_json_repository):

        assert isinstance(element, Beta)
        assert len(siblings) == 1
        assert isinstance(next(iter(siblings)), Alpha)


async def test_json_repository_search_join_many_to_many(
        alpha_json_repository, gamma_json_repository,
        delta_json_repository):

    for alpha, gammas in await alpha_json_repository.search(
        [('id', '=', '1')], join=gamma_json_repository,
            link=delta_json_repository):

        assert isinstance(alpha, Alpha)
        assert len(gammas) == 2
        assert gammas[0].id == '1'
        assert gammas[1].id == '2'


async def test_json_repository_remove(alpha_json_repository):
    file_path = alpha_json_repository.file_path

    with open(file_path) as f:
        data = loads(f.read())
    items_dict = data.get("alphas")
    item_dict = items_dict.get('2')

    assert len(items_dict) == 3

    item = Alpha(**item_dict)
    deleted = await alpha_json_repository.remove(item)

    with open(file_path) as f:
        data = loads(f.read())
    items_dict = data.get("alphas")

    assert deleted is True
    assert len(items_dict) == 2
    assert "2" not in items_dict.keys()


async def test_json_repository_remove_false(alpha_json_repository):
    file_path = alpha_json_repository.file_path

    item = Alpha(**{'id': '5', 'field_1': 'MISSING'})
    deleted = await alpha_json_repository.remove(item)

    with open(file_path) as f:
        data = loads(f.read())
    items_dict = data.get("alphas")

    assert deleted is False
    assert len(items_dict) == 3


async def test_json_repository_remove_non_existent_file(
        tmp_path, alpha_json_repository):
    alpha_json_repository.data_path = str(tmp_path / '.non_existent_remove')
    item = Alpha(**{'id': '6', 'field_1': 'MISSING'})
    deleted = await alpha_json_repository.remove(item)
    assert deleted is False


async def test_json_repository_count(alpha_json_repository):
    count = await alpha_json_repository.count()
    assert count == 3


async def test_json_repository_count(alpha_json_repository):
    alpha_json_repository.data_path = '/tmp/.non_existent_count'
    count = await alpha_json_repository.count()
    assert count == 0


async def test_json_repository_count_domain(alpha_json_repository):
    domain = [('id', '=', "1")]
    count = await alpha_json_repository.count(domain)
    assert count == 1
