import time
import json
from typing import cast, Dict, Any
from uuid import uuid4
from typing import (
    List, Type, Tuple, Mapping, Generic, Callable, Union, overload)
from ..common import (
    T, R, L, Domain, Conditioner, DefaultConditioner,
    Locator, DefaultLocator, Editor, DefaultEditor)
from ..connector import Connector
from .repository import Repository


class RestRepository(Repository, Generic[T]):
    def __init__(
        self,
        endpoint: str,
        connector: Connector,
        constructor: Callable = None,
        settings: Dict[str, str] = None
    ) -> None:
        self.endpoint = endpoint
        self.connector = connector
        self.constructor = constructor
        self.settings = settings or {}

    async def add(self, item: Union[T, List[T]]) -> List[T]:
        items = item if isinstance(item, list) else [item]

        connection = await self.connector.get()
        add_method = self.settings.get('add_method', 'PUT')
        parameters = {'method': add_method, 'payload': [
            vars(item) for item in items]}

        records = await connection.fetch(self.endpoint, **parameters)

        if self.constructor:
            records = [self.constructor(**record) for record in records]

        return cast(List[T], records)

    async def search(self, domain: Domain,
                     limit: int = None, offset: int = None,
                     order: str = None) -> List[T]:

        filter = json.dumps(domain)

        connection = await self.connector.get()
        parameters: Dict[str, Any] = {'method': 'GET'}
        query_params: Dict[str, str] = {}

        if domain:
            domain_param = self.settings.get('domain_param', 'filter')
            query_params[domain_param] = json.dumps(domain)
        if limit:
            query_params['limit'] = str(limit)
        if offset:
            query_params['offset'] = str(offset)
        if order:
            query_params['order'] = str(order)

        if query_params:
            parameters['query_params'] = query_params

        records = await connection.fetch(self.endpoint, **parameters)

        if self.constructor:
            records = [self.constructor(**record) for record in records]

        return cast(List[T], records)

    async def remove(self, item: Union[T, List[T]]) -> bool:
        if not item:
            return False

        items = item if isinstance(item, list) else [item]
        ids = [getattr(item, 'id') for item in items]

        connection = await self.connector.get()

        parameters: Dict[str, Any] = {'method': 'DELETE'}
        if len(items) == 1:
            parameters['path'] = f'/{ids[0]}'
        else:
            parameters['payload'] = ids

        records = await connection.fetch(self.endpoint, **parameters)

        return True

    async def count(self, domain: Domain = None) -> int:
        connection = await self.connector.get()
        parameters: Dict[str, Any] = {'method': 'HEAD'}
        if domain:
            domain_param = self.settings.get('domain_param', 'filter')
            parameters['query_params'] = {
                domain_param: json.dumps(domain)
            }

        result: Mapping = next(iter(
            await connection.fetch(self.endpoint, **parameters)), {})

        count_header = self.settings.get('count_header', 'Total-Count')

        return result.get(count_header, 0)
