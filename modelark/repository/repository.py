from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Tuple, Type, List, Generic, Union, Optional, overload
from ..common import T, R, L
from ..filterer import Domain


class Repository(ABC, Generic[T]):
    @property
    def model(self) -> Type[T]:
        raise NotImplementedError('Provide the repository model')

    @abstractmethod
    async def add(self, item: Union[T, List[T]]) -> List[T]:
        "Add method to be implemented."

    @abstractmethod
    async def remove(self, item: Union[T, List[T]]) -> bool:
        "Remove method to be implemented."

    @abstractmethod
    async def count(self, domain: Domain = None) -> int:
        "Count items matching a query domain"

    @abstractmethod
    async def search(self, domain: Domain,
                     limit: int = None, offset: int = None,
                     order: str = None) -> List[T]:
        """Standard search method"""

    async def join(
            self, domain: Domain,
            join: 'Repository[R]',
            link: 'Repository[L]' = None,
            source: str = None,
            target: str = None) -> List[Tuple[T, List[R]]]:
        """Standard joining method"""

        items = await self.search(domain)

        reference = (link == self) and join or self
        source = source or f'{reference.model.__name__.lower()}_id'
        pivot = link not in (self, join) and link

        field, key = source, 'id'
        if reference is join:
            field, key = key, source

        entries: Union[List[T], List[L]] = items
        if pivot and link:
            entries = await link.search([
                (field, 'in', [getattr(entry, key) for entry in entries])])
            target = target or f'{join.model.__name__.lower()}_id'
            field, key = 'id', target

        record_map = defaultdict(list)
        for record in await join.search([
                (field, 'in', [getattr(entry, key) for entry in entries])]):
            record_map[getattr(record, field)].append(record)

        relation_map = record_map
        if pivot:
            relation_map = defaultdict(list)
            for entry in entries:
                relation_map[getattr(entry, source)].extend(
                    record_map[getattr(entry, key)])
            field, key = source, 'id'

        return [(item, relation_map[getattr(item, key)]) for item in items]
