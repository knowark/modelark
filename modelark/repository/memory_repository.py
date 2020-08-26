import time
from uuid import uuid4
from collections import defaultdict
from typing import List, Tuple, Dict, Generic, Union, Any, overload
from ..common import (
    T, R, L, Domain, Locator, DefaultLocator, Filterer, DefaultFilterer)
from .repository import Repository


class MemoryRepository(Repository, Generic[T]):

    def __init__(self, locator: Locator = None,
                 filterer: Filterer = None) -> None:
        self.data: Dict[str, Dict[str, T]] = defaultdict(dict)
        self.filterer: Filterer = filterer or DefaultFilterer()
        self.locator: Locator = locator or DefaultLocator()
        self.max_items = 10_000

    async def add(self, item: Union[T, List[T]]) -> List[T]:
        items = item if isinstance(item, list) else [item]
        for item in items:
            item.id = item.id or str(uuid4())
            item.updated_at = int(time.time())
            existing_item = self.data[self._location].get(item.id)
            if existing_item:
                item.created_at = existing_item.created_at
            else:
                item.created_at = item.updated_at

            self.data[self._location][item.id] = item
        return items

    async def remove(self, item: Union[T, List[T]]) -> bool:
        items = item if isinstance(item, list) else [item]
        deleted = False
        for item in items:
            if item.id not in self.data[self._location]:
                continue
            del self.data[self._location][item.id]
            deleted = True

        return deleted

    async def count(self, domain: Domain = None) -> int:
        count = 0
        domain = domain or []
        filter_function = self.filterer.parse(domain)
        for item in list(self.data[self._location].values()):
            if filter_function(item):
                count += 1
        return count

    @overload
    async def search(self, domain: Domain,
                     limit: int = None, offset: int = None) -> List[T]:
        """Standard search method"""

    @overload
    async def search(self, domain: Domain,
                     limit: int = None, offset: int = None,
                     *,
                     join: 'Repository[R]',
                     link: 'Repository[L]' = None,
                     source: str = None,
                     target: str = None) -> List[Tuple[T, List[R]]]:
        """Joining search method"""

    async def search(
            self, domain: Domain,
            limit: int = None,
            offset: int = None,
            *,
            join: 'Repository[R]' = None,
            link: 'Repository[L]' = None,
            source: str = None,
            target: str = None) -> Union[List[T], List[Tuple[T, List[R]]]]:

        items: List[T] = []
        filter_function = self.filterer.parse(domain)
        for item in list(self.data[self._location].values()):
            if filter_function(item):
                items.append(item)

        if offset is not None:
            items = items[offset:]

        if limit is not None:
            items = items[:min(limit, self.max_items)]

        if not join:
            return items

        reference = (link == self) and join or self
        source = source or f'{reference.model.__name__.lower()}_id'
        pivot = link and link not in (self, join)

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

    def load(self, data: Dict[str, Dict[str, T]]) -> None:
        self.data = data

    @property
    def _location(self) -> str:
        return self.locator.location
