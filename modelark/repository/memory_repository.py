import time
from uuid import uuid4
from collections import defaultdict
from typing import List, Tuple, Dict, Generic, Union, Any, overload
from ..common import (
    T, R, L, Domain, Locator, DefaultLocator,
    Filterer, DefaultFilterer, Editor, DefaultEditor)
from .repository import Repository


class MemoryRepository(Repository, Generic[T]):
    def __init__(self, filterer: Filterer = None,
                 locator: Locator = None,
                 editor: Editor = None) -> None:
        self.data: Dict[str, Dict[str, T]] = defaultdict(dict)
        self.filterer: Filterer = filterer or DefaultFilterer()
        self.locator: Locator = locator or DefaultLocator()
        self.editor: Editor = editor or DefaultEditor()
        self.max_items = 10_000

    async def add(self, item: Union[T, List[T]]) -> List[T]:
        items = item if isinstance(item, list) else [item]

        for item in items:
            item.id = item.id or str(uuid4())
            item.updated_at = int(time.time())
            item.updated_by = self.editor.reference
            item.created_at = item.created_at or item.updated_at
            item.created_by = item.created_by or item.updated_by
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

    async def search(self, domain: Domain,
                     limit: int = None, offset: int = None) -> List[T]:
        items: List[T] = []
        filter_function = self.filterer.parse(domain)
        for item in list(self.data.setdefault(self._location, {}).values()):
            if filter_function(item):
                items.append(item)

        if offset is not None:
            items = items[offset:]

        if limit is not None:
            items = items[:min(limit, self.max_items)]

        return items

    def load(self, data: Dict[str, Dict[str, T]]) -> None:
        self.data.update(data)

    @property
    def _location(self) -> str:
        zone = self.locator.zone
        location = self.locator.location
        return ":".join(zone and [zone, location] or [location])
