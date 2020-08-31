import time
from pathlib import Path
from collections import defaultdict
from json import loads, load, dump
from uuid import uuid4
from typing import Dict, List, Tuple, Any, Callable, Generic, Union, overload
from ..common import (
    T, R, L, Domain, Filterer, DefaultFilterer,
    Locator, DefaultLocator, Editor, DefaultEditor)
from .repository import Repository


class JsonRepository(Repository, Generic[T]):
    def __init__(self,
                 data_path: str,
                 collection: str,
                 item_class: Callable[..., T],
                 filterer: Filterer = None,
                 locator: Locator = None,
                 editor: Editor = None) -> None:
        self.data_path = data_path
        self.collection = collection
        self.item_class: Callable[..., T] = item_class
        self.filterer = filterer or DefaultFilterer()
        self.locator = locator or DefaultLocator()
        self.editor = editor or DefaultEditor()

    async def add(self, item: Union[T, List[T]]) -> List[T]:

        items = item if isinstance(item, list) else [item]

        data: Dict[str, Any] = defaultdict(lambda: {})
        if self.file_path.exists():
            data.update(loads(self.file_path.read_text()))

        for item in items:
            item.id = item.id or str(uuid4())
            item.updated_at = int(time.time())
            item.updated_by = self.editor.reference
            item.created_at = item.created_at or item.updated_at
            item.created_by = item.created_by or item.updated_by

            data[self.collection][item.id] = vars(item)

        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        with self.file_path.open('w') as f:
            dump(data, f, indent=2)

        return items

    async def remove(self, item: Union[T, List[T]]) -> bool:

        items = item if isinstance(item, list) else [item]
        if not self.file_path.exists():
            return False

        with self.file_path.open('r') as f:
            data = load(f)

        deleted = False
        for item in items:
            deleted_item = data[self.collection].pop(item.id, None)
            deleted = bool(deleted_item) or deleted

        with self.file_path.open('w') as f:
            dump(data, f, indent=2)

        return deleted

    async def count(self, domain: Domain = None) -> int:
        if not self.file_path.exists():
            return 0

        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        with self.file_path.open('r') as f:
            data = load(f)

        count = 0
        domain = domain or []
        filter_function = self.filterer.parse(domain)
        for item_dict in list(data[self.collection].values()):
            item = self.item_class(**item_dict)
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
        if not self.file_path.exists():
            return items

        with self.file_path.open('r') as f:
            data = load(f)
            items_dict = data.get(self.collection, {})

        filter_function = self.filterer.parse(domain)
        for item_dict in items_dict.values():
            item = self.item_class(**item_dict)

            if filter_function(item):
                items.append(item)

        if offset is not None:
            items = items[offset:]
        if limit is not None:
            items = items[:limit]

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

    @property
    def file_path(self) -> Path:
        return (Path(self.data_path) / self.locator.zone /
                self.locator.location / f"{self.collection}.json")
