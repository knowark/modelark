import time
from pathlib import Path
from collections import defaultdict
from json import loads, load, dump
from uuid import uuid4
from typing import Dict, List, Tuple, Any, Callable, Generic, Union, cast
from ..common import (
    T, R, L, Locator, DefaultLocator, Editor, DefaultEditor)
from ..filterer import Filterer, FunctionParser, Domain
from .repository import Repository


class JsonRepository(Repository, Generic[T]):
    def __init__(self,
                 data_path: str,
                 collection: str,
                 constructor: Callable[..., T],
                 filterer: Filterer = None,
                 locator: Locator = None,
                 editor: Editor = None) -> None:
        self.data_path = data_path
        self.collection = collection
        self.constructor: Callable[..., T] = constructor
        self.filterer = filterer or FunctionParser()
        self.locator = locator or DefaultLocator()
        self.editor = editor or DefaultEditor()

    async def add(self, item: Union[T, List[T]]) -> List[T]:

        items = item if isinstance(item, list) else [item]

        data: Dict[str, Any] = defaultdict(lambda: {})
        if self.file_path.exists():
            data.update(loads(self.file_path.read_text()))

        for item in items:
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
            item = self.constructor(**item_dict)
            if filter_function(item):
                count += 1
        return count

    async def search(self, domain: Domain,
                     limit: int = None, offset: int = None,
                     order: str = None) -> List[T]:
        items: List[T] = []
        if not self.file_path.exists():
            return items

        with self.file_path.open('r') as f:
            data = load(f)
            items_dict = data.get(self.collection, {})

        filter_function = self.filterer.parse(domain)
        for item_dict in items_dict.values():
            item = self.constructor(**item_dict)

            if filter_function(item):
                items.append(item)

        if offset is not None:
            items = items[offset:]
        if limit is not None:
            items = items[:limit]
        if order:
            fields = order.lower().split(',')
            for field in reversed(fields):
                key, *direction = field.split()
                items = cast(List[T], sorted(
                    items, key=lambda item: getattr(item, key),
                    reverse=('desc' in direction)))

        return items

    @property
    def file_path(self) -> Path:
        return (Path(self.data_path) / self.locator.zone /
                self.locator.location / f"{self.collection}.json")
