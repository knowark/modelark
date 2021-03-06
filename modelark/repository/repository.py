import contextvars
from collections import defaultdict
from typing import Tuple, Type, List, Generic, Union
from ..filterer import Domain
from .interface import RepositoryInterface, T, R, L


class Repository(RepositoryInterface, Generic[T]):
    def __init_subclass__(cls, **kwargs) -> None:
        setattr(cls, 'context', contextvars.ContextVar(
            f'{cls.__name__}Context', default={}))

    @property
    def model(self) -> Type[T]:
        raise NotImplementedError('Provide the repository model')

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
