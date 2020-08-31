import time
import json
from uuid import uuid4
from typing import List, Tuple, Mapping, Generic, Callable, Union, overload
from ..common import (
    T, R, L, Domain, Conditioner, DefaultConditioner,
    Locator, DefaultLocator, Editor, DefaultEditor)
from ..connector import Connector
from .repository import Repository


class SqlRepository(Repository, Generic[T]):
    def __init__(self,
                 table: str,
                 constructor: Callable,
                 connector: Connector,
                 conditioner: Conditioner = None,
                 locator: Locator = None,
                 editor: Editor = None) -> None:
        self.table = table
        self.constructor = constructor
        self.connector = connector
        self.conditioner = conditioner or DefaultConditioner()
        self.locator = locator or DefaultLocator('public')
        self.editor = editor or DefaultEditor()
        self.max_items = 10_000
        self.jsonb_field = 'data'

    async def add(self, item: Union[T, List[T]]) -> List[T]:
        records = []
        items = item if isinstance(item, list) else [item]
        for item in items:
            item.id = item.id or str(uuid4())
            item.updated_at = int(time.time())
            item.updated_by = self.editor.reference
            item.created_at = item.created_at or item.updated_at
            item.created_by = item.created_by or item.updated_by
            records.append((json.dumps(vars(item)),))

        namespace = f"{self.locator.location}.{self.table}"
        query = f"""
            INSERT INTO {namespace}({self.jsonb_field}) (
                SELECT *
                FROM unnest($1::{namespace}[]) AS d
            )
            ON CONFLICT (({self.jsonb_field}->>'id'))
            DO UPDATE
                SET {self.jsonb_field} = {namespace}.{self.jsonb_field} ||
                EXCLUDED.{self.jsonb_field} - 'created_at' - 'created_by'
            RETURNING *;
        """
        connection = await self.connector.get(self.locator.zone)
        rows = await connection.fetch(query, records)

        return [self.constructor(**json.loads(row[self.jsonb_field]))
                for row in rows if self.jsonb_field in row]

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

        condition, parameters = self.conditioner.parse(domain)

        query = f"""
        SELECT {self.jsonb_field}
        FROM {self.locator.location}.{self.table}
        WHERE {condition}
        {self._order_by()}
        """

        if limit is not None:
            query = "".join([query, f"LIMIT {limit}"])
        if offset is not None:
            query = "".join([query, f"OFFSET {offset}"])

        connection = await self.connector.get(self.locator.zone)
        result = await connection.fetch(query, *parameters)

        return [self.constructor(**json.loads(row[self.jsonb_field]))
                for row in result if self.jsonb_field in row]

    async def remove(self, item: Union[T, List[T]]) -> bool:
        if not item:
            return False
        items = item if isinstance(item, list) else [item]
        ids = [item.id for item in items]
        placeholders = ", ".join(f'${i + 1}' for i in range(len(ids)))

        query = f"""
            DELETE FROM {self.locator.location}.{self.table}
            WHERE ({self.jsonb_field}->>'id') IN ({placeholders})
        """

        connection = await self.connector.get(self.locator.zone)
        result = await connection.execute(query, *ids)

        return bool(int(result.replace('DELETE', '') or 0))

    async def count(self, domain: Domain = None) -> int:
        condition, parameters = self.conditioner.parse(domain or [])

        query = f"""
            SELECT count(*) as count
            FROM {self.locator.location}.{self.table}
            WHERE {condition}
        """

        connection = await self.connector.get(self.locator.zone)
        result: Mapping[str, int] = next(
            iter(await connection.fetch(query, *parameters)), {})

        return result.get('count', 0)

    def _order_by(self) -> str:
        return f"ORDER BY {self.jsonb_field}->>'created_at' DESC NULLS LAST"
