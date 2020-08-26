from typing import Protocol, List, Mapping
from .connection import Connection


class Connector(Protocol):
    async def get(self, zone: str = '') -> Connection:
        """Get a connection from the pool"""

    async def put(self, connection: Connection, zone: str = '') -> None:
        """Return a connection to the pool"""
