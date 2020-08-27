from typing import Protocol, List, Mapping
from .connection import Connection


class Connector(Protocol):
    async def get(self, zone='') -> Connection:
        """Get a connection from the pool"""

    async def put(self, connection: Connection, zone='') -> None:
        """Return a connection to the pool"""
