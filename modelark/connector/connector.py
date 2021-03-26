from typing import Protocol, List, Mapping
from .connection import Connection


class Connector(Protocol):
    async def get(self, *args, **kwargs) -> Connection:
        """Get a connection from the pool"""

    async def put(self, connection: Connection, *args, **kwargs) -> None:
        """Return a connection to the pool"""
