from typing import Protocol, List, Mapping


class Connection(Protocol):
    async def execute(self, query: str, *args) -> str:
        """Execute a query"""

    async def fetch(self, query: str, *args) -> List[Mapping]:
        """Fetch the given query records"""
