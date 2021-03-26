from typing import Protocol, List, Mapping


class Connection(Protocol):
    async def execute(self, query: str, *args, **kwargs) -> str:
        """Execute a query"""

    async def fetch(self, query: str, *args, **kwargs) -> List[Mapping]:
        """Fetch the given query records"""
