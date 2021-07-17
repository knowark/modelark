from abc import ABC, abstractmethod
from typing import List, Generic, Union
from ..common import T, R, L
from ..filterer import Domain


class RepositoryInterface(ABC, Generic[T]):
    @abstractmethod
    async def add(self, item: Union[T, List[T]]) -> List[T]:
        "Add method to be implemented."

    @abstractmethod
    async def remove(self, item: Union[T, List[T]]) -> bool:
        "Remove method to be implemented."

    @abstractmethod
    async def count(self, domain: Domain = None) -> int:
        "Count items matching a query domain"

    @abstractmethod
    async def search(self, domain: Domain,
                     limit: int = None, offset: int = None,
                     order: str = None) -> List[T]:
        """Standard search method"""
