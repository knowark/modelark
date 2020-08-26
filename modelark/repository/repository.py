from abc import ABC, abstractmethod
from typing import Tuple, Type, List, Generic, Union, Optional, overload
from ..common import Domain, T, R, L


class Repository(ABC, Generic[T]):
    @property
    def model(self) -> Type[T]:
        raise NotImplementedError('Provide the repository model')

    @abstractmethod
    async def add(self, item: Union[T, List[T]]) -> List[T]:
        "Add method to be implemented."

    @abstractmethod
    async def remove(self, item: Union[T, List[T]]) -> bool:
        "Remove method to be implemented."

    @abstractmethod
    async def count(self, domain: Domain = None) -> int:
        "Count items matching a query domain"

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

    @abstractmethod
    async def search(
            self, domain: Domain,
            limit: int = None, offset: int = None,
            *,
            join: 'Repository[R]' = None,
            link: 'Repository[L]' = None,
            source: str = None,
            target: str = None) -> Union[List[T], List[Tuple[T, List[R]]]]:
        """Search items matching a query domain"""
