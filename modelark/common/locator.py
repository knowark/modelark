from typing import Protocol, Callable, Any
from ..common import Domain


class Locator(Protocol):
    @property
    def location(self) -> str:
        """Data Location"""


class DefaultLocator:
    def __init__(self, location='default') -> None:
        self._location = location

    @property
    def location(self) -> str:
        return self._location
