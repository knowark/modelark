from typing import Protocol, Callable, Any
from ..common import Domain


class Locator(Protocol):
    @property
    def location(self) -> str:
        """Data Location"""

    @property
    def zone(self) -> str:
        """Data Zone"""


class DefaultLocator:
    def __init__(self, location='default', zone='') -> None:
        self._location = location
        self._zone = zone

    @property
    def location(self) -> str:
        return self._location

    @property
    def zone(self) -> str:
        return self._zone
