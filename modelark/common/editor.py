from typing import Protocol, Callable, Any
from ..common import Domain


class Editor(Protocol):
    @property
    def user(self) -> str:
        """Editing user"""


class DefaultEditor:
    def __init__(self, user='') -> None:
        self._user = user

    @property
    def user(self) -> str:
        return self._user
