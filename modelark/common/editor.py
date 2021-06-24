from typing import Protocol, Callable, Any


class Editor(Protocol):
    @property
    def reference(self) -> str:
        """Editor reference"""


class DefaultEditor:
    def __init__(self, reference='') -> None:
        self._reference = reference

    @property
    def reference(self) -> str:
        return self._reference
