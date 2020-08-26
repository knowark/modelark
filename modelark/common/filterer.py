from typing import Protocol, Callable, Any
from ..common import Domain

Filter = Callable[[Any], bool]


class Filterer(Protocol):
    def parse(self, domain: Domain) -> Filter:
        """Parse domain and return a filter function"""


class DefaultFilterer:
    def parse(self, domain: Domain) -> Filter:
        if not len(domain) == 1:
            return lambda item: True
        value = domain[0][2]
        if not isinstance(domain[0][2], (list, tuple)):
            value = [domain[0][2]]
        return lambda item: getattr(item, domain[0][0]) in value
