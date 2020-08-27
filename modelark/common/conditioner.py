from typing import Protocol, Tuple, Any
from ..common import Domain


Condition = str
Parameters = Tuple


class Conditioner(Protocol):
    def parse(self, domain: Domain) -> Tuple[Condition, Parameters]:
        """Parse domain and return a condition string"""


class DefaultConditioner:
    def parse(self, domain: Domain) -> Tuple[Condition, Parameters]:
        if not len(domain) == 1:
            return ("1 = 1", tuple())
        value = domain[0][2]
        if not isinstance(domain[0][2], (list, tuple)):
            value = [domain[0][2]]
        return (f'{domain[0][0]} = ANY(ARRAY{str(value)})', tuple(value))
