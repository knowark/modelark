from typing import Protocol, Sequence, List, Union, Tuple, Any


Term = Tuple[str, str,  Union[str, int, float, bool, list, tuple]]

Domain = Sequence[Union[str, Term]]

TermTuple = Term

QueryDomain = Domain


class Filterer(Protocol):
    def parse(self, domain: Domain) -> Any:
        """Parse domain and return a filter expression"""


class Conditioner(Protocol):
    def parse(self, domain: Domain) -> Tuple[str, Tuple]:
        """Parse domain and return a condition string with parameters"""
