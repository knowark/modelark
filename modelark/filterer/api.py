from typing import List, Dict, Union, Tuple, Callable, Any
from .expression_parser import ExpressionParser
from .sql_parser import SqlParser
from .types import TermTuple


def expression(domain: List[Union[str, TermTuple]],
               context: Dict[str, Any] = None) -> Callable:
    parser = ExpressionParser()
    return parser.parse(domain, context)


def sql(domain: List[Union[str, TermTuple]],
        context: Dict[str, Any] = None,
        placeholder='numeric') -> Tuple:
    parser = SqlParser(placeholder=placeholder)
    return parser.parse(domain, context)
