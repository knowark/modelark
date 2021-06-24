import operator
from typing import List, Union, Callable, Any, Dict, Tuple
from types import SimpleNamespace
from fnmatch import fnmatchcase
from .types import TermTuple, QueryDomain


class ExpressionParser:

    def __init__(self, evaluator: Callable = lambda x, _: x) -> None:
        self.evaluator = evaluator

        self.comparison_dict = {
            '=': operator.eq,
            '!=': operator.ne,
            '<=': operator.le,
            '<': operator.lt,
            '>': operator.gt,
            '>=': operator.ge,
            'in': lambda x, y: x in y,
            'like': lambda x, y: self._parse_like(x, y),
            'ilike': lambda x, y: self._parse_like(x, y, True),
            'contains': operator.contains
        }

        self.binary_dict = {
            '&': lambda expression_1, expression_2: (
                lambda obj: (expression_1(obj) and expression_2(obj))),
            '|': lambda expression_1, expression_2: (
                lambda obj: (expression_1(obj) or expression_2(obj)))
        }

        self.unary_dict = {
            '!': lambda expression_1: (
                lambda obj: (not expression_1(obj)))
        }

        self.default_join_operator = '&'

    def parse(self, domain: QueryDomain,
              context: Dict[str, Any] = None,
              namespaces: List[str] = []) -> Callable:
        if not domain:
            return lambda obj: True
        stack: List[Callable] = []
        for item in list(reversed(domain)):
            if isinstance(item, str) and item in self.binary_dict:
                first_operand = stack.pop()
                second_operand = stack.pop()
                function = self.binary_dict[str(item)](
                    first_operand, second_operand)
                stack.append(function)
            elif isinstance(item, str) and item in self.unary_dict:
                operand = stack.pop()
                stack.append(self.unary_dict[str(item)](operand))

            stack = self._default_join(stack)

            if isinstance(item, (list, tuple)):
                result = self._parse_term(item, context, namespaces)
                stack.append(result)

        result = self._default_join(stack)[0]
        return result

    def _default_join(self, stack: List[Callable]) -> List[Callable]:
        operator = self.default_join_operator
        if len(stack) == 2:
            first_operand = stack.pop()
            second_operand = stack.pop()
            function = self.binary_dict[operator](
                first_operand, second_operand)
            stack.append(function)
        return stack

    def _parse_term(self, term_tuple: TermTuple,
                    context: Dict[str, Any] = None,
                    namespaces: List[str] = []) -> Callable:
        field, operator, value = term_tuple
        value = self.evaluator(value, context)
        comparator = self.comparison_dict.get(operator)
        return self._build_filter(field, comparator, value, namespaces)

    def _build_filter(self, field, comparator, value, namespaces=[]):
        def function(obj):
            obj_, field_, value_ = self._process_namespaces(
                obj, field, value, namespaces)
            return comparator(getattr(obj_, field_), value_)
        return function

    def _process_namespaces(self, obj, field, value, namespaces):
        if not namespaces or not isinstance(obj, tuple):
            if isinstance(obj, dict):
                obj = SimpleNamespace(**obj)
            return obj, field, value

        base_object = (
            SimpleNamespace(**obj[0]) if
            isinstance(obj[0], dict) else obj[0])

        for i, namespace in enumerate(namespaces):
            namespace_object = (
                SimpleNamespace(**obj[i]) if
                isinstance(obj[i], dict) else obj[i])

            if value.startswith(f"{namespace}."):
                _, attribute = value.split('.')
                value = getattr(namespace_object, attribute)

            if field.startswith(f"{namespace}."):
                _, field = field.split('.')
                base_object = namespace_object

        return base_object, field, value

    @staticmethod
    def _parse_like(value: str, pattern: str, insensitive=False) -> bool:
        if not isinstance(value, str):
            return False
        pattern = pattern.replace('%', '*').replace('_', '?')
        pattern = pattern.lower() if insensitive else pattern
        value = value.lower() if insensitive else value
        return fnmatchcase(value, pattern)
