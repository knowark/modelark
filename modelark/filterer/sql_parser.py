from typing import List, Dict, Union, Tuple, Any, Callable
from .types import QueryDomain, TermTuple


class SqlParser:

    def __init__(self, evaluator: Callable = lambda x, _: x,
                 placeholder: str = 'numeric',
                 jsonb_collection: str = '') -> None:
        self.evaluator = evaluator
        self.placeholder = placeholder
        self.jsonb_collection = jsonb_collection

        self.comparison_dict = {
            '=': lambda x, y:  ' = '.join([str(x), str(y)]),
            '!=': lambda x, y: ' <> '.join([str(x), str(y)]),
            '<=': lambda x, y: ' <= '.join([str(x), str(y)]),
            '<': lambda x, y: ' < '.join([str(x), str(y)]),
            '>': lambda x, y: ' > '.join([str(x), str(y)]),
            '>=': lambda x, y: ' >= '.join([str(x), str(y)]),
            'in': lambda x, y: '{0} = ANY({1})'.format(str(x), str(y)),
            'like': lambda x, y: "{0} LIKE {1}".format(str(x), str(y)),
            'ilike': lambda x, y: "{0} ILIKE {1}".format(str(x), str(y)),
            'contains': lambda x, y: '{0} @> {{{1}}}'.format(str(x), str(y))
        }

        self.binary_dict = {
            '&': lambda a, b: a + ' AND ' + b,
            '|': lambda a, b: a + ' OR ' + b}

        self.unary_dict = {
            '!': lambda a: 'NOT ' + a}

        self.default_join_operator = '&'

    def parse(self, domain: QueryDomain,
              context: Dict[str, Any] = None,
              namespaces: List[str] = [], jsonb_collection=None) -> Tuple:
        if not domain:
            return "TRUE", ()

        jsonb_collection = jsonb_collection or self.jsonb_collection
        if jsonb_collection:
            domain = self._to_jsonb_domain(domain, jsonb_collection)

        stack: List[str] = []
        params = []
        position = 0
        terms = sum(1 if not isinstance(term, str) else 0 for term in domain)
        for item in list(reversed(domain)):
            if isinstance(item, str) and item in self.binary_dict:
                first_operand = stack.pop()
                second_operand = stack.pop()
                string_term = str(
                    self.binary_dict[str(item)](
                        first_operand, second_operand))
                stack.append(string_term)
            elif isinstance(item, str) and item in self.unary_dict:
                operand = stack.pop()
                stack.append(
                    str(self.unary_dict[str(item)](
                        operand)))

            stack = self._default_join(stack)

            if isinstance(item, (list, tuple)):
                result_tuple = self._parse_term(
                    item, context, position=terms - position)
                stack.append(result_tuple[0])
                params.append(result_tuple[1])
                position += 1

        result_query = str(self._default_join(stack)[0])
        result = [result_query, tuple(reversed(params))]

        if namespaces:
            result.append(", ".join(namespaces))

        return tuple(result)

    def _default_join(self, stack: List[str]) -> List[str]:
        operator = self.default_join_operator
        if len(stack) == 2:
            first_operand = stack.pop()
            second_operand = stack.pop()
            value = str(self.binary_dict[operator](
                str(first_operand), str(second_operand)))
            stack.append(value)
        return stack

    def _parse_term(self, term_tuple: TermTuple,
                    context: Dict[str, Any] = None,
                    position: int = 0) -> Tuple[str, Any]:
        field, operator, value = term_tuple
        if isinstance(value, str):
            value = self.evaluator(value, context)
        function = self.comparison_dict[operator]
        placeholder = (
            f'${position}' if self.placeholder == 'numeric' else '%s')
        result = (function(field, placeholder), value)
        return result

    def _to_jsonb_domain(self, domain: QueryDomain,
                         collection: str) -> List[Union[str, TermTuple]]:
        casts = {'bool': 'boolean', 'int': 'integer', 'float': 'float'}
        normalized_domain: List[Union[str, TermTuple]] = []
        for term in domain:
            if isinstance(term, (tuple, list)):
                field, operator, value = term
                cast = casts.get(type(value).__name__,  'text')
                field = f"(data->>'{field}')::{cast}"
                term = (field, operator, value)
            normalized_domain.append(term)
        return normalized_domain
