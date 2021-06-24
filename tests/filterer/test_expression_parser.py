from typing import cast
import unittest
from fnmatch import fnmatchcase
from unittest.mock import Mock
from modelark.filterer.types import QueryDomain
from modelark.filterer.safe_eval import SafeEval
from modelark.filterer.expression_parser import ExpressionParser


class TestExpressionParser(unittest.TestCase):

    def setUp(self):
        self.parser = ExpressionParser()

    def test_expression_parser_object_creation(self):
        self.assertTrue(isinstance(self.parser, ExpressionParser))

    def test_expression_parser_parse_tuple(self):
        filter_tuple_list = [
            (('field', '=', 9), lambda obj: obj.field == 9, Mock(field=9)),
            (('field', '!=', 9), lambda obj: obj.field != 9, Mock(field=8)),
            (('field', '>', 9), lambda obj: obj.field > 9, Mock(field=10)),
            (('field', '<', 9), lambda obj: obj.field < 9, Mock(field=8)),
            (('field', '<=', 9), lambda obj: obj.field <= 9, Mock(field=8)),
            (('field', '>=', 9), lambda obj: obj.field >= 9, Mock(field=10)),
            (('field', 'in', [1, 2, 3]), lambda obj: obj.field in [1, 2, 3],
             Mock(field=2))
        ]

        for test_tuple in filter_tuple_list:
            filter_tuple = test_tuple[0]
            expected_function = test_tuple[1]
            mock_object = test_tuple[2]
            function = self.parser._parse_term(filter_tuple)

            self.assertTrue(callable(function))
            self.assertTrue(function(mock_object))
            self.assertEqual(
                function(mock_object), expected_function(mock_object))

    def test_expression_parser_parse_single_term(self):
        domain: QueryDomain = [('field', '=', 7)]

        def expected(obj):
            return getattr(obj, 'field') == 7

        mock_object = Mock()
        mock_object.field = 7

        function = self.parser.parse(domain)

        self.assertTrue(callable(function))
        self.assertTrue(function(mock_object))
        self.assertEqual(function(mock_object), expected(mock_object))

        mock_object.field = 5
        self.assertFalse(function(mock_object))

    def test_expression_parser_default_join(self):
        stack = [lambda obj: obj.field2 != 8, lambda obj: obj.field == 7]

        def expected(obj):
            return (obj.field == 7 and obj.field2 != 8)

        result_stack = self.parser._default_join(stack)

        mock_object = Mock()
        mock_object.field = 7
        mock_object.field2 = 9

        self.assertTrue(isinstance(result_stack, list))
        self.assertEqual(result_stack[0](mock_object), expected(mock_object))
        self.assertTrue(result_stack[0](mock_object))

        mock_object.field = 5
        self.assertFalse(result_stack[0](mock_object))

    def test_expression_parser_parse_multiple_terms(self):
        test_domains = [
            ([('field', '=', 7), ('field2', '!=', 8)],
             lambda obj: (obj.field2 != 8 and obj.field == 7),
             Mock(field=7, field2=8)),
            ([('field', '=', 7), ('field2', '!=', 8), ('field3', '>=', 9)],
             (lambda obj: (obj.field2 != 8 and
                           obj.field == 7 and obj.field3 >= 9)),
             Mock(field=7, field2=5, field3=9)),
            (['|', ('field', '=', 7), ('field2', '!=', 8)],
             lambda obj: (obj.field2 != 8 or obj.field == 7),
             Mock(field=7, field2=8)),
            (['|', ('field', '=', 7), '!', ('field2', '!=', 8),
              ('field3', '>=', 9)],
                (lambda obj: (obj.field == 7 or
                              not obj.field2 != 8 and obj.field3 >= 9)),
             Mock(field=7, field2=8, field3=9)),
            (['!', ('field', '=', 7)],
             lambda obj: (not obj.field == 7),
             Mock(field=7)),
        ]

        for test_domain in test_domains:
            result = self.parser.parse(test_domain[0])
            expected = test_domain[1]
            obj = test_domain[2]
            self.assertTrue(callable(result))
            self.assertEqual(result(obj), expected(obj))

    def test_expression_parser_with_empty_list(self):
        domain = []
        result = self.parser.parse(domain)
        mock_object = Mock()
        mock_object.field = 7
        self.assertTrue(result(mock_object))

    def test_expression_parser_with_lists_of_lists(self):
        domain = cast(QueryDomain, [['field', '=', 7], ['field2', '!=', 8]])

        def expected(obj):
            return (obj.field == 7 and obj.field2 != 8)

        result = self.parser.parse(domain)

        mock_object = Mock()
        mock_object.field = 7
        mock_object.field2 = 9

        self.assertTrue(result(mock_object))
        self.assertEqual(result(mock_object), expected(mock_object))

    def test_expression_parser_parse_like(self):
        filter_tuple_list = [
            (('field', 'like', "Hello"),
             lambda obj: fnmatchcase(obj.field, "Hello"),
             Mock(field="Hello")),
            (('field', 'like', "Hello%"),
             lambda obj: fnmatchcase(obj.field, "Hello*"),
             Mock(field="Hello World")),
            (('field', 'like', "%World"),
             lambda obj: fnmatchcase(obj.field, "*World"),
             Mock(field="Hello World")),
            (('field', 'like', "Hello_"),
             lambda obj: fnmatchcase(obj.field, "Hello?"),
             Mock(field="HelloX")),
            (('field', 'ilike', "%World"),
             lambda obj: fnmatchcase(obj.field, "*world"),
             Mock(field="hello world")),
            (('field', 'ilike', "%eLLo%"),
             lambda obj: fnmatchcase(obj.field, "*ello*"),
             Mock(field="hello world")),
            (('field', 'like', "Hello%"),
             lambda obj: False, Mock(field=9)),
        ]

        for test_tuple in filter_tuple_list:
            filter_tuple = test_tuple[0]
            expected_function = test_tuple[1]
            mock_object = test_tuple[2]

            function = self.parser._parse_term(filter_tuple)

            self.assertTrue(callable(function))
            self.assertEqual(function(mock_object),
                             expected_function(mock_object))

    def test_expression_parser_parse_contains(self):
        filter_tuple_list = [
            (('field', 'contains', "333"), lambda obj: '333' in obj.field,
             Mock(field=['1', '22', '333']))]

        for test_tuple in filter_tuple_list:
            filter_tuple = test_tuple[0]
            expected_function = test_tuple[1]
            mock_object = test_tuple[2]

            function = self.parser._parse_term(filter_tuple)

            self.assertTrue(callable(function))
            self.assertEqual(
                function(mock_object), expected_function(mock_object))

    def test_expression_parser_parse_evaluator(self):
        self.parser.evaluator = SafeEval()
        domain = [('field', '=', '>>> 3 + 4')]
        result = self.parser.parse(domain)
        mock_object = Mock(field=7)
        self.assertTrue(result(mock_object))

    def test_expression_parser_evaluator_context(self):
        self.parser.evaluator = SafeEval()
        domain = [('field', '=', '>>> 3 + value')]
        context = {'value': 4}
        result = self.parser.parse(domain, context)
        mock_object = Mock(field=7)
        self.assertTrue(result(mock_object))

    def test_expression_parser_namespaces(self):
        namespaces = ['orders', 'customers']
        domain = [('orders.customer_id', '=', 'customers.id')]

        result = self.parser.parse(domain, namespaces=namespaces)
        mock_object = (
            {'id': '77', 'customer_id': '03'}, {'id': '03', 'name': 'Joe'})

        result(mock_object)

        self.assertTrue(result(mock_object))

    def test_expression_parser_with_dict(self):
        domain = [('field', '=', 7)]

        result = self.parser.parse(domain)
        mock_object = {'field': 7}

        self.assertTrue(result(mock_object))
