import unittest
from typing import cast
from modelark.filterer.types import QueryDomain
from modelark.filterer.safe_eval import SafeEval
from modelark.filterer.sql_parser import SqlParser


class TestSqlParser(unittest.TestCase):

    def setUp(self):
        self.parser = SqlParser(placeholder='string')

    def test_sql_parser_object_creation(self):
        self.assertTrue(isinstance(self.parser, SqlParser))

    def test_sql_parser_parse_tuple(self):
        filter_tuple_list = [
            (('field', '=', 99), ('field = %s', 99)),
            (('field', 'like', 'world'), ("field LIKE %s", "world")),
            (('field', 'ilike', 'world'), ("field ILIKE %s", "world")),
            (('field', 'contains', 99), ("field @> {%s}", 99))
        ]
        position = 1
        for filter_tuple, expected in filter_tuple_list:
            result = self.parser._parse_term(filter_tuple, position=position)
            self.assertEqual(result, expected)

    def test_sql_parser_parse_single_term(self):
        domain = [('field', '=', 7)]
        expected = ('field = %s', (7,))
        result = self.parser.parse(domain)
        self.assertEqual(result, expected)

    def test_sql_parser_default_join(self):
        stack = ['field2 <> %s', 'field = %s']
        expected = 'field = %s AND field2 <> %s'
        result = self.parser._default_join(stack)
        self.assertEqual(result, [expected])

    def test_string_parser_parse_multiple_terms(self):
        test_domains = [
            ([('field', '=', 7), ('field2', '!=', 8)],
             ('field = %s AND field2 <> %s', (7, 8))),
            ([('field', '=', 7), ('field2', '!=', 8), ('field3', '>=', 9)],
             ('field = %s AND field2 <> %s AND field3 >= %s', (7, 8, 9))),
            (['|', ('field', '=', 7), ('field2', '!=', 8),
              ('field3', '>=', 9)],
                ('field = %s OR field2 <> %s AND field3 >= %s', (7, 8, 9))),
            (['|', ('field', '=', 7),
              '!', ('field2', '!=', 8), ('field3', '>=', 9)],
             ('field = %s OR NOT field2 <> %s AND field3 >= %s', (7, 8, 9))),
            (['!', ('field', '=', 7)], ('NOT field = %s', (7,))),
        ]

        for test_domain in test_domains:
            result = self.parser.parse(test_domain[0])
            expected = test_domain[1]
            self.assertEqual(result, expected)

    def test_sql_parser_with_empty_list(self):
        domain = []
        result = self.parser.parse(domain)
        expected = "1 = 1", ()
        self.assertEqual(result, expected)

    def test_sql_parser_with_lists_of_lists(self):
        domain = cast(QueryDomain, [['field', '=', 7], ['field2', '!=', 8]])
        expected = ('field = %s AND field2 <> %s', (7, 8))
        result = self.parser.parse(domain)
        self.assertEqual(result, expected)

    def test_sql_parser_with_lists_parameters(self):
        domain = cast(QueryDomain, [['field', 'in', [7]]])
        expected = ('field = ANY(%s)', ([7],))
        result = self.parser.parse(domain)
        self.assertEqual(result, expected)

    def test_sql_parser_parse_evaluator(self):
        self.parser.evaluator = SafeEval()
        domain = [('field', '=', '>>> 3 + 4')]
        expected = ('field = %s', (7,))
        result = self.parser.parse(domain)
        self.assertEqual(result, expected)

    def test_sql_parser_evaluator_context(self):
        self.parser.evaluator = SafeEval()
        domain = [('field', '=', '>>> 3 + value')]
        expected = ('field = %s', (7,))
        context = {'value': 4}
        result = self.parser.parse(domain, context)
        self.assertEqual(result, expected)

    def test_sql_parser_namespaces(self):
        namespaces = ['orders', 'customers']
        domain = [('orders.customer_id', '=', 'customers.id')]

        expected = ('orders.customer_id = %s',
                    ('customers.id',), 'orders, customers')

        result = self.parser.parse(domain, namespaces=namespaces)

        self.assertEqual(result, expected)

    def test_sql_parser_comparison_dict(self):
        comparison_dict = self.parser.comparison_dict
        for key, value in comparison_dict.items():
            term_1 = 'field'
            term_2 = 'value'
            result = value(term_1, term_2)
            assert key is not None
            assert isinstance(result, str)

    def test_sql_parser_jsonb_collection(self):
        domain = [('field_1', '=', 3), '|',
                  ('field_2', '=', True), ('field_3', '=', 7.77)]
        jsonb_collection = 'data'

        normalized_domain = self.parser._to_jsonb_domain(
            domain, jsonb_collection)

        assert normalized_domain == [
            ("(data->>'field_1')::integer", '=', 3),
            '|',
            ("(data->>'field_2')::boolean", '=', True),
            ("(data->>'field_3')::float", '=', 7.77)
        ]

    def test_sql_parser_parse_with_jsonb_collection(self):
        domain = [('field_1', '=', 3)]
        jsonb_collection = 'data'

        result, params = self.parser.parse(
            domain, jsonb_collection=jsonb_collection)

        assert result == "(data->>'field_1')::integer = %s"
        assert params == (3,)

    def test_sql_parser_parse_and_with_numeric_placeholders(self):
        self.parser.placeholder = 'numeric'

        domain = [('field_1', '=', 3), ('field_2', '=', 'world')]
        jsonb_collection = 'data'

        result, params = self.parser.parse(
            domain, jsonb_collection=jsonb_collection)

        assert result == (
            "(data->>'field_1')::integer = $1 AND "
            "(data->>'field_2')::text = $2"
        )
        assert params == (3, 'world')

    def test_sql_parser_parse_or_with_numeric_placeholders(self):
        self.parser.placeholder = 'numeric'

        domain = ['|', ('field_1', '=', 3), ('field_2', '=', 'world')]
        jsonb_collection = 'data'

        result, params = self.parser.parse(
            domain, jsonb_collection=jsonb_collection)

        assert result == (
            "(data->>'field_1')::integer = $1 OR "
            "(data->>'field_2')::text = $2"
        )
        assert params == (3, 'world')

    def test_sql_parser_parse_complex_with_numeric_placeholders(self):
        self.parser.placeholder = 'numeric'

        domain = [
            ('field_1', '=', 3), '!', ('field_2', '=', True), '|',
            ('field_3', '=', 7), ('field_4', '=', 'world')
        ]
        jsonb_collection = 'data'

        result, params = self.parser.parse(
            domain, jsonb_collection=jsonb_collection)

        assert result == (
            "(data->>'field_1')::integer = $1 AND NOT "
            "(data->>'field_2')::boolean = $2 AND "
            "(data->>'field_3')::integer = $3 OR "
            "(data->>'field_4')::text = $4"
        )
        assert params == (3, True, 7, 'world')
