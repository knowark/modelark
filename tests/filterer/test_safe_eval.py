import unittest
import datetime
from modelark.filterer.safe_eval import SafeEval


class TestSafeEval(unittest.TestCase):

    def setUp(self):
        self.safe_eval = SafeEval()

    def test_safe_eval_object_creation(self):
        self.assertTrue(isinstance(self.safe_eval, SafeEval))

    def test_safe_eval_doesnt_have_globals_or_locals_by_default(self):
        with self.assertRaises(NameError) as e:
            self.safe_eval('>>> dir()')

    def test_safe_eval_doesnt_evaluate_unprefixed_strings(self):
        result = self.safe_eval('8 + 8')
        self.assertEqual(result, '8 + 8')

    def test_safe_eval_evaluate_simple_expression(self):
        result = self.safe_eval('>>> 2 + 2')
        self.assertEqual(result, 4)

    def test_safe_custom_prefix(self):
        safe_eval = SafeEval(prefix='')
        result = safe_eval('2 + 2')
        self.assertEqual(result, 4)

    def test_safe_eval_forbidden_characters(self):
        forbidden_expressions = [
            '>>> __name__',
            '>>> 4 ** 4'
        ]

        for expression in forbidden_expressions:
            result = self.safe_eval(expression)
            self.assertEqual(result, expression)

    def test_safe_eval_safe_builtins(self):
        forbidden_expressions = [
            ('>>> datetime.datetime.now()', datetime.datetime),
            ('>>> max(1, 2)', int),
            ('>>> float(5)', float),
            ('>>> int(time.time()) + 3600', int)
        ]

        for expression, expected in forbidden_expressions:
            result = self.safe_eval(expression)
            self.assertEqual(type(result), expected)

    def test_safe_eval_custom_locals(self):
        expression = '>>> custom_function()'
        custom_locals = {
            'custom_function': lambda: 77
        }

        result = self.safe_eval(expression, custom_locals)
        self.assertEqual(result, 77)

    def test_safe_eval_only_works_with_strings(self):
        expression = True
        result = self.safe_eval(expression)  # type: ignore
        self.assertEqual(result, expression)

        expression = 55
        result = self.safe_eval(expression)  # type: ignore
        self.assertEqual(result, expression)
