import calendar
import datetime
import time
from typing import Dict, Any


class SafeEval:
    def __init__(self, prefix: str = None) -> None:
        self._prefix = '>>>' if prefix is None else prefix
        self._globals: Dict = {
            '__builtins__': {}
        }
        self._forbidden = ['__', '**']
        self._safe_builtins = {
            'abs': abs,
            'calendar': calendar,
            'datetime': datetime,
            'float': float,
            'int': int,
            'max': max,
            'min': min,
            'round': round,
            'str': str,
            'sum': sum,
            'time': time
        }

    def __call__(self, expression: str, locals: Dict[str, Any] = None) -> Any:
        if not isinstance(expression, str) or not expression.startswith(
                self._prefix) or any(
                    character in expression for character in self._forbidden):
            return expression

        if not locals:
            locals = {}

        expression = expression[len(self._prefix):].strip()
        locals.update(self._safe_builtins)

        return eval(expression, self._globals, locals)
