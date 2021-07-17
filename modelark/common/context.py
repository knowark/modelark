from typing import Dict
from contextvars import ContextVar


class MetaContext:
    def __init__(self, context: ContextVar, value: Dict) -> None:
        self.context = context
        self.token = self.context.set(value)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.clear()

    def __getattr__(self, name):
        return getattr(self.context, name)

    def clear(self):
        self.context.reset(self.token)
