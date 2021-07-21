from typing import TypeVar, Dict, Any
from uuid import uuid4


class Entity:
    def __init__(self, **attributes) -> None:
        self.id = str(attributes.get('id', uuid4()))
        self.status = attributes.get('status', '')
        self.created_at = attributes.get('created_at', 0)
        self.updated_at = attributes.get('updated_at', self.created_at)
        self.created_by = attributes.get('created_by', '')
        self.updated_by = attributes.get('updated_by', self.created_by)

    def transition(self: 'T', state: Dict[str, Any]) -> 'T':
        self.__dict__.update(state)
        return self


T = TypeVar('T', bound=Entity, covariant=True)
R = TypeVar('R', bound=Entity, covariant=True)
L = TypeVar('L', bound=Entity, covariant=True)
