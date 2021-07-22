from typing import List
from .repository import Repository


class RepositoryResolver:
    def __init__(self, repositories: List[Repository]) -> None:
        self.registry = {
            repository.model.__name__: repository
            for repository in repositories
        }

    def resolve(self, model: str) -> Repository:
        return self.registry[model]
