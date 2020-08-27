import inspect
from pytest import fixture
from modelark.common import Editor, DefaultEditor


def test_editor_definition():
    properties = [item[0] for item in inspect.getmembers(
        Editor, lambda o: isinstance(o, property))]
    assert 'user' in properties


def test_default_editor_parse():
    editor = DefaultEditor(
        '81d70e74-9108-404c-ab5f-4f2fe6c8cd08')
    assert editor.user == '81d70e74-9108-404c-ab5f-4f2fe6c8cd08'
