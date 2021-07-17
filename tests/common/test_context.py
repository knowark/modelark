import inspect
from pytest import fixture, mark
from modelark.common import MetaContext, ContextVar


pytestmark = mark.asyncio


local_context: ContextVar = ContextVar('LocalContext', default={})


async def test_meta_context_definition():
    value = {'custom': 'metadata'}
    meta_context = MetaContext(local_context, value)

    assert meta_context is not None
    assert meta_context.context.get() == value


async def test_meta_context_context_proxy():
    value = {'custom': 'metadata'}

    context = MetaContext(local_context, value)
    assert context.get() == value

    context.reset(context.token)
    assert context.get() == {}


async def test_meta_context_context_manager():
    value = {'custom': 'metadata'}

    with MetaContext(local_context, value) as context:
        assert isinstance(context, MetaContext)
        assert context.name == 'LocalContext'
        assert context.get() == value

    assert context.get() == {}
