import asyncio

from aiohttp import web

from ..config import DEFAULT_HEADERS, PROJECT_DIR

from .auth_decorator import authenticate_token


@asyncio.coroutine
@authenticate_token
def get_benchmark_ops(request):
    """
    Return the json file containing benchmark ops
    :param request: http request object
    :return: json file containing benchmark ops
    """
    return web.FileResponse('{}/ravsock/benchmarkops.json'.format(PROJECT_DIR), headers=DEFAULT_HEADERS)
