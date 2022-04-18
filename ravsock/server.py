import aiohttp_cors

from .app import create_server_app_and_cors
from .events import disconnect, create_credentials, between_callback, scheduler, create_client
from .events.benchmarking import benchmark_callback
from .handlers import *
from .utils import get_logger
from .globals import globals as g

sio = g.sio

logger = get_logger()

# sio, app, cors = create_server_app_and_cors()


