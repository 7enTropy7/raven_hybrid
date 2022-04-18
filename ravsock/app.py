import os

import aiohttp_cors
import aiohttp_jinja2
import jinja2
import socketio
from aiohttp import web

from .config import DEFAULT_OPTIONS


def create_server_app_and_cors():
    # Create an async socket server
    sio = socketio.AsyncServer(
        async_mode="aiohttp", async_handlers=True, logger=False, cors_allowed_origins="*", ping_timeout=600, max_http_buffer_size=(1024**2)*30
    )

    # Creates a new Aiohttp Web Application
    app = web.Application(client_max_size=(1024**2)*30)

    app['static_root_url'] = '/static'
    STATIC_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ravjs")
    app.router.add_static('/static/', STATIC_PATH, name='static')
    aiohttp_jinja2.setup(app,
                         loader=jinja2.FileSystemLoader(STATIC_PATH))

    # Setup cors
    cors = aiohttp_cors.setup(app, defaults=DEFAULT_OPTIONS)

    # Binds our Socket.IO server to our Web App instance
    sio.attach(app)

    return sio, app, cors
