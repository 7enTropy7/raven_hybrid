from .app import create_server_app_and_cors
from .utils import Singleton

from engineio.payload import Payload

Payload.max_decode_packets = 500000

@Singleton
class Globals(object):
    def __init__(self):
        self._sio, self._app, self._cors = create_server_app_and_cors()
        self._Queue = []

    @property
    def sio(self):
        return self._sio

    @property
    def app(self):
        return self._app

    @property
    def cors(self):
        return self._cors

    @property
    def Queue(self):
        return self._Queue


globals = Globals.Instance()
