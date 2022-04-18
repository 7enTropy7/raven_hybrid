import ast

from ..db import ravdb
from ..utils import functions


async def benchmark_callback(sid, data):
    data = ast.literal_eval(data)
    client = ravdb.get_client_by_sid(sid)
    short_names = {}
    short_names['lin'] = 0
    for key, value in data.items():
        new_key = get_key(key, functions)
        short_names[new_key] = value
    if client is not None:
        ravdb.update_client(
            client, capabilities=str(short_names), reporting="idle"
        )


def get_key(val, dict):
    for key, value in dict.items():
        if val == value:
            return key
    return "key doesn't exist"
