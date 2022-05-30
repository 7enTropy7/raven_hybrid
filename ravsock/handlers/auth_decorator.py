import functools
import sys

def authenticate_token(method):
    @functools.wraps(method)
    def wrapper(request, *args, **kwargs):
        if request.headers["Authorization"] == "<ravenverse_token>":
            print("\n======= Authentication Successful: {} =======".format(method.__name__)) # where does user come from?!
        return method(request, *args, **kwargs)
    return wrapper

def socketio_authenticate_token(method):
    @functools.wraps(method)
    async def wrapper(sid, environ, auth):
        if auth["Authorization"] == "<ravenverse_token>":
            print("\n======= Authentication Successful: {} =======".format(method.__name__)) # where does user come from?!
        return await method(sid, environ, auth)
    return wrapper