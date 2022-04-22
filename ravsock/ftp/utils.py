from .client import FTPClient
from ..config import FTP_SERVER_URL


def get_client(host=None, username=None, password=None):
    print("Credentials", host, username, password)
    if host is None:
        host = FTP_SERVER_URL
    print("FTP User credentials:", host, username, password)
    return FTPClient(host=host, user=username, passwd=password)


def check_credentials(host=None, username=None, password=None):
    if host is None:
        host = FTP_SERVER_URL
    print("Credentials", host, username, password)
    try:
        FTPClient(host=host, user=username, passwd=password)
        return True
    except Exception as e:
        print("Error:{}".format(str(e)))
        return False
