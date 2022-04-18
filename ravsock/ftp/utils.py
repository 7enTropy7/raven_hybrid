import subprocess
import sys

import paramiko

from ..config import FTP_SERVER_URL, FTP_SERVER_USERNAME, FTP_SERVER_PASSWORD, FTP_SERVER_LOCATION, \
    FTP_SERVER_DIR, FTP_ENVIRON_DIR
from ravsock.events.scheduler import scheduler, update_client_status
from ..utils import get_random_string
from ravsock.globals import globals as g


