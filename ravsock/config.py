import os
from os.path import expanduser
from pathlib import Path

import aiohttp_cors

WAIT_INTERVAL_TIME = 10

BASE_DIR = os.path.join(expanduser("~"), "rdf/ravsock")
PROJECT_DIR = Path(__file__).parent.parent
DATA_FILES_PATH = os.path.join(BASE_DIR, "files")

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(DATA_FILES_PATH, exist_ok=True)

RAVSOCK_LOG_FILE = os.path.join(BASE_DIR, "ravsock.log")

CONTEXT_FOLDER = os.path.join(BASE_DIR, "contexts")
PARAMS_FOLDER = os.path.join(BASE_DIR, "params")

os.makedirs(CONTEXT_FOLDER, exist_ok=True)
os.makedirs(PARAMS_FOLDER, exist_ok=True)

QUEUE_HIGH_PRIORITY = "queue:high_priority"
QUEUE_LOW_PRIORITY = "queue:low_priority"
QUEUE_COMPUTING = "queue:computing"

QUEUE_OPS = "queue:ops"
QUEUE_CLIENTS = "queue:clients"

RDF_REDIS_HOST = os.environ.get("RDF_REDIS_HOST", "localhost")
RDF_REDIS_PORT = os.environ.get("RDF_REDIS_PORT", "6379")
RDF_REDIS_DB = os.environ.get("RDF_REDIS_DB", "0")

# RDF_DATABASE_URI = os.environ.get(
#     "RDF_DATABASE_URI", "sqlite:///{}/rdf.db?check_same_thread=False".format(BASE_DIR)
# )

RDF_DATABASE_URI = "sqlite:///{}/rdf.db?check_same_thread=False".format(BASE_DIR)

FTP_SERVER_LOCATION = "LOCAL"
# FTP_SERVER_URL = "54.201.212.222"
FTP_SERVER_URL = "localhost"
FTP_SERVER_USERNAME = "ubuntu"
FTP_SERVER_PASSWORD = "******"

FTP_SERVER_DIR = "{}/ravftp".format(Path(__file__).parent.parent)
FTP_ENVIRON_DIR = "/opt/homebrew/Caskroom/miniforge/base/envs/rdf/bin"
FTP_RAVOP_FILES_PATH = os.path.join(expanduser("~"), "rdf/ravftp/files")

MAX_OP_CLIENT_MAPPING = 3

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}

DEFAULT_OPTIONS = {"*": aiohttp_cors.ResourceOptions(
             allow_credentials=True, expose_headers="*", allow_headers="*")}

ENCRYPTION = False
SCHEDULER_RUNNING = False
FLASK_SERVER_URL = "http://127.0.0.1:5000/"