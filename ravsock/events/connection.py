import asyncio
import datetime
import json
import os
import subprocess
import threading
from urllib import request
import requests
import paramiko
from ravop import MappingStatus
from sqlalchemy import or_

from ravsock.db import ravdb, ClientOpMapping, ObjectiveClientMapping
from ravsock.encryption import dump_context, load_context
from ravsock.events.scheduler import run_scheduler,update_client_status
from ravsock.ftp import check_credentials, get_client
from ..config import CONTEXT_FOLDER, ENCRYPTION, SCHEDULER_RUNNING
from ..config import FTP_SERVER_URL, FTP_SERVER_USERNAME, FTP_SERVER_PASSWORD, FTP_SERVER_LOCATION, \
    FTP_SERVER_DIR, FTP_ENVIRON_DIR, FLASK_SERVER_URL
from ..globals import globals as g
from ..utils import get_random_string

sio = g.sio


async def connect(sid, environ):
    from urllib import parse
    ps = parse.parse_qs(environ["QUERY_STRING"])
    # print(ps)

    # Check cid available in the query string
    cid = ps.get("cid", None)
    if cid is None:
        await disconnect(sid)
        return

    # Extract client type
    ctype = ps.get("type", None)
    if ctype is not None:
        client_type = ctype[0]
    else:
        client_type = ""

    if not SCHEDULER_RUNNING:
        task1 = sio.start_background_task(run_scheduler)
        task2 = sio.start_background_task(update_client_status)

    await create_client(cid, sid, client_type)


async def create_client(cid, sid, client_type):
    print("Connected:{} {} {}".format(cid, sid, client_type))

    namespace = "/{}".format(client_type)
    cid = cid[0]

    # Create client
    client = ravdb.get_client_by_cid(cid)

    if client is None:
        print("new client connected")
        client = ravdb.create_client(cid=cid, sid=sid, type=client_type, status="connected")
        # Create client sid mapping
        ravdb.create_client_sid_mapping(
            cid=cid, sid=sid, client_id=client.id, namespace=namespace
        )
    else:
        print("existing client connected ")
        client = ravdb.update_client(
            client, sid=sid, connected_at=datetime.datetime.utcnow(), status="connected",
            last_active_time=datetime.datetime.utcnow()
        )

        # Remove assigned subgraph
        if client.current_subgraph_id is not None and client.current_graph_id is not None:
            assigned_subgraph = ravdb.get_subgraph(subgraph_id=client.current_subgraph_id, graph_id=client.current_graph_id)
            ravdb.update_subgraph(assigned_subgraph, status="ready")
            subgraph_ops = ravdb.get_subgraph_ops(graph_id=client.current_graph_id, subgraph_id=client.current_subgraph_id)
            for subgraph_op in subgraph_ops:
                if subgraph_op.status != "computed":
                    ravdb.update_op(subgraph_op, status="pending")
            if client.reporting == "ready":
                ravdb.update_client(client, current_subgraph_id=None, current_graph_id=None)
            else:
                ravdb.update_client(client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
            
        # Update sid
        client_sid_mapping = ravdb.find_client_sid_mapping(cid=cid, sid=sid)

        if client_sid_mapping is None:
            ravdb.create_client_sid_mapping(
                cid=cid, sid=sid, client_id=client.id, namespace=namespace
            )
        else:
            ravdb.update_client_sid_mapping(
                client_sid_mapping.id, cid=cid, sid=sid, namespace=namespace
            )
    print("client connected")

    # create context
    if ENCRYPTION:
        create_context_file(client)

    # Create FTP credentials
    print("Creating ftp credentials...")
    ftp_credentials = client.ftp_credentials
    args = (cid, ftp_credentials, client)
    download_thread = threading.Thread(
        target=between_callback, name="create_credentials", args=args
    )
    download_thread.start()


async def disconnect(sid):
    print("Disconnected:{}".format(sid))

    client = ravdb.get_client_by_sid(sid=sid)
    if client is not None:
        ravdb.update_client(
            client, status="disconnected", disconnected_at=datetime.datetime.utcnow()
        )

        # Update client sid mapping
        ravdb.delete_client_sid_mapping(sid=sid)

        if client.type == "ravjs":
            # Get ops which were assigned to this
            ops = (
                ravdb.session.query(ClientOpMapping)
                    .filter(ClientOpMapping.client_id == client.id)
                    .filter(
                    or_(
                        ClientOpMapping.status == MappingStatus.SENT,
                        ClientOpMapping.status == MappingStatus.ACKNOWLEDGED,
                        ClientOpMapping.status == MappingStatus.COMPUTING,
                    )
                )
                    .all()
            )

            print(ops)
            # Set those ops to pending
            for op in ops:
                ravdb.update_op(op, status=MappingStatus.NOT_COMPUTED)
        elif client.type == "analytics":
            # Update ops
            ops = (
                ravdb.session.query(ObjectiveClientMapping)
                    .filter(ObjectiveClientMapping.client_id == client.id)
                    .filter(
                    or_(
                        ObjectiveClientMapping.status == MappingStatus.SENT,
                        ObjectiveClientMapping.status == MappingStatus.ACKNOWLEDGED,
                        ObjectiveClientMapping.status == MappingStatus.COMPUTING,
                    )
                )
                    .all()
            )

            print(ops)
            # Set those ops to pending
            for op in ops:
                ravdb.update_op(op, status=MappingStatus.NOT_COMPUTED)


def between_callback(*args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(create_credentials(*args))
    loop.close()


async def create_credentials(cid, ftp_credentials, client):
    # print("Type:", ftp_credentials)
    if ftp_credentials is None or ftp_credentials == "None" or ftp_credentials == "null":
        credentials = add_user(cid)

        ravdb.update_client(client, ftp_credentials=json.dumps(credentials))
    else:
        ftp_credentials = json.loads(ftp_credentials)
        print("FTP credentials:", ftp_credentials)
        if not check_credentials(**ftp_credentials):
            credentials = add_user(cid)
            ravdb.update_client(client, ftp_credentials=json.dumps(credentials))
        else:
            pass
           
    # Upload context file
    if ENCRYPTION:
        upload_context_file(client)


async def pong(sid, data):
    """
    Client is available
    """
    print("Pong: {} {}".format(sid, data))

    # TODO: Update the status of this client


def create_context_file(client):
    """
    Create and dump context
    """
    print("Creating context...")
    # Create
    context = get_context()

    # Dump
    filename = "context_with_private_key_{}.txt".format(client.cid)
    dump_context(
        context, os.path.join(CONTEXT_FOLDER, filename), save_secret_key=True
    )
    ravdb.update_client(client, context=json.dumps({"context_filename": filename}))
    print("Context created and dumped")


def upload_context_file(client):
    """
    Upload context file to ftp server
    :param client: client object
    :return: ftp credentials and file path
    """
    print("Uploading context file...")
    if client.ftp_credentials is not None:
        context = load_context(os.path.join(CONTEXT_FOLDER, json.loads(client.context)['context_filename']))
        context.make_context_public()
        filename2 = "context_without_private_key_{}.txt".format(client.cid)
        filepath2 = os.path.join(CONTEXT_FOLDER, filename2)
        dump_context(context, filepath2, save_secret_key=False)
        ftp_credentials = json.loads(client.ftp_credentials)
        print("Connect to upload", ftp_credentials)
        ftp = get_client(
            username=ftp_credentials["username"],
            password=ftp_credentials["password"],
        )
        ftp.upload(filepath2, filename2)

        print("Context file uploaded")
        return ftp_credentials, filename2
    else:
        print("Client credentials are invalid")
        return None

def add_user(cid):
    username = cid
    password = get_random_string(10)
    
    print("Password:", password)
    if FTP_SERVER_LOCATION == "LOCAL":
        print("Creating...")
        process = subprocess.Popen(
            ["{}/python".format(FTP_ENVIRON_DIR), "{}/add_user.py".format(FTP_SERVER_DIR), "--username", username,
             "--password", password], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        process.terminate()

        endpoint = f"add_user?username={username}&password={password}"
        requests.get("{}{}".format(FLASK_SERVER_URL, endpoint))
    

        # subprocess.Popen(
        #     ["sh", "{}/restart_server.sh".format(FTP_SERVER_DIR), FTP_ENVIRON_DIR, FTP_SERVER_DIR],
        #     stdout=subprocess.PIPE, stderr=subprocess.PIPE)




    else:
        print("Creating:", username, password)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(FTP_SERVER_URL, 22, FTP_SERVER_USERNAME, FTP_SERVER_PASSWORD)

        stdin, stdout, stderr = ssh.exec_command(
            "sudo python3 ~/raven-ftp-server/add_user.py --username {} --password {}".format(
                username, password
            )
        )

        ssh.exec_command(
            "nohup sudo python3 ~/raven-ftp-server/run.py --action restart > output.log &"
        )

        for line in iter(stderr.readline, ""):
            print(line, end="")

        final_output = ""

        for line in iter(stdout.readline, ""):
            print(line, end="")
            final_output += line

        print("Final:", final_output, type(final_output))

        ssh.close()

    return {"username": username, "password": password}
