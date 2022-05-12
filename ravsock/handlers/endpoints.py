import ast
import asyncio
import json
import subprocess
import os
import threading
import requests
import aiohttp_jinja2

from ..db import RavQueue
from ..utils import OpStatus, find_complexity_and_output_dims
from ..config import QUEUE_HIGH_PRIORITY, QUEUE_LOW_PRIORITY, DATA_FILES_PATH, FTP_RAVOP_FILES_PATH, FLASK_SERVER_URL
from aiohttp import web
from sqlalchemy.orm import class_mapper

from ..config import BASE_DIR
from ..db import ravdb
from ..utils import (
    dump_data,
    dump_data_non_ftp,
    copy_data,
    convert_to_ndarray,
    load_data_from_file,
    convert_ndarray_to_str,
    find_dtype,
    get_op_stats)

from ..events.scheduler import final_scheduler_call

from ..utils import get_random_string

from ..config import FTP_SERVER_URL, FTP_SERVER_USERNAME, FTP_SERVER_PASSWORD, FTP_SERVER_LOCATION, \
    FTP_SERVER_DIR, FTP_ENVIRON_DIR

# from ravop.core import t

# ----- Utils -----


def db_model_to_dict(obj):
    obj1 = obj.__dict__
    del obj1["_sa_instance_state"]
    del obj1["created_at"]
    return obj1


def serialize(model):
    """
    db_object => python_dict
    """
    # first we get the names of all the columns on your model
    columns = [c.key for c in class_mapper(model.__class__).columns]
    # then we return their values in a dict
    return dict((c, getattr(model, c)) for c in columns)


# ------ OPS ENDPOINTS ------

# We can define aiohttp endpoints just as we normally would with no change
async def load_worker(request):
    response = aiohttp_jinja2.render_template('worker.html',
                                              request,
                                              {})
    response.headers['Content-Language'] = 'en'
    return response


    with open("static/worker.html") as f:
        return web.Response(text=f.read(), content_type="text/html")


async def op_create(request):
    # http://localhost:9999/op/create/?name=None&graph_id=None&node_type=input&inputs=null&outputs=[1]&op_type=other&operator=linear&status=computed&params={}
    """
    payload = { str : str }
    """
    # try:

    data = await request.json()

    if len(data.keys()) == 0:
        return web.Response(
            text=str({"message": "Invalid parameters"}),
            content_type="text/html",
            status=400,
        )

    op = ravdb.create_op(**data)
    print(op)
    # op_complexity, output_dims = find_complexity_and_output_dims(op)

    ravdb.update_op(op, complexity="1", output_dims="1")

    op_dict = serialize(op)

    # Remove datetime key
    del op_dict["created_at"]
    # print(type(op_dict), op_dict)

    # Add op to queue
    if op.status != OpStatus.COMPUTED and op.status != OpStatus.FAILED:
        # if g.graph_id is None:
        q = RavQueue(name=QUEUE_HIGH_PRIORITY)
        q.push(op.id)
        # else:
        #     q = RavQueue(name=QUEUE_LOW_PRIORITY)
        #     q.push(op.id)

    return web.json_response(op_dict, content_type="application/json", status=200)

    # except Exception as e:
    #
    #     print("\nOP CREATE ENDPOINT ERROR : ", str(e))
    #
    #     return web.json_response(
    #         {"message": "Unable to create op"}, content_type="text/html", status=400
    #     )


async def op_get(request):
    # http://localhost:9999/op/get/?id=3
    """
    params = id(op_id) : int
    """
    try:

        op_id = request.rel_url.query["id"]
        op_object = ravdb.get_op(op_id)
        op_dict = serialize(op_object)

        # Remove datetime key
        del op_dict["created_at"]
        # print(type(op_dict), op_dict)

        return web.json_response(op_dict, content_type="application/json", status=200)

    except Exception as e:

        print("\nOP GET ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid op id"}, content_type="text/html", status=400
        )


async def op_get_by_name(request):
    # http://localhost:9999/op/get/name/?op_name="None"&id="None"
    """
    params = op_name : str, id(graph_id) : int
    """
    try:
        op_name = request.rel_url.query["op_name"]
        graph_id = request.rel_url.query["id"]
        ops = ravdb.get_ops_by_name(op_name, graph_id)
        ops_dicts = []

        for op in ops:
            op_dict = serialize(op)
            # Remove datetime key
            del op_dict["created_at"]
            ops_dicts.append(op_dict)

        # print(type(ops_dicts), ops_dicts)

        return web.json_response(ops_dicts, content_type="application/json", status=200)

    except Exception as e:

        print("\nOP GET ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid op_id or op_name"},
            content_type="text/html",
            status=400,
        )


async def op_get_all(request):
    # http://localhost:9999/op/get/all
    """
    returns : List(ops_dicts)
    """
    try:
        graph_id = request.rel_url.query['graph_id']
        client = ravdb.get_client_by_cid(request.rel_url.query['cid'])

        if graph_id is not None:
            subgraphs = ravdb.get_subgraphs_from_graph(graph_id=graph_id)
            ops = []
            for subgraph in subgraphs:
                ops.extend(ast.literal_eval(subgraph.op_ids))
                ravdb.create_subgraph_client_mapping(subgraph_id=subgraph.id, client_id=client.id)

            ops = [ravdb.get_op(op_id=op_id) for op_id in ops]
        else:
            ops = ravdb.get_all_ops()

        ops_dicts = []
        for op in ops:
            op_dict = serialize(op)
            # Remove datetime key
            del op_dict["created_at"]
            ops_dicts.append(op_dict)

        return web.json_response(ops_dicts, content_type="application/json", status=200)

    except Exception as e:

        print("\nOP GET ALL ERROR : ", str(e))

        return web.json_response(
            {"message": "Unable to get all Ops"}, content_type="text/html", status=400
        )


async def op_status(request):
    # http://localhost:9999/op/status/?id=3
    """
    params = id(op_id) : int
    returns = status(str)
    """

    op_id = request.rel_url.query["id"]

    op = ravdb.get_op(op_id)

    if op is None:
        return web.json_response(
            {"message": "Invalid op id"}, content_type="text/html", status=400
        )
    else:
        return web.json_response(
            {"op_status": op.status}, content_type="application/json", status=200
        )

async def op_delete(request):
    # http://localhost:9999/op/delete/?id=1
    """
    params  = id(op_id) : int
    """
    try:
        op_id = request.rel_url.query["id"]
        op_obj = ravdb.get_op(op_id)
        ravdb.delete(op_obj)
        data = {
            "op_id": op_id,
            "message": "Op has been deleted successfully",
        }

        return web.json_response(
            data,
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nOP DELETE ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid OP id"}, content_type="text/html", status=400
        )


# op_get_my_graph_id
async def op_get_my_graph_id(request):
    # http://localhost:9999/op/get_my_graph_id/?op_id=3
    """
    params = id(op_id) : int
    returns = graph id of that op
    """

    op_id = request.rel_url.query["op_id"]

    op = ravdb.get_op(op_id)

    if op is None:
        return web.json_response(
            {"message": "Invalid op id"}, content_type="text/html", status=400
        )
    else:
        graph_id = op.graph_id
        return web.json_response(
            {"my_graph_id": graph_id}, content_type="application/json", status=200
        )

# ------ DATA ENDPOINTS ------


async def data_create(request):
    # http://localhost:9999/data/create/
    """
    dtype : str
    value : int, list, float, ndarray
    """
    try:
        data = await request.json()

        if 'value' in data:
            value = convert_to_ndarray(data["value"])
            username = data['username']
            dtype = data['dtype']
            data = ravdb.create_data(dtype=dtype)
            file_path = dump_data_non_ftp(data.id, value, username)
            ravdb.update_data(data, file_path=file_path, file_size=value.size*value.itemsize)
            data_dict = serialize(data)

            if data.file_path is not None:
                data_dict["value"] = value.tolist()

        else:

            dtype = data['dtype']
            username = data['username']
            data = ravdb.create_data(dtype=dtype)
            file_path = os.path.join(FTP_RAVOP_FILES_PATH, "{}/data_{}.pkl".format(username,data.id))
            # Update file path
            ravdb.update_data(data, file_path=file_path)
            # Serialize db object
            data_dict = serialize(data)

        # Remove datetime key
        del data_dict["created_at"]
        del data_dict["file_path"]

        return web.json_response(data_dict, content_type="application/json", status=200)

    except Exception as e:

        print("\nDATA CREATE ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Unable to create Data"}, content_type="text/html", status=400
        )


async def data_get(request):
    # http://localhost:9999/data/get?id=1
    """
    params = id(data_id) : int
    """

    # try:
    data_id = request.rel_url.query["id"]
    # print(data_id)

    if data_id is None:
        return web.json_response(
                   {"message": "Data id parameter is required"}, content_type="text/html", status=400
               )

    data = ravdb.get_data(data_id=data_id)

    if data is None:
        return web.json_response(
            {"message": "Invalid Data id"}, content_type="text/html", status=400
        )
    data_dict = serialize(data)
    if data.file_path is not None:
        data_dict["value"] = load_data_from_file(data.file_path).tolist()
    # print(type(data_dict), data_dict)

    # Remove datetime key
    del data_dict["created_at"]
    del data_dict["file_path"]

    # print(data_dict)

    return web.json_response(data_dict, content_type="application/json", status=200)

    # except Exception as e:
    #
    #     print("\nDATA GET ENDPOINT ERROR : ", str(e))
    #
    #     return web.json_response(
    #         {"message": "Invalid Data id"}, content_type="text/html", status=400
    #     )


async def data_get_data(request):
    # http://localhost:9999/data/get/data/?id=1
    """
    params = id(data_id) : int
    returns = data
    """

    try:
        data_id = request.rel_url.query["id"]
        # print(data_id)

        data = ravdb.get_data(data_id=data_id)

        value = load_data_from_file(data.file_path).tolist()

        return web.json_response(
            {"value": value}, content_type="application/json", status=200
        )

    except Exception as e:

        print("\nDATA GET DATA ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Data id"}, content_type="text/html", status=400
        )


async def data_delete(request):
    # http://localhost:9999/data/delete/?id=1

    try:
        data_id = request.rel_url.query["id"]
        # print(data_id)

        ravdb.delete_data(data_id=data_id)

        return web.json_response(
            {"data_id": data_id, "message": "Data has been deleted successfully"},
            content_type="application/json",
            status=200,
        )

    except Exception as e:

        print("\nDATA DELETE ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Data id"}, content_type="text/html", status=400
        )


# ------ GRAPH ENDPOINTS ------


async def graph_create(request):
    # http://localhost:9999/graph/create/
    try:
        data = await request.json()
        # print(data)

        # Create a new graph
        graph_obj = ravdb.create_graph()
        ravdb.update("graph", id=graph_obj.id, **data)

        # Serialize db object
        graph_dict = serialize(graph_obj)
        # Remove datetime key
        del graph_dict["created_at"]

        # print("GRAPH TYPE == ", type(graph_dict), "GRAPH == ", graph_dict)

        return web.json_response(
            graph_dict,
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nGRAPH CREATE ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Unable to create Graph"}, content_type="text/html", status=400
        )

async def graph_get_last_graph_id(request):
    # http://localhost:9999/graph/get/graph_id
    """
    returns = graph_id
    """
    try:
        graph_id = ravdb.get_last_graph_id()
        print(graph_id)
        # graph_id = graph_obj.id
        return web.json_response(
            {"graph_id": graph_id},
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )
    except Exception as e:

        print("\nGRAPH GET LAST GRAPH_ID ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Failed to Fetch Latest Graph_id"}, content_type="text/html", status=400
        )


async def graph_get(request):
    # http://localhost:9999/graph/get/?id=1
    """
    params = id(graph_id) : int
    returns = graph_dict
    """

    try:
        graph_id = request.rel_url.query["id"]
        # print('Getting Graph_id: ', graph_id)
        graph_obj = ravdb.get_graph(graph_id=graph_id)
        # Serialize db object
        graph_dict = serialize(graph_obj)
        # Remove datetime key
        del graph_dict["created_at"]

        return web.json_response(
            graph_dict,
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nGRAPH GET ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Graph id"}, content_type="text/html", status=400
        )


@asyncio.coroutine
async def graph_get_all(request):
    # http://localhost:9999/graph/get/all
    """
    returns = list(graph_dicts)
    """

    try:
        approach = request.rel_url.query["approach"]
        graph_objs = ravdb.get_all_graphs(approach=approach)
        graph_dicts = []

        for graph in graph_objs:
            graph_dict = serialize(graph)
            # Remove datetime key
            del graph_dict["created_at"]
            graph_dicts.append(graph_dict)

        return web.json_response(
            graph_dicts, content_type="application/json", status=200
        )


    except Exception as e:

        print("\nGRAPH GET ALL ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Unable to get all graphs"},
            content_type="text/html",
            status=400
        )

async def graph_op_dependency_get(request):
    # http://localhost:9999/graph/op_dependency/get/?id=1
    """
    params  = id(graph_id) : int
    returns = list(subgraphs associated with a graph)
    """

    try:
        graph_id = request.rel_url.query["id"]
        op_dependency, min_op_id = ravdb.get_graph_op_dependency(graph_id=graph_id)

        return web.json_response(
            {'op_dependency': op_dependency, 'min_op_id': min_op_id},
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nGRAPH SUBGRAPH GET ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Graph id"}, content_type="text/html", status=400
        )


async def graph_op_get(request):
    # http://localhost:9999/graph/op/get/?id=1
    """
    params  = id(graph_id) : int
    returns = list(ops associate with a graph)
    """

    try:
        graph_id = request.rel_url.query["id"]
        ops = ravdb.get_graph_ops(graph_id=graph_id)

        ops_dicts = []

        for op in ops:
            op_dict = serialize(op)
            # Remove datetime key
            del op_dict["created_at"]
            ops_dicts.append(op_dict)

        return web.json_response(
            ops_dicts,
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nGRAPH OP GET ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Graph id"}, content_type="text/html", status=400
        )


async def graph_op_name_get(request):
    # http://localhost:9999/graph/op/name/get/?op_name=""&id=1

    try:
        op_name = request.rel_url.query["op_name"]
        graph_id = request.rel_url.query["id"]
        ops = ravdb.get_ops_by_name(op_name=op_name, graph_id=graph_id)

        ops_dicts = []

        for op in ops:
            op_dict = serialize(op)
            # Remove datetime key
            del op_dict["created_at"]
            ops_dicts.append(op_dict)

        return web.json_response(
            ops_dicts,
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )
    except Exception as e:

        print("\nGRAPH OP NAME GET ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid op_name or graph id"},
            content_type="text/html",
            status=400
        )

async def graph_op_get_stats(request):
    # http://localhost:9999/graph/op/get/stats/?id=4
    """
    Get stats of all ops
    params  = id(graph_id) : int
    """

    try:
        graph_id = request.rel_url.query["id"]
        ops = ravdb.get_graph_ops(graph_id=graph_id)
        stats = get_op_stats(ops)

        return web.json_response(
            stats,
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nGRAPH STATS ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Graph id"}, content_type="text/html", status=400
        )


async def graph_get_progress(request):
    # http://localhost:9999/graph/op/get/progress/?id=4

    """
    Get Graph Ops Progress
    params  = id(graph_id) : int
    """

    try:

        graph_id = request.rel_url.query["id"]
        ops = ravdb.get_graph_ops(graph_id=graph_id)
        stats = get_op_stats(ops)

        if stats["total_ops"] == 0:
            return 0
        progress = (
            (stats["computed_ops"])
            / stats["total_ops"]
        ) * 100

        return web.json_response(
            {"progress": progress},
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nGRAPH STATS ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Graph id"}, content_type="text/html", status=400
        )


async def graph_end(request):
    # http://localhost:9999/graph/end/?id=4

    """
    Updates Graph Status after algorithm is finished running.
    params  = id(graph_id) : int
    """

    try:
        graph_id = request.rel_url.query["id"]
        await final_scheduler_call(graph_id)
        graph = ravdb.get_graph(graph_id)
        ops = ravdb.get_graph_ops(graph_id=graph_id)
        stats = get_op_stats(ops)

        if stats["total_ops"] == 0:
            return 0
        progress = (
                           (stats["computed_ops"])
                           / stats["total_ops"]
                   ) * 100

        message = ''
        if progress == 100:
            message = 'Graph computed successfully!'
        else:
            ravdb.update_graph(graph, status='failed')
            message = 'Graph failed to compute.'

        return web.json_response(
            {"message": message},
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nGRAPH END ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Graph id"}, content_type="text/html", status=400
        )


async def graph_delete(request):
    # http://localhost:9999/graph/delete/?id=1
    """
    params  = id(graph_id) : int
    """

    try:
        graph_id = request.rel_url.query["id"]
        graph_obj = ravdb.get_graph(graph_id=graph_id)
        ravdb.delete(graph_obj)
        data = {
            "graph_id": graph_id,
            "message": "Graph has been deleted successfully",
        }

        return web.json_response(
            data,
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nGRAPH OP DELETE ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Invalid Graph id"}, content_type="text/html", status=400
        )


# SUBGRAPH ENDPOINTS

async def post_subgraph_op_mappings(request):
    # http://localhost:9999/subgraph/op/mappings
    try:
        d = await request.json()
        # To arrange all op_ids in ascending order
        data = {}
        for i in d:
            data[int(i)] = d[i]

        for subgraph_id in data:
            data[subgraph_id].sort()

        print('\nSubgraph_Op_Mappings : ',data, type(data))

        graph_id = ravdb.get_last_graph_id()-1
        print('\nLATEST GRAPH ID : ', graph_id)
        for subgraph_id in data:
            complexity = 0
            for op_id in data[subgraph_id]:
                op = ravdb.get_op(op_id=op_id)
                if op.status == "pending":
                    complexity += op.complexity
            subgraph = ravdb.create_subgraph(graph_id=int(graph_id),op_ids=str(data[subgraph_id]),complexity=complexity)


        for subgraph_id in data:
            for op_id in data[subgraph_id]:
                op = ravdb.get_op(op_id=op_id)
                ravdb.update_op(op, subgraph_id=subgraph_id)

        ravdb.initialize_subgraph_complexities_list(data)

        return web.json_response(
            {"message": "Subgraph op mappings updated successfully"},
            text=None,
            body=None,
            status=200,
            reason=None,
            headers=None,
            content_type="application/json",
            dumps=json.dumps
        )

    except Exception as e:

        print("\nSUBGRAPH OP MAPPING ENDPOINT ERROR : ", str(e))

        return web.json_response(
            {"message": "Unable to create SubGraph-Op-Mappings"}, content_type="text/html", status=400
        )


async def get_ftp_credentials(request):
    """
    Get ftp credentials for a particular client
    :param request: aiohttp request object
    :return: client ftp credentials
    """
    try:
        cid = request.rel_url.query["cid"]
        if cid is None:
            return web.json_response(
                {"message": "CID is missing. Pass a valid CID"}, content_type="text/html", status=400
            )

        client = ravdb.get_client_by_cid(cid=cid)
        if client is None:
            return web.json_response(
                {"message": "Invalid CID"}, content_type="text/html", status=400
            )

        return web.json_response({"ftp_credentials": client.ftp_credentials, "context_filename": client.context},
                                 content_type="application/json", status=200)

    except Exception as e:
        return web.json_response(
            {"message": "Error:{}".format(str(e))}, content_type="text/html", status=400
        )


async def update_graph_client_mapping(request):
    try:
        pass
    except Exception as e:
        return web.json_response(
            {"message": "Error:{}".format(str(e))}, content_type="text/html", status=400
        )


async def subgraph_ops_get(request):
    try:
        graph_id = request.rel_url.query['graph_id']
        client = ravdb.get_client_by_cid(request.rel_url.query['cid'])

        if graph_id is not None and client is not None:
            subgraphs = ravdb.get_subgraphs_from_graph(graph_id=graph_id)
            subgraph_ops = []
            for subgraph in subgraphs:
                op_ids = ast.literal_eval(subgraph.op_ids)
                ops = [db_model_to_dict(ravdb.get_op(op_id=op_id)) for op_id in op_ids]

                subgraph_ops.append({"id": subgraph.id, "ops": ops})

                if ravdb.find_subgraph_client_mapping(subgraph_id=subgraph.id, client_id=client.id) is None:
                    ravdb.create_subgraph_client_mapping(subgraph_id=subgraph.id, client_id=client.id)

            return web.json_response({
                "subgraph_ops": subgraph_ops
            }, content_type="application/json", status=200)
        else:
            return web.json_response(
                {"message": "Error: Graph id and client id are required"}, content_type="text/html", status=400
            )
    except Exception as e:
        return web.json_response(
            {"message": "Error:{}".format(str(e))}, content_type="text/html", status=400
        )

async def update_global_subgraph_id(request):
    '''
        /global/subgraph/update/id/?graph_id=1
    '''
    try:
        graph_id = request.rel_url.query['graph_id']
        max_subgraph_id = len(ravdb.get_all_subgraphs(graph_id=graph_id))

        if graph_id is not None and max_subgraph_id is not None:

            return web.json_response({
                "global_subgraph_id": max_subgraph_id
            }, content_type="application/json", status=200)

        else:
            return web.json_response(
                {"message": "Error: Graph id is required"}, content_type="text/html", status=400
            )
    except Exception as e:
        return web.json_response(
            {"message": "Error:{}".format(str(e))}, content_type="text/html", status=400
        )

# ------------- Ravop User Creation Endpoint for FTP --------------------- #

async def add_developer(request):
    '''
        /ravop/developer/add/?token=1
    '''
    username = request.rel_url.query['token']
    password = get_random_string(10)
    print("Password:", password)
    if FTP_SERVER_LOCATION == "LOCAL":
        print("Creating...")

        process = subprocess.Popen(
            ["{}/python".format(FTP_ENVIRON_DIR), "{}/add_user.py".format(FTP_SERVER_DIR), "--username", username,
             "--password", password], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print("Std out for popen: ", stdout)
        process.terminate()

        endpoint = f"add_user?username={username}&password={password}"
        requests.get("{}{}".format(FLASK_SERVER_URL, endpoint))

        # subprocess.Popen(
        #     ["sh", "{}/restart_server.sh".format(FTP_SERVER_DIR), FTP_ENVIRON_DIR, FTP_SERVER_DIR],
        #     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # print('FTP Server Restarted.')


        
    return web.json_response(
        {
            "message":"FTP developer creds created",
            "username":username,
            "password": password
        }, content_type="application/json", status=200
    )