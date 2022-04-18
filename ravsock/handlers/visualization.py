import jinja2
import ast
from ..db import ravdb
import aiohttp_jinja2
from ..utils import (
    dump_data,
    copy_data,
    convert_to_ndarray,
    load_data_from_file,
    convert_ndarray_to_str,
    find_dtype,
    get_op_stats,
    serialize,
)

# --- Home ---


@aiohttp_jinja2.template("viz.html")
async def viz(request):

    return {"title": "Visualize"}


# --- Client ---


@aiohttp_jinja2.template("clients.html")
async def viz_clients(request):
    clients = ravdb.get_all_clients()
    clients_dicts = []
    for client in clients:
        clients_dict = serialize(client)
        clients_dicts.append(clients_dict)

    return {"clients_dicts": clients_dicts, "title": "Clients"}


# --- Data ---


@aiohttp_jinja2.template("data_viewer.html")
async def viz_data(request):

    data_id = request.match_info["data_id"]
    data = ravdb.get_data(data_id=data_id)

    if data is not None:
        data_dict = serialize(data)
        if data.file_path is not None:
            data_dict["value"] = load_data_from_file(data.file_path).tolist()
        return {"data_dict": data_dict, "title": "Data"}
    else:
        return {"data_dict": None, "title": "Data"}


# --- OPS ---


@aiohttp_jinja2.template("ops.html")
async def viz_ops(request):
    ops = ravdb.get_all_ops()
    ops_dicts = []
    for op in ops:
        op_dict = serialize(op)
        if op_dict["inputs"] != "null" and op_dict["inputs"] is not None:
            op_dict["inputs"] = ast.literal_eval(op_dict["inputs"])
        else:
            op_dict["inputs"] = None

        if op_dict["outputs"] != "null" and op_dict["outputs"] is not None:
            op_dict["outputs"] = ast.literal_eval(op_dict["outputs"])
        else:
            op_dict["outputs"] = None

        ops_dicts.append(op_dict)

    return {"ops_dicts": ops_dicts, "title": "Ops"}


@aiohttp_jinja2.template("ops_viewer.html")
async def viz_op_view(request):
    op_id = request.match_info["op_id"]
    op = ravdb.get_op(op_id)
    op_dict = serialize(op)
    if op is not None:
        if op_dict["inputs"] != "null" and op_dict["inputs"] is not None:
            op_dict["inputs"] = ast.literal_eval(op_dict["inputs"])
        else:
            op_dict["inputs"] = None

        if op_dict["outputs"] != "null" and op_dict["outputs"] is not None:
            op_dict["outputs"] = ast.literal_eval(op_dict["outputs"])
        else:
            op_dict["outputs"] = None
        return {"op_dict": op_dict, "title": "Ops"}
    else:
        return {"op_dict": None, "title": "Ops"}


# --- Graph ---


@aiohttp_jinja2.template("graphs.html")
async def viz_graphs(request):

    graph_objs = ravdb.get_all_graphs()
    graph_dicts = []

    for graph in graph_objs:
        graph_dict = serialize(graph)
        graph_dicts.append(graph_dict)

    return {"graph_dicts": graph_dicts, "title": "Graph"}


@aiohttp_jinja2.template("graph_ops.html")
async def viz_graph_ops(request):

    graph_id = request.match_info["graph_id"]
    ops = ravdb.get_graph_ops(graph_id=graph_id)
    ops_dicts = []

    for op in ops:
        op_dict = serialize(op)
        if op_dict["inputs"] != "null" and op_dict["inputs"] is not None:
            op_dict["inputs"] = ast.literal_eval(op_dict["inputs"])
        else:
            op_dict["inputs"] = None

        if op_dict["outputs"] != "null" and op_dict["outputs"] is not None:
            op_dict["outputs"] = ast.literal_eval(op_dict["outputs"])
        else:
            op_dict["outputs"] = None
        ops_dicts.append(op_dict)

    return {"ops_dicts": ops_dicts, "title": "Graph"}


@aiohttp_jinja2.template("graph_viewer.html")
async def viz_graph_view(request):

    graph_id = request.match_info["graph_id"]
    graph = ravdb.get_graph(graph_id=graph_id)

    if graph is not None:
        graph_dict = serialize(graph)
        return {"graph_dict": graph_dict, "title": "Graph"}
    else:
        return {"graph_dict": None, "title": "Graph"}
