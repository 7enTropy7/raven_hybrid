import aiohttp_cors

from .events import disconnect, create_credentials, between_callback, op_completed, get_op, check_callback, subgraph_completed
from .events.benchmarking import benchmark_callback
from .events.connection import connect
from .events.manage_outputs import receive_params
from .events.scheduler import run_scheduler
from .handlers import *
from .globals import globals as g

sio = g.sio
app = g.app
cors = g.cors

"""
Socket events
"""

# Connection
sio.on("connect", namespace="/", handler=connect)
sio.on("connect", namespace="/client", handler=connect)
sio.on("disconnect", handler=disconnect)
sio.on("between_callback", handler=between_callback)
sio.on("create_credentials", handler=create_credentials)

# Benchmarking
sio.on("benchmark_callback", namespace="/client", handler=benchmark_callback)

# Output handler
sio.on("op_completed", namespace="/", handler=op_completed)
sio.on("op_completed", namespace="/client", handler=op_completed)

sio.on("subgraph_completed", namespace="/", handler=subgraph_completed)
sio.on("subgraph_completed", namespace="/client", handler=subgraph_completed)

sio.on("params", namespace="/client", handler=receive_params)
sio.on("scheduler", namespace="/client", handler=run_scheduler)

sio.on("get_op", namespace="/", handler=get_op)
sio.on("get_op", namespace="/client", handler=get_op)

# Client Status check handler
sio.on("check_callback", namespace="/", handler=check_callback)
sio.on("check_callback", namespace="/client", handler=check_callback)

"""
Add routes and resources
"""

# GET: Get all graphs
# get_all_graphs_resource = cors.add(app.router.add_resource("/graph/get/all/"))
# cors.add(get_all_graphs_resource.add_route("GET", graph_get_all))

# GET: Benchmark file
# get_benchmark_file_resource = cors.add(app.router.add_resource("/ravenjs/get/benchmark/"))
# cors.add(get_benchmark_file_resource.add_route("GET", get_benchmark_ops))

# Worker endpoint
app.router.add_get("/", load_worker)

app.router.add_get("/graph/get/all/", graph_get_all)
app.router.add_get("/ravenjs/get/benchmark/", get_benchmark_ops)

# OPS web endpoints
app.router.add_post("/op/create/", op_create)
app.router.add_get("/op/get/", op_get)
app.router.add_get("/op/get/name/", op_get_by_name)
app.router.add_get("/op/get/all/", op_get_all)
app.router.add_get("/op/status/", op_status)
app.router.add_get("/op/delete/", op_delete)

app.router.add_get("/op/get_my_graph_id/", op_get_my_graph_id)

# Data web endpoints
app.router.add_post("/data/create/", data_create)
app.router.add_get("/data/get/", data_get)
app.router.add_get("/data/get/data/", data_get_data)
app.router.add_get("/data/delete/", data_delete)

# Graph web endpoints
app.router.add_post("/graph/create/", graph_create)
app.router.add_get("/graph/get/", graph_get)
app.router.add_get("/graph/get/graph_id", graph_get_last_graph_id)
app.router.add_get("/graph/op/get/", graph_op_get)
app.router.add_get("/graph/op/name/get/", graph_op_name_get)
app.router.add_get("/graph/op/get/stats/", graph_op_get_stats)
app.router.add_get("/graph/op/get/progress/", graph_get_progress)
app.router.add_get("/graph/delete/", graph_delete)
app.router.add_get("/graph/end/", graph_end)
app.router.add_get("/graph/op_dependency/get/", graph_op_dependency_get)

# Subgraph web endpoints
app.router.add_post("/subgraph/op/mappings/", post_subgraph_op_mappings)
app.router.add_get("/subgraph/ops/get/", subgraph_ops_get)

app.router.add_get("/global/subgraph/update/id/", update_global_subgraph_id)

"""
Visualization
"""
app.router.add_get("/viz/", viz)
app.router.add_get("/viz/data/{data_id}/", viz_data)
app.router.add_get("/viz/clients/", viz_clients)

app.router.add_get("/viz/ops/", viz_ops)
app.router.add_get("/viz/ops/{op_id}/", viz_op_view)

app.router.add_get("/viz/graphs/", viz_graphs)
app.router.add_get("/viz/graph/ops/{graph_id}/", viz_graph_ops)
app.router.add_get("/viz/graph/{graph_id}/", viz_graph_view)

# Client related endpoints
app.router.add_get("/client/ftp_credentials/", get_ftp_credentials)

# Ravop create developer endpoint
app.router.add_get("/ravop/developer/add/", add_developer)

#Subgraph completed
app.router.add_post("/subgraph/completed/", subgraph_completed_request)