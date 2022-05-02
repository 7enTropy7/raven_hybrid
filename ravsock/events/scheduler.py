import ast
from concurrent.futures import thread
import datetime
import json
import random
import os
import shutil
import threading
import networkx as nx
from ravop import GraphStatus, OpStatus

from ravsock.config import QUEUE_OPS, QUEUE_CLIENTS, SCHEDULER_RUNNING
from ravsock.db import ravdb, RavQueue
from ravsock.utils import functions, get_logger, SubgraphStatus, ClientStatus
from ..globals import globals as g
from ..config import FTP_RAVOP_FILES_PATH

sio = g.sio
Queue = g.Queue

queue_ops = RavQueue(name=QUEUE_OPS)
queue_clients = RavQueue(name=QUEUE_CLIENTS)

logger = get_logger()

def get_pending_graphs():
    graphs = ravdb.get_graphs(status=GraphStatus.PENDING)
    return graphs


def create_sub_graphs(graph_id):
    op_dependency = ravdb.get_graph_op_dependency(graph_id)
    # print('OP DEPENDENCY: ',op_dependency)
    for subgraph_id in op_dependency:
        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)

        if subgraph is not None:
            complexity = calculate_subgraph_complexity(subgraph=subgraph)
            ravdb.update_subgraph(subgraph, subgraph_id=subgraph_id, graph_id=graph_id,
                                  op_ids=str(op_dependency[subgraph_id]),
                                  complexity=complexity)
        else:
            subgraph = ravdb.create_subgraph(subgraph_id=subgraph_id, graph_id=graph_id,
                                             op_ids=str(op_dependency[subgraph_id]), status=SubgraphStatus.READY)
            complexity = calculate_subgraph_complexity(subgraph=subgraph)
            ravdb.update_subgraph(subgraph, complexity=complexity)

async def vertical_split(graph_id):
    op_dependency = ravdb.get_graph_op_dependency(graph_id)

    # print('OP DEPENDENCY: ',op_dependency)

    for subgraph_id in op_dependency:
        op_ids = op_dependency[subgraph_id]
        for op_id in op_ids:
            op = ravdb.get_op(op_id)
            ravdb.update_op(op,subgraph_id=subgraph_id)

        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id) 
        
        subgraph_ops = ravdb.get_subgraph_ops(graph_id=graph_id,subgraph_id=subgraph_id)
        subgraph_op_ids = []
        for subgraph_op in subgraph_ops:
            subgraph_op_ids.append(subgraph_op.id)
        subgraph_op_ids.sort()
        
        if subgraph is None: 
            subgraph = ravdb.create_subgraph(subgraph_id=subgraph_id, graph_id=graph_id,
                                            op_ids=str(subgraph_op_ids), status=SubgraphStatus.READY)
        else:
            if subgraph.status != 'failed':
                if subgraph.status == 'standby':
                    parent_subgraph = ravdb.get_subgraph(subgraph_id=subgraph.parent_subgraph_id, graph_id=graph_id)
                    # if parent_subgraph.status == SubgraphStatus.COMPUTED:# or parent_subgraph.status == SubgraphStatus.COMPUTING:
                    #     ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids), status='not_ready')
                    standby_ops_ids = ast.literal_eval(subgraph.op_ids)
                    standby_flag = False
                    for standby_op_id in standby_ops_ids:
                        standby_op = ravdb.get_op(standby_op_id)
                        if standby_op.inputs != 'null':
                            standby_op_inputs = ast.literal_eval(standby_op.inputs)                        
                            for standby_op_input_id in standby_op_inputs:
                                standby_op_input = ravdb.get_op(standby_op_input_id)
                                if standby_op_input.status != 'computed' and standby_op_input.subgraph_id != subgraph_id:
                                    standby_flag = True
                                    break
                            if standby_flag:
                                break
                    if not standby_flag or parent_subgraph.status == SubgraphStatus.COMPUTED:
                        ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids), status='not_ready')
                else:
                    op_ids = subgraph.op_ids
                    if len(subgraph_op_ids) == 0:
                        ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids), status='computed', optimized='True')
                        assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                        if assigned_client is not None:
                            ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
                    else:
                        if subgraph.status != 'assigned' and subgraph.status != 'computing':
                            ravdb.update_subgraph(subgraph, op_ids=str(subgraph_op_ids))

    last_id = len(ravdb.get_all_subgraphs(graph_id=graph_id))
    if last_id == 0:
        last_id = 1
    new_op_dependency = {}
    for subgraph_id in op_dependency:
        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)
        if subgraph is not None and subgraph.status != 'standby' and subgraph.status != 'computed' and subgraph.status != 'computing':
            if subgraph.optimized == "False":
                computed_ops = []
                G = nx.DiGraph()

                op_ids = ast.literal_eval(subgraph.op_ids)

                for op_id in op_ids:
                    op = ravdb.get_op(op_id)
                    if op.inputs != "null":
                        for input_id in ast.literal_eval(op.inputs):
                            input_op = ravdb.get_op(input_id)
                            if input_id in computed_ops:
                                name = "ghost_op_"+str(input_id)
                                ghost_op = ravdb.create_op(name=name,graph_id=input_op.graph_id,subgraph_id=subgraph_id,
                                                            complexity=input_op.complexity,output_dims=input_op.output_dims,inputs="null",
                                                            outputs=input_op.outputs,node_type="input",op_type="other",
                                                            operator="lin",status="computed",params=input_op.params)
                                op_inputs = ast.literal_eval(op.inputs)
                                for j in range(len(op_inputs)):
                                    if op_inputs[j] == input_id:
                                        op_inputs[j] = ghost_op.id
                                ravdb.update_op(op,inputs=str(op_inputs))
                            else:
                                if input_op.status == "computed":
                                    computed_ops.append(input_id)

                for op_id in op_ids:
                    op = ravdb.get_op(op_id)
                    if op.inputs != "null":
                        for input_id in ast.literal_eval(op.inputs):    
                            G.add_edge(input_id, op_id)

                subsubgraphs = list(nx.weakly_connected_components(G))
                subsubgraphs = [list(x) for x in subsubgraphs]

                if len(subsubgraphs) > 1:
                        new_op_dependency[subgraph_id] = subsubgraphs[0]
                        for i in range(1,len(subsubgraphs)):
                            new_op_dependency[last_id + i] = subsubgraphs[i]
                elif len(subsubgraphs) == 1:
                    new_op_dependency[subgraph_id] = subsubgraphs[0]

                if len(new_op_dependency) != 0:
                    last_id = list(new_op_dependency.keys())[-1]

    # print('\nNEW OP DEPENDENCY: ',new_op_dependency)    
    for subgraph_id in new_op_dependency:
        op_ids = new_op_dependency[subgraph_id]
        for k in range(len(op_ids)):
            op = ravdb.get_op(op_ids[k])
            ravdb.update_op(op,subgraph_id=subgraph_id)

        # for subgraph_id in new_op_dependency:
        subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)
        
        # sorted_new_op_deps = new_op_dependency[subgraph_id]
        sorted_new_op_deps = op_ids

        sorted_new_op_deps.sort()
        if subgraph is not None:
            complexity = calculate_subgraph_complexity(subgraph=subgraph)
            ravdb.update_subgraph(subgraph, subgraph_id=subgraph_id, graph_id=graph_id,
                                  op_ids=str(sorted_new_op_deps),
                                  complexity=complexity, optimized="True", status=SubgraphStatus.READY)
        else:
            subgraph = ravdb.create_subgraph(subgraph_id=subgraph_id, graph_id=graph_id,
                                             op_ids=str(sorted_new_op_deps), status=SubgraphStatus.READY, optimized="True")
            complexity = calculate_subgraph_complexity(subgraph=subgraph)
            ravdb.update_subgraph(subgraph, complexity=complexity)



async def horizontal_split(graph_id, minimum_split_size=50):
    # subgraphs = ravdb.get_all_subgraphs(graph_id=graph_id)
    subgraphs = ravdb.get_horizontal_split_subgraphs(graph_id=graph_id)
    for subgraph in subgraphs:
        if subgraph.has_failed == "False" and int(subgraph.retry_attempts) <= 1:# and subgraph.status != SubgraphStatus.COMPUTED and subgraph.status != 'standby' and subgraph.status != 'failed' and subgraph.status != 'computing':
            op_ids = ast.literal_eval(subgraph.op_ids)
            if len(op_ids) > minimum_split_size:
                row1 = op_ids[:minimum_split_size]

                parent_subgraph = ravdb.get_subgraph(subgraph_id=subgraph.parent_subgraph_id, graph_id=graph_id)
                if parent_subgraph is not None:
                    if str(row1) == str(parent_subgraph.op_ids):
                        minimum_split_size += random.randint(1,len(op_ids)-minimum_split_size)
                        row1 = op_ids[:minimum_split_size]

                row2 = op_ids[minimum_split_size:]
                ravdb.update_subgraph(subgraph, op_ids=str(row1))
                last_subgraph_id = len(ravdb.get_all_subgraphs(graph_id=graph_id))
                if len(row2) > 0:
                    new_subgraph = ravdb.create_subgraph(subgraph_id=last_subgraph_id + 1, graph_id=graph_id,
                                        optimized="False", op_ids=str(row2), status="standby", parent_subgraph_id=subgraph.subgraph_id)
                    for op_id in row2:
                        op = ravdb.get_op(op_id)
                        ravdb.update_op(op, subgraph_id=new_subgraph.subgraph_id)

async def retry_failed_subgraphs(graph_id):
    global Queue
    graph = ravdb.get_graph(graph_id)
    failed_subgraph_ids = ravdb.get_failed_subgraphs_from_graph(graph)
    if len(failed_subgraph_ids)>0:
        print("\nFailed subgraph ids Retry: ",failed_subgraph_ids)
        for failed_subgraph_id in failed_subgraph_ids:
            failed_subgraph = ravdb.get_subgraph(subgraph_id=failed_subgraph_id, graph_id=graph_id)
            assigned_client = ravdb.get_assigned_client(failed_subgraph.subgraph_id, failed_subgraph.graph_id)
            if assigned_client is not None:
                ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)

            retries = failed_subgraph.retry_attempts

            op_ids = ast.literal_eval(failed_subgraph.op_ids)
            if int(retries) <= 5:                
                failed_combination = (failed_subgraph.subgraph_id,failed_subgraph.graph_id)
                if failed_combination not in Queue:
                    Queue.append(failed_combination)

            else:
                ravdb.update_subgraph(failed_subgraph, status='failed')
                ravdb.update_graph(graph, status="failed")
                for op_id in op_ids:
                    failed_op = ravdb.get_op(op_id)
                    ravdb.update_op(failed_op,status="failed")


def get_failed_subgraphs_from_queue(graph_id):
    global Queue
    failed_subgraph_ids = []
    for failed_subgraph_id, failed_graph_id in Queue:
        if failed_graph_id == graph_id:
            failed_subgraph_ids.append(failed_subgraph_id)
    return failed_subgraph_ids

def calculate_subgraph_complexity(subgraph):
    op_ids = ast.literal_eval(subgraph.op_ids)
    pending_ops = []
    for op in op_ids:
        op_obj = ravdb.get_op(op)
        if op_obj.status == "pending":
            pending_ops.append(op_obj)
    subgraph_complexity = 0
    for pending_op in pending_ops:
        subgraph_complexity += pending_op.complexity
    return subgraph_complexity


async def emit_op(sid, op=None):
    """
    1. Find an op
    2. Create payload
    3. Emit Op
    """

    client = ravdb.get_client_by_sid(sid)
    if client is not None:
        subgraph_id = client.current_subgraph_id
        graph_id = client.current_graph_id
        if subgraph_id is not None and graph_id is not None:
            subgraph = ravdb.get_subgraph(subgraph_id, graph_id)
            if subgraph.status == 'assigned':
                payloads = []
                ready_flag = True
                for op_id in ast.literal_eval(subgraph.op_ids):
                    op = ravdb.get_op(op_id)
                    if op.status == OpStatus.PENDING:
                        inputs = ast.literal_eval(op.inputs)
                        if inputs is not None:
                            for input_op_id in inputs:
                                input_op = ravdb.get_op(input_op_id)
                                if input_op_id not in ast.literal_eval(subgraph.op_ids):
                                    if ravdb.get_op_readiness(input_op) == "not_ready":
                                        ready_flag = False
                                        break
                        if not ready_flag:
                            break

                if ready_flag:
                    appended_ops = []
                    for op_id in ast.literal_eval(subgraph.op_ids):
                        op = ravdb.get_op(op_id)
                        if op is not None:
                            if op.status == "pending" and op.name is None:
                                payloads.append(create_payload(op, client.cid))
                                appended_ops.append(op_id)

                    # if subgraph.has_failed == "True":
                    #     print('Sending Payload: ', payloads)
                    logger.debug("Emitting Subgraph:{}, {}".format(sid, payloads))

                    await sio.emit("subgraph", payloads, namespace="/client", room=sid)
                    print("\n Emitted subgraph: ", subgraph_id)
                    
                    ravdb.update_graph(ravdb.get_graph(graph_id), inactivity=0)

                    ravdb.update_subgraph(subgraph, status=SubgraphStatus.COMPUTING, retry_attempts = subgraph.retry_attempts + 1)

                    for op_id in appended_ops:
                        ravop = ravdb.get_op(op_id)
                        if ravop is not None:
                            if ravop.status == "pending":
                                ravdb.update_op(ravop, status=OpStatus.COMPUTING)

                else:
                    print("\n\nSubgraph not ready")
                    await sio.sleep(0.1)
                    await emit_op(sid)

def copy_file(src, dest):
    try:
        shutil.copy(src, dest)
    except:
        print("Error copying file")

def create_payload(op, cid):
    """
    Create payload for the operation
    params:
    op: database op
    """
    values = []
    inputs = json.loads(op.inputs)

    for op_id in inputs:

        # output = ravdb.get_op_output(op_id)
        # if output is not None:
        input_op = ravdb.get_op(op_id)
        if input_op.status == 'computed':
            # value = {"value": output.tolist()}
            data_id = ast.literal_eval(input_op.outputs)[0]
            op_data_obj = ravdb.get_data(data_id)
            
            if op_data_obj.file_size is not None:
                output = ravdb.get_op_output(op_id)
                if output is not None:
                    value = {"value": output.tolist()}
                else:
                    value = {"op_id": op_id}

            else:
                op_data_path = op_data_obj.file_path

                src = op_data_path
                dst = FTP_RAVOP_FILES_PATH + '/' + str(cid) + '/' + os.path.basename(op_data_path)
                to_delete = False
                if src != dst:
                    copy_thread = threading.Thread(target=copy_file, args=(src, dst))
                    copy_thread.start()
                    to_delete = True

                file_path_list = op_data_path.split('/')[-2:]
                file_path = '/'.join(file_path_list)
                value = {"value": op_id, "path": '/'+file_path, "to_delete": str(to_delete)}
        else:
            value = {"op_id": op_id}
        values.append(value)

    payload = dict()
    payload["op_id"] = op.id
    payload["values"] = values
    payload["op_type"] = op.op_type
    payload["operator"] = functions[op.operator]

    params = dict()
    for key, value in json.loads(op.params).items():
        if type(value).__name__ == "int":
            op1 = ravdb.get_op_output(value)
            params[key] = op1.tolist()
        elif type(value).__name__ == "str":
            params[key] = value

    payload["params"] = params

    return payload


async def update_client_status():
    while True:
        clients = ravdb.get_clients(status='connected')
        for client in clients:
            if (datetime.datetime.utcnow() - client.last_active_time).seconds > 200: # To be reduced.
                ravdb.update_client(client, status="disconnected", reporting='ready', disconnected_at=datetime.datetime.utcnow())
                assigned_subgraph = ravdb.get_subgraph(client.current_subgraph_id, client.current_graph_id)
                if assigned_subgraph is not None:
                    ravdb.update_subgraph(assigned_subgraph, status="ready")

            client_type = "/{}".format(client.type)
            await sio.emit("check_status",
                            {"sid": client.sid},
                            namespace=client_type,
                            room=client.sid,
                            )

        await sio.sleep(5)


async def check_callback(sid, data):
    client = ravdb.get_client_by_sid(sid=data["sid"])
    ravdb.update_client(client, status="connected", last_active_time=datetime.datetime.utcnow())

async def final_scheduler_call(graph_id):
    await sio.sleep(1)
    distributed_graph = ravdb.get_graph(graph_id=graph_id)
    subgraphs = ravdb.get_ready_subgraphs_from_graph(graph_id=graph_id)

    for subgraph in subgraphs:
        idle_clients = ravdb.get_idle_clients(reporting=ClientStatus.IDLE)
        if idle_clients is not None:
            op_ids = ast.literal_eval(subgraph.op_ids)
            prelim_times = {}
            for idle_client in idle_clients:
                idle_client_time = 0
                for op_id in op_ids:
                    op = ravdb.get_op(op_id=op_id)
                    if op is not None:
                        operator = op.operator
                        capabilities_dict = ast.literal_eval(idle_client.capabilities)
                        if operator not in capabilities_dict.keys():
                            client_time = random.random()*10
                        else:
                            client_time = capabilities_dict[operator]
                        idle_client_time += client_time
                prelim_times[idle_client.id] = idle_client_time
            if bool(prelim_times):
                fastest_client_id = min(prelim_times, key=prelim_times.get)
                client = ravdb.get_client(id=fastest_client_id)
                ravdb.update_subgraph(subgraph, status='assigned')
                ravdb.update_client(client, reporting='busy', current_subgraph_id=subgraph.subgraph_id,
                                    current_graph_id=subgraph.graph_id)

            else:
                print('\n\nNo idle clients')
        else:
            print('\nNo idle clients')

        if subgraph.status != 'computed':
            subgraph_op_ids = ast.literal_eval(subgraph.op_ids)
            actual_op_ids = []
            for op_id in subgraph_op_ids:
                op = ravdb.get_op(op_id)
                if op.subgraph_id == subgraph.subgraph_id:
                    actual_op_ids.append(op)
                                                
            num_ops = len(actual_op_ids)
            counter = {'pending': 0, 'computed': 0, 'failed': 0, 'computing': 0}
            for subgraph_op in actual_op_ids:
                if subgraph_op.status == "pending":
                    counter['pending'] += 1
                elif subgraph_op.status == "computed":
                    counter['computed'] += 1
                elif subgraph_op.status == "failed":
                    counter['failed'] += 1
                elif subgraph_op.status == "computing":
                    counter['computing'] += 1
            if counter['computed'] == num_ops:
                ravdb.update_subgraph(subgraph, status="computed")
                assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                if assigned_client is not None:
                    ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
            elif counter['failed'] > 0:
                ravdb.update_subgraph(subgraph, status="failed")
                assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                if assigned_client is not None:
                    ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
           

    graph_completed= True
    for check_subgraph in subgraphs:
        if check_subgraph.status != "computed":
            graph_completed = False
    
    if graph_completed:
        ravdb.update_graph(distributed_graph, status='computed')
    
    


async def run_scheduler():
    global SCHEDULER_RUNNING, Queue
    SCHEDULER_RUNNING = True
    while True:
        print("Scheduler Running...")
        distributed_graphs = ravdb.get_graphs(status=GraphStatus.PENDING, approach="distributed")
        federated_graphs = ravdb.get_graphs(status=GraphStatus.PENDING, approach="federated")

        if len(distributed_graphs) == 0 and len(federated_graphs) == 0:
            print("No graphs found")

        else:
            for federated_graph in federated_graphs:
                create_sub_graphs(federated_graph.id)

            for distributed_graph in distributed_graphs:
                current_graph_id = distributed_graph.id

                ravdb.update_graph(distributed_graph, inactivity = distributed_graph.inactivity + 1)
                
                ready_subgraphs = ravdb.get_ready_subgraphs_from_graph(graph_id=current_graph_id)

                if len(ready_subgraphs)>=1:
                    # dead_subgraph = ravdb.get_first_ready_subgraph_from_graph(graph_id=current_graph_id)
                    dead_subgraph = ready_subgraphs[0]
                    if dead_subgraph is not None:
                        ravdb.update_subgraph(dead_subgraph, optimized="False")

                if distributed_graph.inactivity >= 100:
                    # dead_subgraph = ravdb.get_first_ready_subgraph_from_graph(graph_id=current_graph_id)
                    dead_subgraph = ready_subgraphs[0]
                    if dead_subgraph is not None:
                        ravdb.update_subgraph(dead_subgraph, optimized="False")
                        ravdb.update_graph(distributed_graph, inactivity = 0)

                # ready_subgraphs = ravdb.get_ready_subgraphs_from_graph(graph_id=current_graph_id)
                # not_ready_subgraphs = ravdb.get_not_ready_subgraphs_from_graph(graph_id=current_graph_id)

                # if len(ready_subgraphs) == 0 or len(not_ready_subgraphs)>=0:
                await vertical_split(distributed_graph.id)
                await sio.sleep(0.1)

                await horizontal_split(distributed_graph.id)
                await sio.sleep(0.1)

                await retry_failed_subgraphs(distributed_graph.id)
                await sio.sleep(0.1)
                
                failed_subgraph_ids = get_failed_subgraphs_from_queue(current_graph_id)
                
                for subgraph_id in failed_subgraph_ids:
                    subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=current_graph_id)
                    
                    if subgraph.status != "assigned" and subgraph.status != "computing" and subgraph.status != "computed": 
                        if subgraph.has_failed != "True" or subgraph.status == "failed":
                            op_ids = ast.literal_eval(subgraph.op_ids)                        
                            final_subsubgraph_list = []
                            for op_id in op_ids:
                                op = ravdb.get_op(op_id)
                                if op.status != "computed":
                                    if op.inputs != "null":
                                        for input_op_id in ast.literal_eval(op.inputs):
                                            input_op = ravdb.get_op(input_op_id)
                                            if input_op.status != "computed":
                                                final_subsubgraph_list.append(input_op_id)
                                    final_subsubgraph_list.append(op_id)
                            
                            final_subsubgraph_list = list(set(final_subsubgraph_list))

                            failed_ops = final_subsubgraph_list
                            failed_ops.sort()

                            for failed_op_id in failed_ops:
                                failed_op = ravdb.get_op(failed_op_id)
                                if failed_op.operator != "lin" and failed_op.status != "computed":
                                    ravdb.update_op(failed_op,subgraph_id=subgraph_id, message=None, status="pending")
                                elif failed_op.operator == "lin":
                                    ravdb.update_op(failed_op,subgraph_id=subgraph_id, message=None)
    
                            updated_subgraph = ravdb.update_subgraph(subgraph, op_ids=str(failed_ops), status='ready', optimized='True', has_failed = "True")    
                if len(Queue) > 0:
                    print('\nQUEUE', Queue)

                subgraphs = ravdb.get_ready_subgraphs_from_graph(graph_id=current_graph_id)

                for subgraph in subgraphs:

                    ready_flag = True
                    op_ids = ast.literal_eval(subgraph.op_ids)
                    for op_id in op_ids:
                        op = ravdb.get_op(op_id)
                        if op.inputs != 'null':
                            for input_op_id in ast.literal_eval(op.inputs):
                                input_op = ravdb.get_op(input_op_id)
                                if input_op.subgraph_id != subgraph.subgraph_id:
                                    if input_op.status != "computed":
                                        ready_flag = False
                                        break
                            if not ready_flag:
                                break
                    if not ready_flag:
                        continue

                    idle_clients = ravdb.get_idle_clients(reporting=ClientStatus.IDLE)
                    if idle_clients is not None:
                        op_ids = ast.literal_eval(subgraph.op_ids)
                        prelim_times = {}
                        for idle_client in idle_clients:
                            idle_client_time = 0
                            for op_id in op_ids:
                                op = ravdb.get_op(op_id=op_id)
                                if op is not None:
                                    operator = op.operator
                                    capabilities_dict = ast.literal_eval(idle_client.capabilities)
                                    if operator not in capabilities_dict.keys():
                                        client_time = random.random()*10
                                    else:
                                        client_time = capabilities_dict[operator]
                                    idle_client_time += client_time
                            prelim_times[idle_client.id] = idle_client_time
                        if bool(prelim_times):
                            fastest_client_id = min(prelim_times, key=prelim_times.get)
                            client = ravdb.get_client(id=fastest_client_id)
                            ravdb.update_subgraph(subgraph, status='assigned')
                            ravdb.update_client(client, reporting='busy', current_subgraph_id=subgraph.subgraph_id,
                                                current_graph_id=subgraph.graph_id)

                        else:
                            print('\n\nNo idle clients')
                    else:
                        print('\nNo idle clients')

                subgraphs = ravdb.get_all_subgraphs(graph_id=current_graph_id)
                if len(subgraphs) > 30:
                    subgraphs = subgraphs[-30:]
                for subgraph in subgraphs:
                    subgraph_op_ids = ast.literal_eval(subgraph.op_ids)
                    actual_op_ids = []
                    for op_id in subgraph_op_ids:
                        op = ravdb.get_op(op_id)
                        if op.subgraph_id == subgraph.subgraph_id:
                            actual_op_ids.append(op)
                                                        
                    num_ops = len(actual_op_ids)
                    counter = {'pending': 0, 'computed': 0, 'failed': 0, 'computing': 0}
                    for subgraph_op in actual_op_ids:
                        if subgraph_op.status == "pending":
                            counter['pending'] += 1
                        elif subgraph_op.status == "computed":
                            counter['computed'] += 1
                        elif subgraph_op.status == "failed":
                            counter['failed'] += 1
                        elif subgraph_op.status == "computing":
                            counter['computing'] += 1

                    if subgraph.status != 'computed':
                        if counter['computed'] == num_ops:
                            ravdb.update_subgraph(subgraph, status="computed")
                            assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                            if assigned_client is not None:
                                ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
                        
                        # elif counter['pending'] > 0 and (subgraph.status == 'computing' or subgraph.status == 'computed'):
                        #     assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                        #     if assigned_client is not None:
                        #         ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
                        #     ravdb.update_subgraph(subgraph, status="not_ready", optimized="False")
                            
                        elif counter['pending'] == 0 and counter['computing'] == 0 and counter['failed'] == 0 and counter['computed'] == 0:
                            ravdb.update_subgraph(subgraph, status="computed")
                            assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                            if assigned_client is not None:
                                ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)

                    elif subgraph.status == "computed" and subgraph.has_failed == "True":#and int(subgraph.retry_attempts) >= 2:
                        temp_Queue = Queue
                        for queue_subgraph_id, queue_graph_id in Queue:
                            if queue_subgraph_id == subgraph.subgraph_id and queue_graph_id == current_graph_id:
                                temp_Queue.remove((queue_subgraph_id, queue_graph_id))
                        Queue = temp_Queue

                    if subgraph.status == "computed":
                        # if counter['computed'] == num_ops:
                        #     assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                        #     if assigned_client is not None:
                        #         ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
                        if counter['pending'] > 0:
                            assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                            if assigned_client is not None:
                                ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
                            ravdb.update_subgraph(subgraph, status="not_ready", optimized="False", retry_attempts=0)

                    # Add failed and pending case
                    elif subgraph.status == "failed" and counter['pending'] > 0:
                        assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
                        if assigned_client is not None:
                            ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)
                        ravdb.update_subgraph(subgraph, status="not_ready", optimized="False")


        await sio.sleep(0.1)
