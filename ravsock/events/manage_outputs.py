import ast
import datetime
import json
import os
import pickle

import numpy as np

from ravsock.encryption import load_context
from ravsock.utils.strings import SubgraphStatus
from .scheduler import emit_op
from ..config import CONTEXT_FOLDER, PARAMS_FOLDER, FTP_RAVOP_FILES_PATH
from ..db import ravdb
from ..ftp import get_client
from ..globals import globals as g
from ..utils import OpStatus, MappingStatus, GraphStatus, dump_data, dump_data_non_ftp, get_logger

logger = get_logger()

sio = g.sio


async def get_op(sid, message):
    """
    Send an op to the client
    """
    print("get_op", message)

    # Find, create payload and emit op
    await emit_op(sid)

async def subgraph_completed(sid, results_list):
    # Save the results

    results_list = json.loads(results_list)
    
    # subgraph_id = None
    
    # print('RESULTS_LIST: \n\n\n\n\n',results_list)
    for data in results_list:
        data = json.loads(data)
        print("\nResult received: op_type: {}, operator: {}, op_id: {}, status: {}".format(data['op_type'],data['operator'],data['op_id'],data['status']))

        if "file_name" not in data:
            op_id = data["op_id"]
            op = ravdb.get_op(op_id)
            if data["status"] == "success":
                data_obj = ravdb.create_data(dtype="ndarray")
                result_array = np.array(data["result"])
                file_path = dump_data_non_ftp(data_obj.id, result_array, data["username"])
                ravdb.update_data(data_obj, file_path=file_path, file_size=result_array.size*result_array.itemsize)
                # Update op
                ravdb.update_op(
                    op, outputs=json.dumps([data_obj.id]), status=OpStatus.COMPUTED
                )
        
        else:

            op_id = data["op_id"]

            op = ravdb.get_op(op_id)

            if data["status"] == "success":
                data_obj = ravdb.create_data(dtype="ndarray")
                # file_path = dump_data(data_obj.id, value=np.array(data["result"]))
                temp_file_name = str(data["file_name"])

                username = str(data['username'])

                file_path_dir = FTP_RAVOP_FILES_PATH + '/' + str(username) + '/'

                temp_file_path = file_path_dir + temp_file_name

                new_file_path = file_path_dir + 'data_' + str(data_obj.id) + '.npy' 

                os.rename(temp_file_path, new_file_path)

                ravdb.update_data(data_obj, file_path=new_file_path)

                # Update op
                ravdb.update_op(
                    op, outputs=json.dumps([data_obj.id]), status=OpStatus.COMPUTED
                )

            # subgraph_id = op.subgraph_id
    
    client = ravdb.get_client_by_sid(sid)
    subgraph_id = client.current_subgraph_id
    graph_id = client.current_graph_id

    subgraph = ravdb.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)
    ravdb.update_subgraph(subgraph, status="computed")
    
    ravdb.update_client(client, reporting="idle", current_subgraph_id=None, current_graph_id=None, last_active_time=datetime.datetime.utcnow())

    # Emit another op to this client
    await emit_op(sid)



async def op_completed(sid, data):
    # Save the results
    logger.debug("\nResult received {}".format(data))
    data = json.loads(data)
    print("\nResult received: op_type: {}, operator: {}, op_id: {}, status: {}".format(data['op_type'],data['operator'],data['op_id'],data['status']))

    op_id = data["op_id"]

    op = ravdb.get_op(op_id)

    if data["status"] != "success":
        # Update op
        ravdb.update_op(
            op, outputs=None, status=OpStatus.FAILED, message=data["error"]
        )
        subgraph = ravdb.get_subgraph(subgraph_id=op.subgraph_id, graph_id=op.graph_id)
        ravdb.update_subgraph(subgraph, status="failed")
        assigned_client = ravdb.get_assigned_client(subgraph.subgraph_id, subgraph.graph_id)
        if assigned_client is not None:
            ravdb.update_client(assigned_client, reporting="idle", current_subgraph_id=None, current_graph_id=None)

    # Emit another op to this client
    await emit_op(sid)


def update_client_op_mapping(op_id, sid, status):
    client = ravdb.get_client_by_sid(sid)
    mapping = ravdb.find_client_op_mapping(client.id, op_id)
    ravdb.update_client_op_mapping(
        mapping.id, status=status, response_time=datetime.datetime.now()
    )


def deserialize_decypt_values(values, ckks_context, secret_key):

    subgraph_params = values['params']

    dd = {}
    for subgraph_id, subgraph_ops_params in subgraph_params.items():
        list1 = dict()
        for op_id, subgraph_op_params in subgraph_ops_params.items():
            list2 = []
            for column in subgraph_op_params:
                # 3. Deserialize encrypted bytearrays
                if values.get("encryption", False):
                    deserialized_values = {}
                    for key, value in column.items():
                        print(key, type(value))
                        if key == "op_id":
                            continue

                        if value is not None:
                            deserialized_values[key] = round(
                                ts.ckks_tensor_from(ckks_context, value)
                                    .decrypt(secret_key)
                                    .tolist()[0], 2)
                        else:
                            deserialized_values[key] = value

                    list2.append(deserialized_values)
            list1[op_id] = list2
        dd[subgraph_id] = list1
    return dd


async def receive_params(sid, client_data):
    
    if client_data.get("graph_id", None) is not None:
        print("Client params:", client_data)

        graph_id = client_data["graph_id"]
        graph = ravdb.get_graph(graph_id=graph_id)
        client = ravdb.get_client_by_sid(sid)

        # 1. Fetch params file
        ftp_client = get_client(**ast.literal_eval(client.ftp_credentials))
        ftp_client.download(
            os.path.join(PARAMS_FOLDER, client_data["params_file"]),
            client_data["params_file"],
        )
        with open(
                os.path.join(PARAMS_FOLDER, client_data["params_file"]), "rb"
        ) as f:
            client_params = pickle.load(f)

        dd = client_params['params']

        if client_params.get("encryption", False):
            import tenseal as ts
            # 2. Load context
            ckks_context = load_context(
                os.path.join(
                    CONTEXT_FOLDER, json.loads(client.context)["context_filename"]
                )
            )
            secret_key = ckks_context.secret_key()
            dd = deserialize_decypt_values(client_params, ckks_context, secret_key)

        for subgraph_id, subgraph_ops_params in dd.items():
            mapping = ravdb.find_subgraph_client_mapping(subgraph_id=subgraph_id, client_id=client.id)
            if not mapping.status == MappingStatus.COMPUTED:

                # 4. Update objective client mapping if any
                ravdb.update_subgraph_client_mapping(
                    mapping.id,
                    status=MappingStatus.COMPUTED,
                    result=json.dumps(subgraph_ops_params),
                )
                
                # print("subgraph_ops_params added")
                aggregated_graph_results = None
                # 5. Aggregate and update objective
                if graph.outputs is None:
                    # Update objective if this is the first objective result
                    ravdb.update_graph(graph,
                                       outputs=json.dumps(subgraph_ops_params),
                    )
                    aggregated_graph_results = subgraph_ops_params
                else:
                    aggregated_ops_params = dict()
                    for op_id, subgraph_op_params in subgraph_ops_params.items():

                        # Aggregate values
                        previous_params = json.loads(graph.outputs)[str(op_id)]
                        current_params = subgraph_op_params

                        list_aggregated_column_params = []
                        op = ravdb.get_op(op_id=op_id)
                        for index, current_param in enumerate(subgraph_op_params):
                            previous_param = previous_params[index]
                            current_mean = current_param.get("federated_mean", None)
                            previous_mean = previous_param.get("federated_mean", None)

                            n1 = previous_param["size"]
                            n2 = current_param["size"]
                            final_mean = None
                            global_variance = None
                            global_standard_deviation = None
                            global_min = min(
                                previous_param["minimum"], current_param.get("minimum", float("inf"))
                            )
                            global_max = max(
                                previous_param["maximum"], current_param.get("maximum", float("-inf"))
                            )

                            if op.operator == "federated_mean":
                                final_mean = (previous_mean * n1) / (n1 + n2) + (
                                        current_mean * n2
                                ) / (n1 + n2)
                            elif op.operator == "federated_variance":
                                final_mean = (previous_mean * n1) / (n1 + n2) + (
                                        current_mean * n2
                                ) / (n1 + n2)
                                global_variance = (
                                                          n1 * previous_param.get("federated_variance", None)
                                                          + n2 * current_param.get("federated_variance", None)
                                                  ) / (n1 + n2) + (
                                                          (n1 * n2 * (previous_mean - current_mean) ** 2) / (n1 + n2) ** 2
                                                  )
                            elif op.operator == "federated_standard_deviation":
                                final_mean = (previous_mean * n1) / (n1 + n2) + (
                                        current_mean * n2
                                ) / (n1 + n2)
                                global_variance = (
                                                          n1 * previous_param.get("federated_variance", None)
                                                          + n2 * current_param.get("federated_variance", None)
                                                  ) / (n1 + n2) + (
                                                          (n1 * n2 * (previous_mean - current_mean) ** 2) / (n1 + n2) ** 2
                                                  )
                                global_standard_deviation = np.sqrt(global_variance)

                            aggregated_column_params = {
                                "federated_mean": final_mean,
                                "size": n1 + n2,
                                "federated_variance": global_variance,
                                "minimum": global_min,
                                "maximum": global_max,
                                "federated_standard_deviation": global_standard_deviation
                            }
                            list_aggregated_column_params.append(aggregated_column_params)
                        aggregated_ops_params[op_id] = list_aggregated_column_params

                    ravdb.update_graph(
                        graph=graph,
                        outputs=json.dumps(
                            aggregated_ops_params
                        ),
                    )
                    aggregated_graph_results = aggregated_ops_params
                # 6. Update objective status based on rules and mappings count
                mappings = ravdb.get_subgraph_client_mappings(
                    subgraph_id=subgraph_id, status=MappingStatus.COMPUTED
                )

                # print(graph.rules)
                rules = json.loads(graph.rules)
                if mappings.count() >= rules["max_clients"]:
                    ravdb.update_graph(
                        graph=graph, status=MappingStatus.COMPUTED
                    )
                    # Update SubgraphStatus of all Subgraphs in the graph.
                    subgraphs = ravdb.get_subgraphs_from_graph(graph_id=graph.id)
                    for subgraph in subgraphs:
                        ravdb.update_subgraph(subgraph,status=SubgraphStatus.COMPUTED)

                    column_names = client_params["column_names"]

                    # Update OpStatus of all Ops belonging to Graph here.
                    ops = ravdb.get_ops(graph_id=graph.id, status=OpStatus.PENDING)
                    for op in ops:
                        data_obj = ravdb.create_data(dtype="ndarray")
                        op_operator = op.operator
                        op_result = []
                        for index,column_name in enumerate(column_names):
                            column_output = aggregated_graph_results[op.id][index]

                            column_result = {}
                            for key, value in column_output.items():
                                if key == op_operator:
                                    column_result[column_name] = value
                            op_result.append(column_result)
                        file_path = dump_data(data_obj.id, value=np.array(op_result))
                        ravdb.update_data(data_obj, file_path=file_path)

                        ravdb.update_op(op,outputs=json.dumps([data_obj.id]), status=OpStatus.COMPUTED)

                    print('\n\nAGGREGATION COMPLETED!!\n')
                    import pprint
                    print(pprint.pformat(aggregated_graph_results))
                    print('\n')
            else:
                print("Duplicate results")
