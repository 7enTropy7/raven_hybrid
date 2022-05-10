import json
import ast
import numpy as np
import sqlalchemy as db
from sqlalchemy import or_, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import (
    database_exists,
    create_database as cd,
    drop_database as dba,
)
from ..utils import load_data_from_file
from .models import (
    Op,
    Graph,
    SubGraph,
    ClientOpMapping,
    Client,
    Data,
    Base,
    GraphClientMapping,
    Objective,
    ObjectiveClientMapping,
    ClientSIDMapping,
    SubgraphClientMapping)
from ..config import RDF_DATABASE_URI
from ..utils import MappingStatus, OpStatus
from ..utils import delete_data_file, save_data_to_file, Singleton


@Singleton
class DBManager(object):
    def __init__(self):
        self.create_database()
        self.engine, self.session = self.connect()

    def connect(self):
        print("Database uri:", RDF_DATABASE_URI)
        engine = db.create_engine(RDF_DATABASE_URI, isolation_level="READ UNCOMMITTED")
        connection = engine.connect()
        Base.metadata.bind = engine
        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        return engine, session

    def create_database(self):
        if not database_exists(RDF_DATABASE_URI):
            cd(RDF_DATABASE_URI)
            print("Database created")

    def drop_database(self):
        if database_exists(RDF_DATABASE_URI):
            dba(RDF_DATABASE_URI)
            print("Database dropped")

    def create_session(self):
        """
        Create a new session
        """
        DBSession = sessionmaker(bind=self.engine)
        return DBSession()

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def refresh(self, obj):
        """
        Refresh an object
        """
        self.session.refresh(obj)
        return obj

    def get(self, name, id):
        if name == "op":
            obj = self.session.query(Op).get(id)
        elif name == "data":
            obj = self.session.query(Data).get(id)
        elif name == "graph":
            obj = self.session.query(Graph).get(id)
        elif name == "client":
            obj = self.session.query(Client).get(id)
        elif name == "objective":
            obj = self.session.query(Objective).get(id)
        elif name == "objective_client_mapping":
            obj = self.session.query(ObjectiveClientMapping).get(id)
        elif name == "client_sid_mapping":
            obj = self.session.query(ClientSIDMapping).get(id)
        else:
            obj = None

        return obj

    def add(self, n, **kwargs):
        if n == "op":
            obj = Op()
        elif n == "data":
            obj = Data()
        elif n == "graph":
            obj = Graph()
        elif n == "client":
            obj = Client()
        elif n == "objective":
            obj = Objective()
        elif n == "objective_client_mapping":
            obj = ObjectiveClientMapping()
        elif n == "client_sid_mapping":
            obj = ClientSIDMapping()
        else:
            obj = None

        for key, value in kwargs.items():
            setattr(obj, key, value)
        self.session.add(obj)
        self.session.commit()

        return obj

    def update(self, n, id, **kwargs):
        if n == "op":
            obj = self.session.query(Op).get(id)
        elif n == "data":
            obj = self.session.query(Data).get(id)
        elif n == "graph":
            obj = self.session.query(Graph).get(id)
        elif n == "client":
            obj = self.session.query(Client).get(id)
        elif n == "objective":
            obj = self.session.query(Objective).get(id)
        elif n == "objective_client_mapping":
            obj = self.session.query(ObjectiveClientMapping).get(id)
        elif n == "client_sid_mapping":
            obj = self.session.query(ClientSIDMapping).get(id)
        else:
            obj = None

        for key, value in kwargs.items():
            setattr(obj, key, value)
        self.session.commit()
        return obj

    def delete(self, obj):
        self.session.delete(obj)
        self.session.commit()

    def create_op(self, **kwargs):
        op = Op()

        for key, value in kwargs.items():
            setattr(op, key, value)

        self.session.add(op)
        self.session.commit()
        return op

    def get_op(self, op_id):
        """
        Get an existing op
        """
        return self.session.query(Op).get(op_id)

    def get_ops_by_subgraph(self, subgraph_id, graph_id):
        """
        Get all ops of a subgraph
        """
        return self.session.query(Op).filter(
            and_(Op.subgraph_id == subgraph_id,
            Op.graph_id == graph_id)).all()

    def delete_op(self, op):
        self.session.delete(op)
        self.session.commit()

    def update_op(self, op, **kwargs):
        for key, value in kwargs.items():
            setattr(op, key, value)

        self.session.commit()
        return op

    def create_data(self, **kwargs):
        data = Data()

        for key, value in kwargs.items():
            setattr(data, key, value)

        self.session.add(data)
        self.session.commit()
        return data

    def get_data(self, data_id):
        """
        Get an existing data
        """
        return self.session.query(Data).get(data_id)

    def update_data(self, data, **kwargs):
        for key, value in kwargs.items():
            setattr(data, key, value)

        self.session.commit()
        return data

    def delete_data(self, data_id):
        data = self.session.query(Data).get(data_id)
        self.session.delete(data)
        self.session.commit()

    def create_data_complete(self, data, data_type):
        # print("Creating data:", data)

        if isinstance(data, (np.ndarray, np.generic)):
            if data.ndim == 1:
                data = data[..., np.newaxis]

        d = self.create_data(type=data_type)

        # Save file
        file_path = save_data_to_file(d.id, data, data_type)

        # Update file path
        self.update(d, file_path=file_path)

        return d

    def get_op_status(self, op_id):
        status = self.session.query(Op).get(op_id).status
        return status

    def get_last_graph_id(self):
        """
        Get the last graph id
        """
        return self.session.query(Graph).count() + 1

    def get_graph_op_dependency(self, graph_id):
        """
        Get op_dependencies of a given graph
        """
        ops = self.session.query(Op).filter(and_(Op.graph_id == graph_id, Op.status == 'pending')).all()
        subgraph_ops = {}
        temp = []
        for op in ops:
            if op.subgraph_id not in temp:
                temp.append(op.subgraph_id)
        for subgraph_id in temp:
            t = []
            for op in ops:
                if op.subgraph_id == subgraph_id:
                    t.append(op.id)
            subgraph_ops[subgraph_id] = t
        return subgraph_ops

    def get_graph(self, graph_id):
        """
        Get an existing graph
        """
        return self.session.query(Graph).get(graph_id)

    def create_graph(self):
        """
        Create a new graph
        """
        graph = Graph()
        self.session.add(graph)
        self.session.commit()
        return graph

    def update_graph(self, graph, **kwargs):
        for key, value in kwargs.items():
            setattr(graph, key, value)
        self.session.commit()
        return graph

    def get_graph_ops(self, graph_id):
        return self.session.query(Op).filter(Op.graph_id == graph_id).all()

    def delete_graph_ops(self, graph_id):
        print("Deleting graph ops...")
        ops = self.get_graph_ops(graph_id=graph_id)

        for op in ops:
            print("Op id:{}".format(op.id))
            data_ids = json.loads(op.outputs)
            if data_ids is not None:
                for data_id in data_ids:
                    print("Data id:{}".format(data_id))

                    print("Deleting Data file...")
                    # Delete data file
                    delete_data_file(data_id)

                    print("Deleting Data object...")
                    # Delete data object
                    self.delete_data(data_id)

            print("Deleting op...")
            # Delete op object
            self.delete(op)

    def create_client(self, **kwargs):
        obj = Client()

        for key, value in kwargs.items():
            setattr(obj, key, value)

        self.session.add(obj)
        self.session.commit()
        return obj

    def get_client(self, id):
        """
        Get an existing client by id
        """
        return self.session.query(Client).get(id)

    def get_assigned_client(self, subgraph_id, graph_id):
        """
        Get assigned client by subgraph_id and graph_id
        """
        return self.session.query(Client).filter(Client.status == 'connected').filter(Client.current_graph_id == graph_id).filter(Client.current_subgraph_id == subgraph_id).first()

    def get_assigned_clients(self, subgraph_id, graph_id):
        """
        Get all assigned client by subgraph_id and graph_id
        """
        return self.session.query(Client).filter(Client.status == 'connected').filter(Client.current_graph_id == graph_id).filter(Client.current_subgraph_id == subgraph_id).all()

    def get_client_by_cid(self, cid):
        """
        Get an existing client by cid
        """
        return self.session.query(Client).filter(Client.cid == cid).first()

    def update_client(self, client, **kwargs):
        for key, value in kwargs.items():
            setattr(client, key, value)
        self.session.commit()
        return client

    def get_all_clients(self):
        return self.session.query(Client).order_by(Client.created_at.desc()).all()

    def get_all_graphs(self, approach=None, algorithm=None):
        if approach:
            return self.session.query(Graph).filter(Graph.approach == approach).order_by(Graph.created_at.desc()).all()
        if algorithm:
            return self.session.query(Graph).filter(Graph.algorithm == algorithm).order_by(Graph.created_at.desc()).all()

        return self.session.query(Graph).order_by(Graph.created_at.desc()).all()

    def get_all_ops(self):
        return self.session.query(Op).order_by(Op.id.desc()).all()

    # def deactivate_all_graphs(self):
    #     for graph in self.session.query(Graph).all():
    #         graph.status = "inactive"
    #
    #     self.session.commit()
    #
    # def deactivate_graph(self, graph_id):
    #     graph = self.get_graph(graph_id=graph_id)
    #     graph.status = "inactive"
    #     self.session.commit()

    def disconnect_all_clients(self):
        for cliet in self.session.query(Client).all():
            cliet.status = "disconnected"

        self.session.commit()

    def disconnect_client(self, client_id):
        client = self.get_client(client_id=client_id)
        client.status = "disconnected"
        self.session.commit()

    def get_ops_by_name(self, op_name, graph_id=None):
        if graph_id is not None:
            ops = (
                self.session.query(Op)
                .filter(Op.graph_id == graph_id)
                .filter(Op.name.contains(op_name))
                .all()
            )
        else:
            ops = self.session.query(Op).filter(Op.name.contains(op_name)).all()

        return ops

    def get_op_readiness(self, op):
        """
        Get op readiness
        """
        inputs = json.loads(op.inputs)
        params = json.loads(op.params)

        cs = 0

        if inputs is not None:

            for input_op in inputs:
                input_op1 = self.get_op(op_id=input_op)
                if input_op1.status in ["pending", "computing"]:
                    return "parent_op_not_ready"
                elif input_op1.status == "failed":
                    return "parent_op_failed"
                elif input_op1.status == "computed":
                    cs += 1

            for index, value in params.items():
                if type(value).__name__ == "int":
                    cop = self.get_op(op_id=value)
                    if cop.status in ["pending", "computing"]:
                        return "parent_op_not_ready"
                    elif cop.status == "failed":
                        return "parent_op_failed"
                    elif cop.status == "computed":
                        cs += 1
                else:
                    cs += 1

            if cs == len(inputs) + len(params.keys()):
                return "ready"
            else:
                return "not_ready"

        return "ready"

    def get_ops_without_graph(self, status=None):
        """
        Get a list of all ops not associated to any graph
        """
        if status is not None:
            return (
                self.session.query(Op)
                .filter(Op.graph_id is None)
                .filter(Op.status == status)
                .all()
            )
        else:
            return self.session.query(Op).filter(Op.graph_id is None).all()

    def get_graphs(self, status=None, approach=None):
        """
        Get a list of graphs
        """
        graphs = self.session.query(Graph)
        if status is not None:
            graphs = graphs.filter(Graph.status == status)

        if approach is not None:
            graphs = graphs.filter(Graph.approach == approach)

        return graphs.all()

    def get_clients(self, status=None):
        """
        Get a list of clients
        """
        if status is not None:
            return self.session.query(Client).filter(Client.status == status).all()
        else:
            return self.session.query(Client).all()

    def get_idle_clients(self, reporting=None):
        """
        Get a list of idle clients based on reporting column
        """
        if reporting is not None:
            return self.session.query(Client).filter(Client.status == 'connected').filter(Client.reporting == reporting).all()
        else:
            return self.session.query(Client).filter(Client.status == 'connected').all()

    def get_available_clients(self):
        """
        Get all clients which are available
        """
        clients = self.session.query(Client).filter(Client.status == "connected").all()

        client_list = []
        for client in clients:
            client_ops = client.client_ops.filter(
                or_(
                    ClientOpMapping.status == MappingStatus.SENT,
                    ClientOpMapping.status == MappingStatus.ACKNOWLEDGED,
                    ClientOpMapping.status == MappingStatus.COMPUTING,
                )
            )
            if client_ops.count() == 0:
                client_list.append(client)

        return client_list

    def get_subgraph_ops(self, graph_id=None, subgraph_id=None):
        """
        Get a list of ops based on graph and subgraph ids
        """
        if graph_id is None and subgraph_id is None:
            return self.session.query(Op).all()
        elif graph_id is not None and subgraph_id is not None:
            return (
                self.session.query(Op)
                .filter(Op.graph_id == graph_id)
                .filter(Op.subgraph_id == subgraph_id)
                .all()
            )
        else:
            if graph_id is not None:
                return self.session.query(Op).filter(Op.graph_id == graph_id).all()
            elif subgraph_id is not None:
                return self.session.query(Op).filter(Op.subgraph_id == subgraph_id).all()
            else:
                return self.session.query(Op).all()

    def get_ops(self, graph_id=None, status=None):
        """
        Get a list of ops based on certain parameters
        """
        if graph_id is None and status is None:
            return self.session.query(Op).all()
        elif graph_id is not None and status is not None:
            return (
                self.session.query(Op)
                .filter(Op.graph_id == graph_id)
                .filter(Op.status == status)
                .all()
            )
        else:
            if graph_id is not None:
                return self.session.query(Op).filter(Op.graph_id == graph_id).all()
            elif status is not None:
                return self.session.query(Op).filter(Op.status == status).all()
            else:
                return self.session.query(Op).all()

    def create_client_op_mapping(self, **kwargs):
        mapping = ClientOpMapping()

        for key, value in kwargs.items():
            setattr(mapping, key, value)

        self.session.add(mapping)
        self.session.commit()
        return mapping

    def update_client_op_mapping(self, client_op_mapping_id, **kwargs):
        mapping = self.session.query(ClientOpMapping).get(client_op_mapping_id)
        for key, value in kwargs.items():
            setattr(mapping, key, value)
        self.session.commit()
        return mapping

    def find_client_op_mapping(self, client_id, op_id):
        mapping = (
            self.session.query(ClientOpMapping)
            .filter(
                ClientOpMapping.client_id == client_id, ClientOpMapping.op_id == op_id
            )
            .first()
        )
        return mapping

    def get_incomplete_op(self):
        ops = self.session.query(Op).filter(Op.status == OpStatus.COMPUTING).all()

        for op in ops:
            op_mappings = op.op_mappings
            if (
                op_mappings.filter(ClientOpMapping.status == MappingStatus.SENT).count()
                >= 3
                or op_mappings.filter(
                    ClientOpMapping.status == MappingStatus.COMPUTING
                ).count()
                >= 2
                or op_mappings.filter(
                    ClientOpMapping.status == MappingStatus.REJECTED
                ).count()
                >= 5
                or op_mappings.filter(
                    ClientOpMapping.status == MappingStatus.FAILED
                ).count()
                >= 3
            ):
                continue

            return op
        return None

    def get_op_status_final(self, op_id):
        op = self.get_op(op_id=op_id)
        op_mappings = op.op_mappings
        if (
            op_mappings.filter(ClientOpMapping.status == MappingStatus.FAILED).count()
            >= 3
        ):
            return "failed"

        return "computing"

    def get_first_graph_op(self, graph_id):
        """
        Return the first graph op
        """
        ops = self.get_graph_ops(graph_id=graph_id)
        return ops[0]

    def get_last_graph_op(self, graph_id):
        """
        Return the last graph op
        """
        ops = self.get_graph_ops(graph_id=graph_id)
        return ops[-1]

    """
    Federated learning
    """

    def get_available_federated_clients(self):
        """
        Get a list of federated clients
        """
        clients = self.session.query(Client).filter(Client.status == "connected").all()
        clients_sids = [client.client_id for client in clients]
        return clients, clients_sids

    def update_federated_op(self, **kwargs):
        ops = (
            self.session.query(Op)
            .filter(Op.operator == "federated_training")
            .filter(Op.id == kwargs.get("id"))
            .all()
        )
        print(ops)
        for op in ops:
            for key, value in kwargs.items():
                setattr(op, key, value)
        self.session.commit()

    def update_federated_client_status(self, client, **kwargs):
        # self.session.query(Op).filter(Op.status == OpStatus.COMPUTING).all()
        for key, value in kwargs.items():
            setattr(client, key, value)
        self.session.query(Op).commit()

    """
    Graph client mapping
    """

    def create_graph_client_mapping(self, **kwargs):
        mapping = GraphClientMapping()

        for key, value in kwargs.items():
            setattr(mapping, key, value)

        self.session.add(mapping)
        self.session.commit()
        return mapping

    def update_graph_client_mapping(self, graph_client_mapping_id, **kwargs):
        mapping = self.session.query(GraphClientMapping).get(graph_client_mapping_id)
        for key, value in kwargs.items():
            setattr(mapping, key, value)
        self.session.commit()
        return mapping

    def find_graph_client_mapping(self, graph_id, client_id):
        mapping = (
            self.session.query(GraphClientMapping)
            .filter(
                GraphClientMapping.client_id == client_id,
                GraphClientMapping.graph_id == graph_id,
            )
            .first()
        )
        return mapping

    """
    Subgraph client mapping
    """

    def create_subgraph_client_mapping(self, **kwargs):
        mapping = SubgraphClientMapping()

        for key, value in kwargs.items():
            setattr(mapping, key, value)

        self.session.add(mapping)
        self.session.commit()
        return mapping

    def update_subgraph_client_mapping(self, subgraph_client_mapping_id, **kwargs):
        mapping = self.session.query(SubgraphClientMapping).get(subgraph_client_mapping_id)
        for key, value in kwargs.items():
            setattr(mapping, key, value)
        self.session.commit()
        return mapping

    def find_subgraph_client_mapping(self, subgraph_id, client_id):
        mapping = (
            self.session.query(SubgraphClientMapping)
                .filter(
                SubgraphClientMapping.client_id == client_id,
                SubgraphClientMapping.subgraph_id == subgraph_id,
            ).first()
        )
        return mapping

    def get_subgraph_client_mappings(self, subgraph_id=None, client_id=None, status=None):
        if subgraph_id is None and client_id is None:
            return None

        mappings = self.session.query(SubgraphClientMapping)

        if subgraph_id is not None:
            mappings = (
                mappings
                    .filter(
                    SubgraphClientMapping.subgraph_id == subgraph_id,
                )
            )
        elif client_id is not None:
            mappings = (
                mappings
                    .filter(
                    SubgraphClientMapping.client_id == client_id,
                )
            )

        if status is not None:
            mappings = (
                mappings
                    .filter(
                    SubgraphClientMapping.status == status,
                )
            )

        return mappings

    """
    Objective
    """

    def create_objective(self, **kwargs):
        print(kwargs)
        return self.add("objective", **kwargs)

    def update_objective(self, objective_id, **kwargs):
        return self.update("objective", objective_id, **kwargs)

    def get_objective(self, objective_id):
        return self.get("objective", objective_id)

    def find_active_objective(self, client_id):
        objectives = (
            self.session.query(Objective)
            .filter(or_(Objective.status == "pending", Objective.status == "active"))
            .all()
        )
        for objective in objectives:
            if self.find_objective_client_mapping(objective.id, client_id) is None:
                return objective
        return None

    def get_objectives(self):
        return self.session.query(Objective).all()

    def create_objective_client_mapping(self, **kwargs):
        return self.add("objective_client_mapping", **kwargs)

    def update_objective_client_mapping(self, objective_client_mapping_id, **kwargs):
        return self.update(
            "objective_client_mapping", objective_client_mapping_id, **kwargs
        )

    def get_objective_client_mapping(self, objective_client_mapping_id):
        return self.get("objective_client_mapping", objective_client_mapping_id)

    def get_objective_client_mappings(self):
        return self.session.query(ObjectiveClientMapping).all()

    def find_objective_client_mapping(self, objective_id, client_id):
        mapping = (
            self.session.query(ObjectiveClientMapping)
            .filter(
                ObjectiveClientMapping.client_id == client_id,
                ObjectiveClientMapping.objective_id == objective_id,
            )
            .first()
        )
        return mapping

    def get_objective_mappings(self, objective_id, status=None):
        if status is not None:
            return self.session.query(ObjectiveClientMapping).filter(
                ObjectiveClientMapping.objective_id == objective_id,
                ObjectiveClientMapping.status == MappingStatus.COMPUTED,
            )
        else:
            return self.session.query(ObjectiveClientMapping).filter(
                ObjectiveClientMapping.objective_id == objective_id
            )

    """
    Client SID Mapping
    """

    def create_client_sid_mapping(self, **kwargs):
        return self.add("client_sid_mapping", **kwargs)

    def update_client_sid_mapping(self, client_sid_mapping_id, **kwargs):
        return self.update("client_sid_mapping", client_sid_mapping_id, **kwargs)

    def get_client_sid_mapping(self, client_sid_mapping_id):
        return self.get("client_sid_mapping", client_sid_mapping_id)

    def delete_client_sid_mapping(self, sid):
        self.session.query(ClientSIDMapping).filter(
            ClientSIDMapping.sid == sid
        ).first().delete()

    def find_client_sid_mapping(self, cid, sid):
        return (
            self.session.query(ClientSIDMapping)
            .filter(and_(ClientSIDMapping.sid == sid, ClientSIDMapping.cid == cid))
            .first()
        )

    def get_client_by_sid(self, sid):
        client_sid_mapping = (
            self.session.query(ClientSIDMapping)
            .filter(ClientSIDMapping.sid == sid)
            .first()
        )
        if client_sid_mapping is None:
            return None
        else:
            return self.get_client_by_cid(cid=client_sid_mapping.cid)

    def get_op_output(self,op_id):
        op = self.get_op(op_id)
        if op.outputs is None or op.outputs == "null":
            return None

        data_id = json.loads(op.outputs)[0]
        data = self.get_data(data_id=data_id)
        data = load_data_from_file(data.file_path)
        return data

    def initialize_subgraph_complexities_list(self,subgraph_dictionary):
        for subgraph_id in subgraph_dictionary:
            op_list_complexity = 0
            for op_id in subgraph_dictionary[subgraph_id]:
                current_graph_id = self.get_op(op_id).graph_id
                op = self.get_op(op_id)
                if op.status == "pending":
                    op_list_complexity += self.get_op(op_id).complexity
            subgraph_dictionary[subgraph_id] = op_list_complexity
        if len(subgraph_dictionary) > 0:
            subgraph_complexities_list = []
            for i in subgraph_dictionary:
                subgraph_complexities_list.append(subgraph_dictionary[i])

        # graph = self.get_graph(graph_id=current_graph_id)
        # self.update_graph(graph,subgraph=str(subgraph_dictionary))        

    def get_subgraph_complexities(self,graph_id):
        graph = self.get_graph(graph_id=graph_id)
        if graph.subgraph_complexities is None:
            return None
        return graph.subgraph_complexities

    def get_all_subgraphs(self, graph_id):
        """
        Get all subgraphs belonging to a graph
        """
        return self.session.query(SubGraph).filter(SubGraph.graph_id == graph_id).all()

    def get_horizontal_split_subgraphs(self, graph_id):
        """
        Get all subgraphs belonging to a graph
        """
        return self.session.query(SubGraph).filter(and_(SubGraph.graph_id == graph_id, 
            SubGraph.status != 'computed',
            SubGraph.status != 'standby',
            SubGraph.status != 'failed',
            SubGraph.status != 'computing',
            SubGraph.status != 'assigned'
        )).all()

    def get_first_ready_subgraph_from_graph(self, graph_id):
        """
        Get an existing subgraph
        """
        return self.session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(SubGraph.status == 'ready').first()

    def get_first_active_subgraph_from_graph(self, graph_id):
        """
        Get an existing subgraph
        """
        return self.session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(or_(SubGraph.status == 'ready',SubGraph.status == 'not_ready',SubGraph.status == 'standby')).first()

    def get_ready_subgraphs_from_graph(self, graph_id):
        """
        Get existing ready subgraphs
        """
        return self.session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(SubGraph.status == 'ready').all()

    def get_not_ready_subgraphs_from_graph(self, graph_id):
        """
        Get existing not ready subgraphs
        """
        return self.session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(SubGraph.status == 'not_ready').all()    

    def get_subgraphs_from_graph(self, graph_id):
        """
        Get an existing subgraph
        """
        return self.session.query(SubGraph).filter(SubGraph.graph_id == graph_id).filter(
            or_(
                SubGraph.status == 'ready',
                SubGraph.status == 'computing',
                SubGraph.status == 'assigned')).all()

    def get_failed_subgraphs_from_graph(self, graph):
        """
        Get an existing subgraph
        """
        graph_id = graph.id
        if graph.status != 'failed':
            failed_ops = self.session.query(Op).filter(Op.graph_id == graph_id).filter(Op.status == 'failed').all()#, Op.message == 'values passed to tensor(values) must be a number/boolean/string or an array of numbers/booleans/strings, or a TypedArray')).all()
            unique_subgraph_ids = []
            for failed_op in failed_ops:
                if failed_op.subgraph_id not in unique_subgraph_ids:
                    unique_subgraph_ids.append(failed_op.subgraph_id)
            return unique_subgraph_ids
        else:
            return []

    def get_subgraph(self, subgraph_id, graph_id):
        """
        Get an existing subgraph
        """
        subgraph = self.session.query(SubGraph).filter(SubGraph.subgraph_id == subgraph_id,
                                                        SubGraph.graph_id == graph_id).first()
        return subgraph

    def delete_subgraph(self, subgraph_id, graph_id):
        subgraph = self.get_subgraph(subgraph_id=subgraph_id, graph_id=graph_id)
        self.session.delete(subgraph)
        self.session.commit()

    def update_subgraph_complexity(self, subgraph_id):
        subgraph = self.get_subgraph(subgraph_id=subgraph_id)
        op_ids = ast.literal_eval(subgraph.op_ids)
        pending_ops = []
        for op in op_ids:
            op_obj = self.get_op(op)
            if op_obj.status == "pending":
                pending_ops.append(op_obj)
        subgraph_complexity = 0
        for pending_op in pending_ops:
            subgraph_complexity += pending_op.complexity

        self.update_subgraph(subgraph, complexity=subgraph_complexity)

        if subgraph_complexity == 0:
            self.update_subgraph(subgraph, status="computed")

        # return subgraph_complexities_list

    def create_subgraph(self, **kwargs):
        subgraph = SubGraph()

        for key, value in kwargs.items():
            setattr(subgraph, key, value)

        self.session.add(subgraph)
        self.session.commit()
        return subgraph

    def update_subgraph(self, subgraph, **kwargs):
        for key, value in kwargs.items():
            setattr(subgraph, key, value)
        self.session.commit()
        return subgraph
