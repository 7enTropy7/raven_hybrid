import glob
import json
import os
import secrets
import shutil
import string
from functools import reduce
import shutil
import numpy as np
from sqlalchemy.orm import class_mapper
from sqlalchemy_utils import database_exists

from ravpy.config import FTP_DOWNLOAD_FILES_FOLDER

from .logger import get_logger
from .strings import *
from .strings import Operators
from ..config import DATA_FILES_PATH, RDF_DATABASE_URI, FTP_RAVOP_FILES_PATH


def save_data_to_file(data_id, data):
    """
    Method to save data in a pickle file
    """
    file_path = os.path.join(DATA_FILES_PATH, "data_{}.json".format(data_id))

    if os.path.exists(file_path):
        os.remove(file_path)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        if isinstance(data, np.ndarray):
            data = data.tolist()
        json.dump(data, f)

    return file_path


def load_data_from_file(file_path):
    print("File path:", file_path)
    x = np.load(file_path)
    return x


def delete_data_file(data_id):
    file_path = os.path.join(DATA_FILES_PATH, "data_{}.json".format(data_id))
    if os.path.exists(file_path):
        os.remove(file_path)


class Singleton:
    def __init__(self, cls):
        self._cls = cls

    def Instance(self):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._cls()
            return self._instance

    def __call__(self):
        raise TypeError("Singletons must be accessed through `Instance()`.")

    def __instancecheck__(self, inst):
        return isinstance(inst, self._cls)


def dump_data(data_id, value):
    """
    Dump ndarray to file
    """
    file_path = os.path.join(DATA_FILES_PATH, "data_{}.npy".format(data_id))
    if os.path.exists(file_path):
        os.remove(file_path)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    np.save(file_path, value)
    return file_path

def dump_data_non_ftp(data_id, value, username):
    """
    Dump ndarray to file
    """
    file_path = os.path.join(FTP_RAVOP_FILES_PATH, "{}/data_{}.npy".format(username,data_id))
    if os.path.exists(file_path):
        os.remove(file_path)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    np.save(file_path, value)
    return file_path


def copy_data(source, destination):
    try:
        shutil.copy(source, destination)
        print("File copied successfully.")
    # If source and destination are same
    except shutil.SameFileError:
        print("Source and destination represents the same file.")
    # If there is any permission issue
    except PermissionError:
        print("Permission denied.")
    # For other errors
    except:
        print("Error occurred while copying file.")


def reset_database():
    from ..db import ravdb

    ravdb.drop_database()
    ravdb.create_database()
    ravdb.create_tables()


def create_database():
    from ..db import ravdb

    while not database_exists(RDF_DATABASE_URI):
        ravdb.create_database()
        return True

    return False


def create_tables():
    from ..db import ravdb

    if database_exists(RDF_DATABASE_URI):
        ravdb.create_tables()
        print("Tables created")


def reset():
    # Delete and create database
    reset_database()

    for file_path in glob.glob(os.path.join(DATA_FILES_PATH, "*")):
        if os.path.exists(file_path):
            os.remove(file_path)

    if not os.path.exists(DATA_FILES_PATH):
        os.makedirs(DATA_FILES_PATH)

    shutil.rmtree(FTP_RAVOP_FILES_PATH) 

    if not os.path.exists(FTP_RAVOP_FILES_PATH):
        os.makedirs(FTP_RAVOP_FILES_PATH)

    # Clear redis queues
    from ..db import clear_redis_queues

    clear_redis_queues()


def convert_to_ndarray(x):
    if isinstance(x, str):
        x = np.array(json.loads(x))
        # print(type(x).__name__, '\n\n\n\n')
    elif (
            isinstance(x, list)
            or isinstance(x, tuple)
            or isinstance(x, int)
            or isinstance(x, float)
    ):
        x = np.array(x)
        # print('DTYPE    SECOND',type(x), '\n\n\n\n')

    return x


def parse_string(x):
    x = json.loads(x)


def convert_ndarray_to_str(x):
    return str(x.tolist())


def find_dtype(x):
    if isinstance(x, np.ndarray):
        return "ndarray"
    elif isinstance(x, int):
        return "int"
    elif isinstance(x, float):
        return "float"
    else:
        return None


def get_rank_dtype(data):
    rank = len(np.array(data).shape)

    if rank == 0:
        return rank, np.array(data).dtype.name
    elif rank == 1:
        return rank, np.array(data).dtype.name
    else:
        return rank, np.array(data).dtype.name


def get_op_stats(ops):
    pending_ops = 0
    computed_ops = 0
    computing_ops = 0
    failed_ops = 0

    for op in ops:
        if op.status == "pending":
            pending_ops += 1
        elif op.status == "computed":
            computed_ops += 1
        elif op.status == "computing":
            computing_ops += 1
        elif op.status == "failed":
            failed_ops += 1

    total_ops = len(ops)

    return {
        "total_ops": total_ops,
        "pending_ops": pending_ops,
        "computing_ops": computing_ops,
        "computed_ops": computed_ops,
        "failed_ops": failed_ops,
    }


def serialize(model):
    """
    db_object => python_dict
    """
    # first we get the names of all the columns on your model
    columns = [c.key for c in class_mapper(model.__class__).columns]
    # then we return their values in a dict
    return dict((c, getattr(model, c)) for c in columns)


def find_complexity_and_output_dims(op):
    from ..db import ravdb
    inputs = json.loads(op.inputs)
    if inputs is not None:

        if functions[op.operator] in [Operators.ADDITION, Operators.SUBTRACTION, Operators.MULTIPLY, Operators.DIVISION,
                                      Operators.MEAN, Operators.AVERAGE
                                      ]:
            dims = json.loads(ravdb.get_op(inputs[0]).output_dims)
            dims2 = json.loads(ravdb.get_op(inputs[1]).output_dims)

            if isinstance(dims, int) or isinstance(dims, float):
                if isinstance(dims2, int) or isinstance(dims2, float):
                    return 1, 0
                else:
                    return reduce(lambda x, y: x * y, dims2), dims2
            else:
                if isinstance(dims2, int) or isinstance(dims2, float):
                    return reduce(lambda x, y: x * y, dims), dims
                else:
                    return reduce(lambda x, y: x * y, dims2), dims2

        if functions[op.operator] in [Operators.FIND_INDICES]:
            dims = json.loads(ravdb.get_op(inputs[0]).output_dims)
            rank, _ = get_rank_dtype(dims)
            return np.array(dims).size, (rank,)

        if functions[op.operator] in [Operators.NEGATION, Operators.POSITIVE,
                                      Operators.SQUARE_ROOT, Operators.SQUARE, Operators.NATURAL_LOG,
                                      Operators.EXPONENTIAL, Operators.SIGN]:

            dims = json.loads(ravdb.get_op(inputs[0]).output_dims)
            if isinstance(dims, int) or isinstance(dims, float):
                return 1, 0
            else:
                return reduce(lambda x, y: x * y, dims), dims

        elif functions[op.operator] in [Operators.MATRIX_MULTIPLICATION]:
            dims = []
            for input in inputs:
                parent_op = ravdb.get_op(input)
                dims.append(json.loads(parent_op.output_dims))

            dims_mat1 = dims[0]
            dims_mat2 = dims[1]

            if dims_mat1[1] == dims_mat2[0]:
                n = dims_mat1[0]
                m = dims_mat1[1]
                p = dims_mat2[1]

                return n * ((m + (m - 1)) * p), [n, p]
        elif functions[op.operator] in [Operators.CUBE_ROOT, Operators.CUBE]:
            dims = json.loads(ravdb.get_op(inputs[0]).output_dims)
            return 2 * dims[0] * dims[1], dims

        elif functions[op.operator] in [Operators.LINEAR]:
            dims = json.loads(ravdb.get_op(inputs[0]).output_dims)
            return 0, dims

        elif functions[op.operator] in [Operators.SHAPE]:
            dims = json.loads(ravdb.get_op(inputs[0]).output_dims)
            return 1, dims

        elif functions[op.operator] in [Operators.POWER]:
            dims = []
            for input in inputs:
                parent_op = ravdb.get_op(input)
                dims.append(json.loads(parent_op.output_dims))

            dims1 = json.loads(ravdb.get_op(inputs[0]).output_dims)
            return (dims[0][0] * dims[0][1]) * (dims[1] - 1), dims1
    else:
        return 0, ravdb.get_op_output(op.id).tolist() if len(
            ravdb.get_op_output(op.id).shape) == 0 else ravdb.get_op_output(op.id).shape


def get_random_string(length):
    # choose from all lowercase letter
    # letters = string.ascii_lowercase
    # result_str = "".join(random.choice(letters) for i in range(length))
    # print("Random string of length", length, "is:", result_str)

    return ''.join(secrets.choice(string.ascii_uppercase + string.digits)
                   for i in range(length))
