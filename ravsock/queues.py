from .config import QUEUE_HIGH_PRIORITY, QUEUE_OPS, QUEUE_CLIENTS, QUEUE_LOW_PRIORITY, QUEUE_COMPUTING
from .db import RavQueue

queue_high_priority = RavQueue(name=QUEUE_HIGH_PRIORITY)
queue_low_priority = RavQueue(name=QUEUE_LOW_PRIORITY)
queue_computing = RavQueue(name=QUEUE_COMPUTING)

queue_ops = RavQueue(name=QUEUE_OPS)
queue_clients = RavQueue(name=QUEUE_CLIENTS)
