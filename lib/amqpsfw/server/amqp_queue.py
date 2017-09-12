from queue import Queue


class AmqpQueue(Queue):
    def __init__(self, exchange_name, routing_key):
        super().__init__()
        self.exchange_name = exchange_name
        self.routing_key = routing_key
