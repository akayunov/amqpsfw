from amqpsfw.configuration import Configuration


class ServerConfiguration(Configuration):
    host = 'localhost'
    port = 5672
    heartbeat_interval = 100
    listen_queue_size = 10
