from collections import namedtuple
from amqpsfw.configuration import Configuration


class ClientConfiguration(Configuration):
    host = 'localhost'
    credential = namedtuple('Credential', ['user', 'password'])('root', 'privetserver')
    heartbeat_interval = 100
    virtual_host = '/'
