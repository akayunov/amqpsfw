from collections import namedtuple


class Configuration:
    host = 'localhost'
    port = 5672
    credential = namedtuple('Credential', ['user', 'password'])('root', 'privetserver')
    sals_mechanism = 'PLAIN'
    amqp_version = (0, 0, 9, 1)
    heartbeat_interval = 100
    virtual_host = '/'
