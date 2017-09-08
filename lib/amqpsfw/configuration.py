from collections import namedtuple

# TODO devide it by client and server properties


class Configuration:
    host = 'localhost'
    port = 5672
    credential = namedtuple('Credential', ['user', 'password'])('root', 'privetserver')
    security_mechanism = 'PLAIN'
    amqp_version = (0, 0, 9, 1)
    heartbeat_interval = 100
    virtual_host = '/'
    listen_queue_size = 10
    secure_response = ''
    client_properties = {'client': 'anqpsfw:0.1'}
    server_properties = {'server': 'anqpsfw:0.1'}
