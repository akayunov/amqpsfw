import json
from collections import namedtuple

from amqpsfw.exceptions import SfwException


DEFAULT_CONFIG = {
    'host': 'localhost',
    'port': 5672,
    'credential': namedtuple('Credential', ['user', 'password'])('root', 'privetserver'),
    'sals_mechanism': 'PLAIN',
    'amqp_version': (0, 0, 9, 1),
    'heartbeat_interval': 1,
    'virtual_host': '/',
    'logger': {
        'root': namedtuple('LoggerProperties', ['handler', 'level'])('StreamHandler', 'DEBUG'),
        'amqpsfw.client.application': namedtuple('LoggerProperties', ['handler', 'level'])('StreamHandler', 'DEBUG'),
        'amqpsfw.ioloop': namedtuple('LoggerProperties', ['handler', 'level'])('StreamHandler', 'DEBUG')
    }
}


class _Configuration:
    def __init__(self):
        self.set_config(DEFAULT_CONFIG)

    def set_config(self, config):
        if type(config) is str:
            with open(config, 'r') as f:
                config = json.loads(f.read())
                if type(config) is not dict:
                    raise SfwException('Internal', 'Config type structure must be dictionary')
        for k in config:
            setattr(self, k, config[k])


Configuration = _Configuration()
