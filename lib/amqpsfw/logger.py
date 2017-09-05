import logging
from collections import namedtuple

from amqpsfw.client.configuration import Configuration as CConf
from amqpsfw.server.configuration import Configuration as SConf

ROOT_LOGGER = namedtuple('RootLogger', ['logger'])({
        None: namedtuple('LoggerProperties', ['handler', 'level'])('StreamHandler', 'DEBUG'),
    })

def init_logger():
    for config in [ROOT_LOGGER, CConf, SConf]:
        for logger_name in config.logger:
            log = logging.getLogger(logger_name)
            # log.propagate = False
            if not log.hasHandlers():
                log_handler = getattr(logging, config.logger[logger_name].handler)()
                formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(module)s.py.%(funcName)s:%(lineno)d - %(message)s')
                log_handler.setFormatter(formatter)
                log.addHandler(log_handler)
                log.setLevel(getattr(logging, config.logger[logger_name].level))
