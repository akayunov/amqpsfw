import logging

from amqpsfw.client.configuration import Configuration


def init_logger():
    for logger_name in Configuration.logger:
        log = logging.getLogger(logger_name)
        log.addHandler(getattr(logging, Configuration.logger[logger_name].handler)())
        log.setLevel(getattr(logging, Configuration.logger[logger_name].level))