import logging


def init_logger():
    log = logging.getLogger('amqpsfw')
    if not log.hasHandlers():
        log_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(module)s.py.%(funcName)s:%(lineno)d - %(message)s')
        log_handler.setFormatter(formatter)
        log.addHandler(log_handler)
        log.setLevel(logging.DEBUG)
