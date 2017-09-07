import select


def map_event_to_io_state(events):
    result = []
    if events & select.EPOLLIN:
        result.append('read')
    if events & select.EPOLLOUT:
        result.append('write')
    if events & select.EPOLLERR:
        result.append('error')
    if events & select.EPOLLPRI:
        result.append('pri')
    if events & select.EPOLLHUP:
        result.append('hup')
    if events & select.EPOLLRDHUP:
        result.append('rdhub')
    if events & select.EPOLLRDBAND:
        result.append('rband')
    if events & select.EPOLLWRBAND:
        result.append('wband')
    return ', '.join(result)
