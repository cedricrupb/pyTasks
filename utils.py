import time
try:
    import mmh3 as mmh3
except ImportError:
    import pymmh3 as mmh3


def __prepareContainer(container):
    try:
        return [(k, v) for k, v in container.items()]
    except AttributeError:
        try:
            _ = (e for e in container)
            return container
        except TypeError:
            return [container]


def containerHash(container, seed=0x0, large=False):
    rep = sorted([str(v) for v in __prepareContainer(container)])
    rep = ', '.join(rep)
    if large:
        return mmh3.hash128(rep, seed)
    return mmh3.hash(rep, seed)


class TimeOutException(Exception):
    pass


def tick(obj):
    try:
        timeout = obj.timeout.value
        if timeout is None:
            return

        try:
            firstTick = obj.firstTick
        except AttributeError:
            obj.firstTick = time.time()
            firstTick = obj.firstTick

        if time.time() - firstTick > timeout:
            raise TimeOutException()
    except AttributeError:
        pass
