from task import Parameter
import os
from os.path import dirname
import pickle
import networkx as nx
import json
import pandas as pd


class FileTarget:

    def __init__(self, path):
        self.id = path
        self.path = path
        self.mode = 'w'
        self.sandBox = ''

    def __enter__(self):
        path = self.sandBox + self.path
        if 'w' in self.mode:
            directory = dirname(path)

            if not os.path.exists(directory):
                os.makedirs(directory)

        self.fd = open(path, self.mode)
        return self.fd

    def __exit__(self, type, value, tb):
        self.fd.close()

    def exists(self):
        return os.path.isfile(self.sandBox + self.path)

    def __getstate__(self):
        return {'path': self.path,
                'mode': self.mode}

    def __setstate__(self, state):
        self.id = state['path']
        self.path = state['path']
        self.mode = state['mode']
        self.sandBox = ''


class PickleService:

    def __init__(self, s):
        self.__src = s

    def emit(self, obj):
        pickle.dump(obj, self.__src, pickle.HIGHEST_PROTOCOL)

    def query(self):
        return pickle.load(self.__src)

    def isByte(self):
        return True


class NetworkXService:

    def __init__(self, s):
        self.__src = s

    def emit(self, obj):
        nx.write_gpickle(obj, self.__src)

    def query(self):
        return nx.read_gpickle(self.__src)

    def isByte(self):
        return True


class JsonService:

    def __init__(self, s):
        self.__src = s

    def emit(self, obj):
        json.dump(obj, self.__src, indent=4)

    def query(self):
        return json.load(self.__src)

    def isByte(self):
        return False


class PandasCSVService:

    def __init__(self, s):
        self.__src = s

    def emit(self, obj):
        obj.to_csv(self.__src)

    def query(self):
        return pd.DataFrame.from_csv(self.__src)

    def isByte(self):
        return False


class LocalTarget(FileTarget):

    def __init__(self, path, service=PickleService):
        super().__init__(path)
        self.__service = service

    def __enter__(self):
        mode = self.mode
        if self.__service.isByte(self.__service) and 'b' not in mode:
            self.mode += 'b'
        return self.__service(super().__enter__())

    def __getstate__(self):
        D = super().__getstate__()
        D['service'] = self.__service
        return D

    def __setstate__(self, state):
        super().__setstate__(state)
        self.__service = state['service']


class CacheService:

    def __init__(self, parent, service):
        self.__parent = parent
        self.__service = service

    def emit(self, obj):
        self.__parent.cache = obj
        self.__service.emit(obj)

    def query(self):
        if self.__parent.cache is None:
            self.__parent.cache = self.__service.query()
        return self.__parent.cache

    def isByte(self):
        return self.__service.is_byte()


class CachedTarget:

    def __init__(self, target):
        self.target = target
        self.cache = None
        self.__opened = False

    def __enter__(self):
        if self.cache is None or self.target.mode == 'w':
            self.__opened = True
            return CacheService(self, self.target.__enter__())
        else:
            return CacheService(self, None)

    def __exit__(self, type, value, tb):
        if self.__opened:
            self.target.__exit__(type, value, tb)
            self.__opened = False

    def __getattr__(self, name):
        return getattr(self.target, name)

    def __setattr__(self, name, value):
        if name == 'target':
            self.__dict__[name] = value
        elif name in self.target.__dict__:
            setattr(self.target, name, value)
        else:
            super().__setattr__(name, value)

    def __getstate__(self):
        return self.target

    def __setstate__(self, state):
        self.target = state
        self.cache = None
        self.__opened = False

    def exists(self):
        if self.cache is None:
            return self.target.exists()
        return True


class InMemHelper:

    def __init__(self):
        self.__cache = None

    def emit(self, obj):
        self.__cache = obj

    def query(self):
        return self.__cache


class InMemTarget:

    def __init__(self):
        self.__mem = InMemHelper()

    def __enter__(self):
        return self.__mem

    def __exit__(self, type, value, tb):
        pass

    def __getstate__(self):
        return False

    def exists(self):
        return self.__mem.__cache is not None


class ManagedTarget(CachedTarget):
    out_dir = Parameter('./cache/')

    def __baseDir(self):
        try:
            param = self.__task.out_dir
            if isinstance(param, Parameter):
                return param
            else:
                return Parameter(param)
        except AttributeError:
            return self.out_dir

    def __init__(self, task):
        taskid = task.__taskid__()
        super().__init__(
                LocalTarget(taskid+'.pickle')
        )
        self.__task = task

    def __enter__(self):
        self.__old_sand = self.target.sandBox
        self.target.sandBox = self.__baseDir().value
        return super().__enter__()

    def __exit__(self, type, value, td):
        super().__exit__(type, value, td)
        self.target.sandBox = self.__old_sand

    def exists(self):
        self.__old_sand = self.target.sandBox
        self.target.sandBox = self.__baseDir().value
        ex = super().exists()
        self.target.sandBox = self.__old_sand
        return ex

    def __repr__(self):
        return 'ManagedTarget(%s)' % (self.target.path)
