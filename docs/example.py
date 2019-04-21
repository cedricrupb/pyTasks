from task import Task, ParameterInjector, TaskPlanner,\
                  TaskProgressHelper, TaskExecutor
from target import ManagedTarget, LocalTarget
import json
import os
import time
import networkx as nx
from .utils import log


class FibTask(Task):

    def __init__(self, number):
        self.__number = number

    def require(self):
        if self.__number > 1:
            return FibTask(self.__number-1), FibTask(self.__number-2)

    def output(self):
        return ManagedTarget(self)

    def run(self):
        log(self, 'Run Fib %d' % self.__number)
        if self.__number < 2:
            with self.output() as o:
                o.emit(1)
        else:
            with self.input()[0] as i1:
                n1 = i1.query()
            with self.input()[1] as i2:
                n2 = i2.query()
            with self.output() as o:
                o.emit(n1 + n2)

    def __taskid__(self):
        return 'Fib_%d' % self.__number


class FakTask(Task):

    def __init__(self, number):
        self.__number = number

    def require(self):
        if self.__number > 0:
            return FakTask(self.__number-1)

    def output(self):
        return ManagedTarget(self)

    def run(self):
        print('Run Fak %d' % self.__number)
        if self.__number < 2:
            with self.output() as o:
                o.emit(1)
        else:
            with self.input()[0] as i1:
                n1 = i1.query()
            with self.output() as o:
                o.emit(self.__number * n1)

    def __taskid__(self):
        return 'Fak_%d' % self.__number


class CombTask(Task):

    def __init__(self, number):
        self.__number = number

    def require(self):
        return FibTask(self.__number), FakTask(self.__number)

    def output(self):
        return ManagedTarget(self)

    def run(self):
        print('Run Comb %d' % self.__number)
        with self.input()[0] as i1:
            n1 = i1.query()
        with self.input()[1] as i2:
            n2 = i2.query()
        with self.output() as o:
            o.emit(n1 + n2)

    def __taskid__(self):
        return 'Comb_%d' % self.__number


if __name__ == '__main__':

    # with open('./param_test.json', 'r') as cfg:
    #    config = json.load(cfg)
    config = {}

    injector = ParameterInjector(config)
    task = FibTask(10)

    if not os.path.isfile('./cache/graph.pickle'):
        planner = TaskPlanner(injector=injector)
        start_time = time.time()
        g = planner.plan(task)
        print("Planning time: %0.4f" % (time.time() - start_time))
        nx.write_gpickle(g, './cache/graph.pickle')
    else:
        g = nx.read_gpickle('./cache/graph.pickle')

    tpi = TaskProgressHelper(g)

    if not tpi.isFinished(task):
        start_time = time.time()
        exe = TaskExecutor()
        exe.executePlan(g)
        print("Execution time: %0.4f" % (time.time() - start_time))
        nx.write_gpickle(g, './cache/graph.pickle')
    else:
        g = nx.read_gpickle('./cache/graph.pickle')
        tpi = TaskProgressHelper(g)

    print('absolute: %0.4f' % tpi.caculateAbsoluteTime(task))

    o1 = tpi.output(task)
    injector.inject(o1)

    with o1 as o:
        print(o.query())
