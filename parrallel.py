from task import Task, TaskPlanner
from task import TaskExecutor
import task as T
from utils import containerHash
import networkx as nx
import networkx.algorithms.dag as dag
import math


class ExecutionTask(Task):

    def __init__(self, plan, exe=TaskExecutor):
        self.__plan = plan
        self.__exe = exe
        self.__initTask()

    def __initTask(self):
        graph = self.__plan

        exits = [n for n in graph if graph.out_degree(n) == 0]
        if len(exits) > 1:
            raise ValueError('Too many exits %s' % str(exits))

        self.__out = graph.node[exits[0]]['output']

        entries = [n for n in graph if graph.in_degree(n) == 0]
        req = {n: graph.node[n]['task'].require() for n in entries}
        self.__reqMap = {}
        self.__req = []
        for k, R in req.items():
            self.__reqMap[k] = []
            if R is None:
                continue
            for r in R:
                self.__reqMap[k].append(len(self.__req))
                self.__req.append(r)

    def require(self):
        return self.__req

    def output(self):
        return self.__out

    def __taskid__(self):
        return "ExecutionTask_%s" %\
                (str(containerHash(self.__plan, large=True)))

    def __prepPlan(self):
        graph = self.__plan.copy()
        c = 0
        for k, R in self.__reqMap.items():
            for r in R:
                nid = self.__taskid__ + ('_%d' % c)
                c += 1
                inp = self.input()[r]
                graph.add_node(nid, finish=True, output=inp)
                graph.add_edge(nid, k, optional=False)
        return graph

    def run(self):
        self.__exe.executePlan(self.__prepPlan())


def is_merge(graph, n):
    return graph.in_degree(n) > 1


def __groups(graph, m):
    group = {}
    counter = 0

    for n in dag.topological_sort(graph):
        attach = -1
        min_deg = math.inf

        if not is_merge(graph, n):
            for p in graph.predecessors(n):
                if p not in group:
                    continue
                deg = graph.out_degree(p)
                if deg <= m and deg < min_deg:
                    attach = group[p]
                    min_deg = deg

        if attach == -1:
            attach = counter
            counter += 1

        group[n] = attach
    return group


def serialCount(graph, groups):
    serial = 0
    for n, nbrsdict in graph.adjacency():
        for nbr, eattr in nbrsdict.items():
            if groups[n] != groups[nbr]:
                serial += 1

    return serial


def groups(graph):
    max_deg = max([graph.out_degree(n) for n in graph])
    groups = None
    ser = math.inf
    for i in range(1, max_deg):
        new_group = __groups(graph, i)
        serial = serialCount(graph, new_group)
        if serial < ser:
            groups = new_group
            ser = serial
        else:
            break

    return groups


def groups2(graph):
    save = {}
    groups = {}
    counter = 0

    for n in dag.topological_sort(graph):
        attach = -1

        for p in graph.predecessors(n):
            if p not in save:
                if attach == -1:
                    attach = groups[p]
                else:
                    # collision
                    attach = -2
                    break
        if attach == -2:
            save.update(groups)
            groups = {}

        if attach < 0:
            attach = counter
            counter += 1

        groups[n] = attach

    save.update(groups)
    return save


def compact(graph, groups):
    G = nx.DiGraph()

    for n in graph:
        G.add_node(groups[n])

    for n, nbrsdict in graph.adjacency():
        for nbr, eattr in nbrsdict.items():
            if groups[n] != groups[nbr]:
                G.add_edge(groups[n], groups[nbr])

    return G
