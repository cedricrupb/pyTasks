from abc import ABCMeta, abstractmethod
import networkx as nx
import networkx.algorithms.dag as dag
import time
import traceback
from tqdm import tqdm
from .utils import containerHash
from inspect import signature


def stats(task):
    try:
        return task.__stats__()
    except Exception:
        return {}


class Optional:
    def __init__(self, obj):
        self.optional = obj


class Parameter:

    def __init__(self, default=None):
        self.value = default

    def __repr__(self):
        return 'Parameter(%s)' % (str(self.value))


class ParameterInjector:

    def __init__(self, config):
        self.__config = config

    @staticmethod
    def __collectParams(d):
        params = []
        for k, v in d.items():
            if isinstance(v, Parameter):
                params.append(k)
        return params

    def inject(self, obj):
        if self.__config is None:
            return

        name = obj.__class__.__name__

        if name not in self.__config:
            return

        d = self.__config[name]

        params = ParameterInjector.__collectParams(obj.__dict__)
        params.extend(
                ParameterInjector.__collectParams(obj.__class__.__dict__)
                )

        for p in params:
            if p in d:
                obj.__dict__[p] = Parameter(d[p])


class Task:
    __metaclass__ = ABCMeta

    @abstractmethod
    def require(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def output(self):
        pass

    def input(self):
        pass

    @classmethod
    def _get_param_names(cls):
        """Get parameter names for the estimator"""
        init = getattr(cls.__init__, 'deprecated_original', cls.__init__)
        if init is object.__init__:
            return []

        # introspect the constructor arguments to find the model parameters
        # to represent
        init_signature = signature(init)
        # Consider the constructor parameters excluding 'self'
        parameters = [p for p in init_signature.parameters.values()
                      if p.name != 'self' and p.kind != p.VAR_KEYWORD]
        for p in parameters:
            if p.kind == p.VAR_POSITIONAL:
                raise RuntimeError("Tasks should always "
                                   "specify their parameters in the signature"
                                   " of their __init__ (no varargs)."
                                   " %s with constructor %s doesn't "
                                   " follow this convention."
                                   % (cls, init_signature))
        # Extract and sort argument names excluding 'self'
        return sorted([p.name for p in parameters])

    def get_params(self):
        out = dict()
        for key in self._get_param_names():
            out[key] = getattr(self, key, None)

        return out

    def set_params(self, **params):
        if not params:
            return self

        valid_params = self.get_params()

        for key, value in params.items():
            key, delim, sub_key = key.partition('__')
            if key not in valid_params:
                raise ValueError('Invalid parameter %s for task %s. '
                                 'Check the list of available parameters '
                                 'with `task.get_params().keys()`.' %
                                 (key, self))
            setattr(self, key, value)

        return self


class TargetTask(Task):

    def __init__(self, target):
        self.__target = target

    def require(self):
        pass

    def run(self):
        pass

    def output(self):
        return self.__target


class TaskPlanner:

    def __init__(self, injector=None):
        self.injector = injector

    def __inject(self, obj):
        if self.injector is not None:
            self.injector.inject(obj)

    def __prepTask(self, task):
        self.__inject(task)

        output = task.output()
        self.__inject(output)
        return task, output

    @staticmethod
    def taskid(task):
        if hasattr(task, 'cachedid'):
            return task.cached_id
        elif hasattr(task, '__taskid__'):
            return task.__taskid__()
        else:
            name = task.__class__.__name__
            ha = containerHash(task.__dict__, large=True)
            task.cached_id = '%s_%d' % (name, ha)
            return task.cached_id

    @staticmethod
    def __getrequire(task):
        req = task.require()
        try:
            _ = (e for e in req)
            return req
        except TypeError:
            if req is None:
                return []
            return [req]

    @staticmethod
    def __isoptional(task):
        if isinstance(task, Optional):
            return task.optional, True
        return task, False

    @staticmethod
    def __checkCycle(plan):
        if not dag.is_directed_acyclic_graph(plan):
            raise CycleDependencyException()

    def plan(self, task, graph=nx.DiGraph()):

        task, out = self.__prepTask(task)

        graph.add_node(
                        TaskPlanner.taskid(task),
                        task=task,
                        output=out,
                        finish=False
                       )

        stack = [task]
        while len(stack) > 0:
            t = stack.pop()
            tid = TaskPlanner.taskid(t)
            for t_n in TaskPlanner.__getrequire(t):
                t_n, optional = TaskPlanner.__isoptional(t_n)
                t_n, output = self.__prepTask(t_n)
                t_n_id = TaskPlanner.taskid(t_n)
                if t_n_id not in graph:
                    graph.add_node(t_n_id,
                                   task=t_n,
                                   output=output,
                                   finish=False)
                    stack.append(t_n)

                graph.add_edge(t_n_id, tid, optional=optional)

        TaskPlanner.__checkCycle(graph)
        return graph


class CycleDependencyException(Exception):
    pass


class UnsatisfiedRequirementException(Exception):
    def __init__(self, exception):
        self.message = exception


class TaskExecutor:

    def __init__(self, planner=TaskPlanner()):
        self.planner = planner

    def __prepTask(self, task, inp, output):
        oldI = task.input
        oldO = task.output
        task.input = self.__makeFunc(inp)
        task.output = self.__makeFunc(output)
        return task, oldI, oldO

    @staticmethod
    def __checkCycle(plan):
        if not dag.is_directed_acyclic_graph(plan):
            raise CycleDependencyException()

    @staticmethod
    def __makeFunc(inp):

        def test():
            return inp

        return test

    def __createInputList(self, graph, pre, optionals):
        li = []
        for i, p in enumerate(pre):
            optional = optionals[i]
            if graph.node[p]['finish']:
                o = graph.node[p]['output']
                li.append(o)
            elif optional:
                li.append(None)
            else:
                raise UnsatisfiedRequirementException(
                        'Job %s is not finished' % p
                        )
        return li

    @staticmethod
    def __isFinish(taskNode):
        return taskNode['finish'] or 'exception' in taskNode\
               or taskNode['output'].exists()

    @staticmethod
    def __no_income(plan, task):
        taskNode = plan.node[task]
        if TaskExecutor.__isFinish(taskNode):
            taskNode['finish'] = True
            taskNode['output'].mode = 'r'
            return False

        size = 0
        for p in plan.predecessors(task):
            if not TaskExecutor.__isFinish(plan.node[p]):
                size += 1

        return size == 0

    @staticmethod
    def __to_shedule(plan):
        S = [n for n in plan if TaskExecutor.__no_income(plan, n)]

        while len(S) > 0:
            act = S.pop()

            yield act

            for s in plan.successors(act):
                if TaskExecutor.__no_income(plan, s):
                    S.append(s)

    def executeTask(self, task):
        return self.executePlan(self.planner.plan(task))

    def executePlan(self, plan):
        TaskExecutor.__checkCycle(plan)

        for job in tqdm(TaskExecutor.__to_shedule(plan), total=len(plan)):

            taskNode = plan.node[job]
            task = taskNode['task']

            if taskNode['finish'] or task.output().exists():
                taskNode['finish'] = True
                print('Job %s is already cached. Skip.' % job)
                continue

            if 'exception' in taskNode:
                print('Job %s cancelled because of Exception' % job)
                print(taskNode['exception'])
                continue

            pre = [x for x in plan.predecessors(job)]

            optionals = [True if plan.edges[x, job]['optional'] else False
                         for x in pre]

            try:
                inputList = self.__createInputList(plan,
                                                   pre, optionals)
            except UnsatisfiedRequirementException as e:
                print('Failed job: '+job)
                trace = traceback.format_exc()
                print(trace)
                taskNode['exception'] = trace
                continue

            task, i, o = self.__prepTask(task, inputList, taskNode['output'])

            start_time = time.time()

            try:
                task.run()
                taskNode['finish'] = True
                taskNode['stats'] = stats(task)
                taskNode['output'].mode = 'r'
            except Exception as ex:
                print('Failed job: '+job)
                trace = traceback.format_exc()
                print(trace)
                taskNode['exception'] = trace
                continue
            finally:
                task.input = i
                task.output = o

            end_time = time.time() - start_time
            taskNode['time'] = end_time


class TaskProgressHelper:

    def __init__(self, plan):
        self.__plan = plan

    def caculateAbsoluteTime(self, task):
        if isinstance(task, Task):
            task = TaskPlanner.taskid(task)

        timeOut = 0.0
        stack = [task]
        seen = set([])
        while len(stack) > 0:
            act = stack.pop()
            if act not in seen and self.__plan.nodes[act]['finish']:
                seen.add(act)
                timeOut += self.__plan.nodes[act]['time']
                for n in self.__plan.predecessors(act):
                    stack.append(n)
        return timeOut

    def isFinished(self, task):
        if isinstance(task, Task):
            task = TaskPlanner.taskid(task)

        return self.__plan.nodes[task]['finish']

    def output(self, task):
        if isinstance(task, Task):
            task = TaskPlanner.taskid(task)

        return self.__plan.nodes[task]['output']
