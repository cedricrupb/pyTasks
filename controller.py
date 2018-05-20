from abc import ABCMeta, abstractmethod
from .task import TaskProgressHelper


class TaskController:
    __metaclass__ = ABCMeta

    @abstractmethod
    def nextTask(self, last_task=None):
        pass

    def shutdown(self):
        pass


class Controller:

    def __init__(self, taskController, planner):
        self.taskController = taskController
        self.planner = planner
        self.listen = None

    def load_graph(self, graph):
        self.graph = graph
        self.helper = TaskProgressHelper(self.graph)

        for n in graph:
            node = graph.nodes[n]
            if 'controlled' in node and\
                    node['controlled']:
                self.listen = n

    def progress(self, task_id):
        task = None
        if self.listen is None:
            task = self.taskController.nextTask()
            if task is not None:
                self.listen = self.helper.get_task_id(task)
            else:
                self.listen = '_'
        if task_id == self._listen:
            del self.graph.nodes[task_id]['controlled']
            output = self.helper.output(task_id)
            task = self.taskController.nextTask((task_id, output))
            if task is None:
                self._listen = '_'

        if task is not None:
            self.planner.plan(task, graph=self.graph)
            self.listen = self.helper.get_task_id(task)
            self.graph.nodes[self.listen]['controlled'] = True
            return True

        return False

    def shutdown(self):
        self.taskController.shutdown()
