import networkx as nx
from .task import Optional, UnsatisfiedRequirementException
from .task import Parameter, ParameterInjector, stats
from .utils import containerHash
import os
from tqdm import tqdm
import logging
import traceback
import pika
import json
from .storage import DistributedStorage
from . import target as t
import ast
from glob import iglob
import random
import time


def makeEmitter(obj):

    def func():
        return obj

    return func


def enumerateable(obj):
    if obj is None:
        return []
    try:
        _ = (e for e in obj)
        return obj
    except TypeError:
        return [obj]


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


def isFinish(taskNode, prefix='.'):
    output = taskNode['output']
    if hasattr(output, 'path'):
        path = taskNode['output'].path
        path = os.path.join(prefix, path)
        if os.path.isfile(path):
            return True

    return taskNode['finish'] or 'exception' in taskNode


def buildRegistry(prefix=None):
    registry = {}

    module_prefix = '.'
    pattern = '*.py'

    if prefix is not None:
        pattern = os.path.join(prefix, pattern)
        module_prefix = os.path.split(prefix)[1] + '.'

    for f in iglob(pattern):
        module = module_prefix + os.path.splitext(os.path.basename(f))[0]
        names = []
        with open(f, 'r') as fi:
            par = ast.parse(fi.read())
            for node in ast.walk(par):
                if not isinstance(node, ast.ClassDef):
                    continue
                if 'Task' not in [n.id for n in node.bases]:
                    continue

                names.append(node.name)
        if len(names) > 0:
            try:
                helper = __import__(module, globals(), locals(),
                                    names)

                for name in names:
                    registry[name] = helper.__dict__[name]
            except ImportError:
                print(traceback.format_exc())

    return registry


class MQHandler:
    log_level = Parameter('DEBUG')
    log_file = Parameter(None)

    def __init__(self, config, reactor):
        self._connection = None
        self._channel = None
        self._config = config[self.__class__.__name__]
        self._reactor = reactor
        ParameterInjector(config).inject(self)
        self._setup_logger()

    def _logger_level(self):
        return getattr(
            logging,
            self.log_level.value
        )

    def _setup_logger(self):
        self._logger = logging.getLogger(
            self.__class__.__name__
        )
        self._logger.setLevel(
            self._logger_level()
        )

        print('Setup logger')

        formatter = logging.Formatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        f = self.log_file.value
        if f is not None:
            fh = logging.FileHandler(f)
            fh.setLevel(
                self._logger_level()
            )
            fh.setFormatter(formatter)
            self._logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

    def _connect(self):
        self._logger.info('Connecting to %s', self._config['server'])

        parameter = pika.ConnectionParameters(
            host=self._config['server'],
            port=self._config['port'],
            virtual_host=self._config['vHost'],
            credentials=pika.PlainCredentials(
                self._config['user'], self._config['password']
            )
        )

        self._connection = pika.BlockingConnection(parameter)
        self._logger.info('Establish connection')

        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)

        self._channel.exchange_declare(
            exchange=self._config['exchange'],
            exchange_type='direct'
        )

        self._channel.queue_declare(
            queue=self._config['receiveQueue'],
            durable=True
        )

        self._channel.queue_bind(
            exchange=self._config['exchange'],
            queue=self._config['sendQueue']
        )

    def _consume(self):
        self._logger.info('Init reactor')
        react = self._reactor.startup()
        for r in enumerateable(react):
            self.sendMsg(r)

        self._channel.basic_consume(
            self.consume_msg, queue=self._config['receiveQueue']
        )

        self._channel.start_consuming()

    def connect(self):
        self._connect()
        self._consume()

    def _publish(self, msg):
        self._channel.basic_publish(exchange=self._config['exchange'],
                                    routing_key=self._config['sendQueue'],
                                    body=msg)

    def _publishAndReconnect(self, msg):
        try:
            self._publish(msg)
        except pika.exception.ConnectionClosed:
            self._logger.debug('Reconnect to queue')
            self._connect()
            self._publishAndReconnect(msg)

    def sendMsg(self, msg):
        if self._channel is None or not self._channel.is_open:
            return

        name = self._reactor.__class__.__name__

        properties = pika.BasicProperties(app_id='%s/%s' % (name, self._config['ID']),
                                          content_type='application/json',
                                          delivery_mode=2)

        msg = json.dumps(msg)
        self._publishAndReconnect(msg)
        self._logger.info('Send: %s' % msg)

    def consume_msg(self, ch, method, properties, body):
        obj = json.loads(body)

        react = None

        try:
            react = self._reactor.react(obj)
        except Exception:
            traceback.print_exc()
            self._channel.basic_nack(delivery_tag=method.delivery_tag)
            return

        for r in enumerateable(react):
            self.sendMsg(r)

        self._channel.basic_ack(delivery_tag=method.delivery_tag)

    def shutdown(self):
        self._reactor.shutdown()
        self._connection.close()


class LocalHandler:

    def __init__(self, server, consumer):
        self._server = server
        self._consumer = consumer
        self._consume2server = []
        self._server2consumer = []
        self._last = 0

    def start_server(self):
        react = self._server.startup()
        if react is not None:
            self._server2consumer.extend(enumerateable(react))

    def start_consumer(self):
        for c in self._consumer:
            react = c.startup()
            if react is not None:
                self._consume2server.extend(enumerateable(react))

    def run(self):
        self.start_server()
        self.start_consumer()

        while len(self._server2consumer) + len(self._consume2server) > 0:
            if len(self._consume2server) > 0:
                M = self._consume2server.pop()
                R = self._server.react(M)
                self._server2consumer.extend(enumerateable(R))

            if len(self._server2consumer) > 0:
                M = self._server2consumer.pop()
                R = self._consumer[self._last].react(M)
                self._consume2server.extend(enumerateable(R))
                self._last += 1
                if self._last >= len(self._consumer):
                    self._last = 0

        self._server.shutdown()
        for c in self._consumer:
            c.shutdown()


class SheduleServer:
    ftp_dir = Parameter('.')
    home_dir = Parameter('')
    log_level = Parameter('DEBUG')
    log_file = Parameter(None)
    backup = Parameter(240)

    def __init__(self, graph_ref, config, controller=None):
        self._config = config
        self._ref = graph_ref
        self._injector = ParameterInjector(config)
        self._injector.inject(self)
        self._controller = controller
        self._graph = nx.read_gpickle(self._ref)
        if self._controller is not None:
            self._controller.load_graph(controller)
        self._setup_logger()
        self._init_buffer()

    def _logger_level(self):
        return getattr(
            logging,
            self.log_level.value
        )

    def _setup_logger(self):
        self._logger = logging.getLogger(
            self.__class__.__name__
        )
        self._logger.setLevel(
            self._logger_level()
        )

        formatter = logging.Formatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        f = self.log_file.value
        if f is not None:
            fh = logging.FileHandler(f)
            fh.setLevel(
                self._logger_level()
            )
            fh.setFormatter(formatter)
            self._logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

    def _init_buffer(self):
        self._logger.debug(
            'Initialize buffer'
        )
        self._pbar = tqdm(total=len(self._graph))
        self._buffer = []

        seen = set([])
        stack = [(n, True) for (n, c) in self._graph.out_degree() if c == 0]

        while len(stack) > 0:
            n, state = stack.pop()
            n_node = self._graph.node[n]

            if n in seen:
                continue

            seen.add(n)

            if isFinish(n_node, self.home_dir.value):
                n_node['finish'] = True
                self._pbar.update(1)
                state = False

            add = state
            for p in self._graph.predecessors(n):
                stack.append((p, state))
                if add and not isFinish(self._graph.node[p]):
                    add = False

            if add:
                self._buffer.append(n)

        self._logger.debug(
            'Finished initializing buffer'
        )

    def _get_parameter(self, task):
        out = {}

        keys = [
            task.__class__.__name__,
            task.output().__class__.__name__
        ]

        for k in keys:
            if k in self._config:
                out[k] = self._config[k]

        return out

    @staticmethod
    def _is_optional(dependency):
        if isinstance(dependency, Optional):
            return dependency.optional, True
        return dependency, False

    def _index_input(self, task_id):
        G = self._graph
        task = G.node[task_id]['task']

        require = enumerateable(task.require())

        if len(require) == 0:
            return []

        index = [None] * len(require)

        pre_index = {}

        for p in G.predecessors(task_id):
            pre_index[p] = G.node[p]

        for i, r in enumerate(require):
            r, optional = SheduleServer._is_optional(r)
            self._injector.inject(r)
            r_id = taskid(r)
            if r_id in pre_index:
                pre_node = pre_index[r_id]
                if isFinish(pre_node) and 'exception' not in pre_node:
                    index[i] = pre_node['output']
                elif not optional:
                    raise UnsatisfiedRequirementException(
                        'Job %s is not finished' % r_id
                    )
            else:
                raise UnsatisfiedRequirementException(
                        '%s is not in predecessors of %s expected: %s' % (r_id, task_id, str(pre_index)))

        return index

    @staticmethod
    def _relative_path(path):
        if os.path.isabs(path):
            f = os.path.basename(path)
            rel = os.path.dirname(path)
            rel = os.path.basename(os.path.normpath(rel))
            return os.path.join('.', rel, f)
        return path

    def _cp_input(self, output):
        try:
            path = output.path

            local_path = os.path.join(self.home_dir.value,
                                      SheduleServer._relative_path(path))

            ftp_path = os.path.join(self.ftp_dir.value,
                                    SheduleServer._relative_path(path)
                                    )

            if not (os.path.exists(ftp_path)
                    and os.path.samefile(local_path, ftp_path)):
                os.link(local_path, ftp_path)

            return path

        except AttributeError:
            raise UnsatisfiedRequirementException(
                '%s has to provide a path attribute' % str(output)
            )

    def _provide_input(self, task_id):
        task_in = self._index_input(task_id)

        in_ref = [None] * len(task_in)

        for i, t_i in enumerate(task_in):
            if t_i is not None and hasattr(t_i, 'path'):
                in_ref[i] = self._cp_input(t_i)

        return in_ref

    def _provide_output(self, task_id):
        t_out = self._graph.node[task_id]['output']

        self._injector.inject(t_out)

        try:
            return t_out.path
        except AttributeError:
            return None
            raise ValueError(
                '%s has to provide a path attribute' % str(t_out)
            )

    def _serialize_task(self, task_id):
        G = self._graph
        task = G.node[task_id]['task']

        self._injector.inject(task)

        return {
            'id': task_id,
            'type': task.__class__.__name__,
            'args': task.get_params(),
            'params': self._get_parameter(task),
            'input': self._provide_input(task_id),
            'output': self._provide_output(task_id)
        }

    def _save_progress(self):
        self._logger.debug(
            'Save graph to %s' % self._ref
        )
        if self._controller is not None:
            self._controller.shutdown()
        nx.write_gpickle(self._graph, self._ref)

    def shutdown(self):
        self._logger.info(
            'Shutdown server'
        )
        self._save_progress()
        self._pbar.close()

    def _update_buffer(self, t_id):
        for s in self._graph.successors(t_id):
            if isFinish(self._graph.node[s], self.home_dir.value):
                continue
            add = True
            for ps in self._graph.predecessors(s):
                if not isFinish(self._graph.node[ps], self.home_dir.value):
                    add = False
                    break
            if add:
                self._logger.debug(
                    'Add task %s to chain' % s
                )
                self._buffer.append(s)

    def _finish_task(self, msg):
        t_id = msg['id']
        t_node = self._graph.node[t_id]

        if not isFinish(t_node):
            if 'exception' in msg:
                t_node['exception'] = msg['exception']
                self._logger.error(
                    'Error for task [%s]: %s' % (t_id, t_node['exception'])
                )
            else:
                t_node['finish'] = True
                t_node['stats'] = msg['stats']
                t_node['time'] = msg['time']
                self._logger.info(
                    'Finished job %s after %f seconds' % (t_id, t_node['time'])
                )
                self._pbar.update(1)
                if self._controller is not None and\
                        self._controller.progress(t_id):
                        self._logger.debug(
                            'Controller created a new task.'
                        )
                        self._init_buffer()

        self._update_buffer(t_id)

    def _emit_task(self):
        while len(self._buffer) > 0:
            t_id = self._buffer.pop()
            t_node = self._graph.node[t_id]

            if 'send' in t_node:
                continue

            try:
                ser = [self._serialize_task(t_id)]
                t_node['send'] = True
                return ser
            except UnsatisfiedRequirementException:
                trace = traceback.format_exc()
                self._logger.error(
                    'Cannot serialize task %s: %s' % (t_id, trace)
                )
                node = self._graph.node[t_id]
                node['exception'] = trace
                self._update_buffer(t_id)

        return []

    def _backup_graph(self):
        split = os.path.splitext(self._ref)
        path = split[0] + '_backup' + split[1]
        self._logger.debug(
            'Backup graph to %s' % path
        )
        nx.write_gpickle(self._graph, path)

    def _shedule_backup(self):
        if 'backup_time' not in self.__dict__:
            self.backup_time = time.time()

        if time.time() - self.backup_time > self.backup.value:
            self._backup_graph()
            self.backup_time = time.time()

    def startup(self):
        self._logger.info('Startup server')
        return []

    def react(self, msg):

        self._shedule_backup()

        if 'handshake' in msg:
            self._logger.info('New handshake of worker. Emit msg')
            response = []
            for _ in range(5):
                response.extend(self._emit_task())
            return response
        elif 'result' in msg:
            result = msg['result']

            if 'id' not in result:
                self._logger.error(
                    'id is missing. Skip message %s' % str(msg)
                )
                return self._emit_task()

            t_id = result['id']
            self._logger.info('New finished message: %s' % str(msg))

            if t_id not in self._graph.nodes():
                self._logger.error(
                    'Unknown task id: %s. Skip.' % t_id
                )
                return self._emit_task()

            self._finish_task(result)

            return self._emit_task()
        else:
            self._logger.error(
                'Unknown message: %s' % str(msg)
            )
            return None


class OpenTarget:

    def __init__(self, opener, path, service=None):
        self.id = path
        self.path = path
        self.mode = 'w'
        self.sandBox = ''
        self.opener = opener
        self.service = service

    def __enter__(self):
        mode = self.mode
        if self.service is not None\
           and self.service.isByte(self.service) and 'b' not in mode:
            mode += 'b'

        path = self.sandBox + self.path
        if 'w' in self.mode:
            directory = os.path.dirname(path)

            if not os.path.exists(directory):
                os.makedirs(directory)

        self.fd = self.opener._open(path, mode)

        if self.service is not None:
            return self.service(self.fd)

        return self.fd

    def __exit__(self, type, value, tb):
        self.fd.close()

    def exists(self):
        return os.path.isfile(self.sandBox + self.path)


class Worker:
    cache_dir = Parameter('.')
    ftp = Parameter(None)
    log_level = Parameter('DEBUG')
    log_file = Parameter(None)

    def __init__(self, config, registry):
        self._registry = registry
        self._injector = ParameterInjector(config)
        self._injector.inject(self)
        self._setup_logger()
        self._setup_ftp()
        self._logger.info('Created new worker')

    def _logger_level(self):
        return getattr(
            logging,
            self.log_level.value
        )

    def _setup_logger(self):
        self._logger = logging.getLogger(
            self.__class__.__name__
        )
        self._logger.setLevel(
            self._logger_level()
        )

        formatter = logging.Formatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        f = self.log_file.value
        if f is not None:
            fh = logging.FileHandler(f)
            fh.setLevel(
                self._logger_level()
            )
            fh.setFormatter(formatter)
            self._logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

    def _setup_ftp(self):
        self._ftp = None
        if self.ftp.value is not None:
            ftp_cfg = self.ftp.value
            cfg = {
                'DistributedStorage': {
                    'cache_dir': self.cache_dir.value,
                    'ftp_server': ftp_cfg['host'],
                    'ftp_user': ftp_cfg['username'],
                    'ftp_pwd': ftp_cfg['password']
                }
            }

            if 'prefix' in ftp_cfg:
                cfg['DistributedStorage']['ftp_prefix'] = ftp_cfg['prefix']

            self._ftp = DistributedStorage()
            ParameterInjector(cfg).inject(self._ftp)
            self._logger.info('Setup FTP.\n %s' % str(cfg))

    def _mkdir(self, path):
        dir_name = os.path.dirname(path)

        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)

    def _open(self, path, mode):
        if self._ftp is not None:
            return self._ftp.open(path, mode)

        path = os.path.join(self.cache_dir.value, path)

        if 'w' in mode:
            self._mkdir(path)

        return open(path, mode)

    def _rebuildTarget(self, target):
        if isinstance(target, t.CachedTarget):
            target.target = self._rebuildTarget(
                target.target
            )
        if isinstance(target, t.LocalTarget):
            target = OpenTarget(
                self, target.path, target.service
            )
        elif isinstance(target, t.FileTarget):
            target = OpenTarget(self, target.path)

        return target

    @staticmethod
    def _is_optional(dependency):
        if isinstance(dependency, Optional):
            return dependency.optional, True
        return dependency, False

    def _build_input(self, task, inputList):
        require = []

        for t in enumerateable(task.require()):
            t, opt = Worker._is_optional(t)
            require.append(t.output())

        if len(require) != len(inputList):
            raise ValueError('Input has to have the same size as require' +
                             ': got %d but expected %d' % (len(inputList),
                                                           len(require)))

        inList = [None] * len(require)

        for i, r in enumerate(require):
            path = inputList[i]
            if path is None:
                continue
            r.path = path
            target = self._rebuildTarget(r)
            target.mode = 'r'
            inList[i] = target

        return inList

    def _build_task(self, msg):
        t = msg['type']
        if t not in self._registry:
            raise ValueError('Unknown Task %s' % t)

        self._logger.debug('Build task of type %s' % t)

        args = msg['args']
        task = self._registry[t](**args)

        injector = ParameterInjector(msg['params'])
        injector.inject(task)
        self._injector.inject(task)
        self._logger.debug('Task is created and parameter are injected.')

        out = task.output()
        injector.inject(out)
        out.path = msg['output']
        out = self._rebuildTarget(out)
        task.output = makeEmitter(out)

        inp = self._build_input(
            task, msg['input']
        )
        task.input = makeEmitter(inp)

        self._logger.debug('Task is successfully build.')

        return task

    @staticmethod
    def makeReturn(t_id, D):
        Out = {
                'result': {
                    'id': t_id
                }
            }

        Out['result'].update(D)
        return [Out]

    def startup(self):
        self._logger.info("Send handshake.")
        return [
            {'handshake': random.uniform(0, 100000)}
        ]

    def react(self, msg):
        if 'id' in msg:
            t_id = msg['id']
            try:
                self._logger.info('New task [%s] has arrived.' % str(t_id))
                task = self._build_task(msg)

                start_time = time.time()

                self._logger.info('Run task [%s]' % str(t_id))
                task.run()
                self._logger.info('Finish task [%s]' % str(t_id))

                out_path = None
                out = task.output()

                if hasattr(out, 'path'):
                    out_path = out.path

                taskD = {
                    'output': out_path,
                    'stats': stats(task),
                    'time': time.time() - start_time
                }

                return Worker.makeReturn(t_id, taskD)

            except Exception:
                trace = traceback.format_exc()
                self._logger.error('Error in %s: %s' % (t_id, trace))
                return Worker.makeReturn(t_id, {'exception': trace})
        else:
            self._logger.error(
                'Unknown msg %s' % str(msg)
            )

    def shutdown(self):
        self._logger.info('Shutdown server')
        pass
