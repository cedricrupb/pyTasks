from task import Parameter
import pysftp
import os
from tqdm import tqdm


def makeProgress(name):
    memory = {}

    def progress(status, allBytes):
        if name not in memory:
            memory[name] = [tqdm(total=allBytes, desc=name), 0]

        update = status - memory[name][1]

        with memory[name][0] as pbar:
            pbar.update(update)

        memory[name][1] = status

    return progress


class AutoSend:

    def __init__(self, flo, local_path, remote_path, server, user, pwd):
        self._flo = flo
        self._remote = remote_path
        self._local = local_path
        self._server = server
        self._user = user
        self._pwd = pwd

    def _send(self):
        with pysftp.Connection(
            self._server,
            username=self._user,
            password=self._pwd
        ) as connection:
            remote_dir = os.path.dirname(self._remote)

            if not connection.isdir(remote_dir):
                connection.makedirs(remote_dir)

            name = os.path.basename(self._remote)

            connection.put(
                self._local,
                self._remote,
                callback=makeProgress('Send: %s' % name)
            )

    def __enter__(self):
        return self._flo.__enter__()

    def __exit__(self, type, value, traceback):
        exit = self._flo.__exit__(type, value, traceback)

        if value is None:
            self._send()

        return exit


class DistributedStorage:
    cache_dir = Parameter('')
    ftp_server = Parameter('')
    ftp_user = Parameter('')
    ftp_pwd = Parameter('')

    def __path(self, path):
        return os.path.join(self.cache_dir.value, path)

    def __connect(self):
        return pysftp.Connection(
            self.ftp_server.value,
            username=self.ftp_user.value,
            password=self.ftp_pwd.value
        )

    def __syncPath(self, remote_path, mode):
        local_path = self.__path(remote_path)

        if os.path.isfile(local_path):
            return local_path

        local_dir = os.path.dirname(local_path)

        if not os.path.isdir(local_dir):
            os.makedirs(local_dir)

        with self.__connect() as connection:
            if not connection.exists(remote_path):
                if 'w' in mode:
                    return local_path

                raise ValueError('Unknown remote path: %s' % remote_path)

            name = os.path.basename(local_path)

            connection.get(
                remote_path,
                localpath=local_path,
                callback=makeProgress(name)
            )

        return local_path

    def open(self, path, mode):
        local_path = self.__syncPath(path, mode)

        out = open(local_path, mode)

        if 'w' in mode:
            out = AutoSend(
                out, local_path, path, self.ftp_server.value,
                self.ftp_user.value, self.ftp_pwd.value
            )

        return out
