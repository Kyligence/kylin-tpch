from enum import Enum

import lightning


class ServerMode(Enum):
    ALL = 'all'
    JOB = 'job'
    QUERY = 'query'


class KylinInstance:

    def __init__(self, **kwargs):
        self._host = kwargs['host']
        self._port = kwargs['port']
        self.client = lightning.connect(host=self._host, port=self._port)
