import socket
import time
from collections import defaultdict


class ClientError(Exception):
    pass


class Client:
    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout

        try:
            self.connection = socket.create_connection((host, port), timeout)
        except socket.error as err:
            raise ClientError

    def get(self, key_name):
        message = "get " + key_name + '\n'
        self.connection.send(message.encode())
        try:
            data = self.connection.recv(1024).decode()
            if data == 'ok\n\n':
                return {}
            if data == 'error\nwrong command\n\n':
                raise ClientError

            metric_items = data.lstrip('ok\n').rstrip('\n\n')
            metric_items = [x.split() for x in metric_items.split('\n')]

            metric_dict = defaultdict(list)
            for key, metric, timestamp in metric_items:
                metric_dict[key].append((int(timestamp), float(metric)))
            for key in metric_dict:
                metric_dict[key].sort()
        except:
            raise ClientError

        return metric_dict

    def put(self, key_name, value, timestamp=None):
        if timestamp is None:
            timestamp = int(time.time())
        message = 'put '+ key_name + ' ' + str(value) + ' ' + str(timestamp) + '\n'
        self.connection.send(message.encode())
        data = self.connection.recv(1024).decode()
        if data == 'error\nwrong command\n\n':
            raise ClientError


