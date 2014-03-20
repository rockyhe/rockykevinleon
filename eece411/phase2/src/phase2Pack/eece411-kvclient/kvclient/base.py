from contextlib import contextmanager
from socket import create_connection
import struct
from kvclient.exceptions import KeyValueError


class KVCommands(object):
    PUT = 0x01
    GET = 0x02
    DELETE = 0x03
    SHUTDOWN = 0x04

KVSuccess = 0x00


def command_function(command_value):
    def call_run_command(self, *a, **k):
        return self.send_command(command_value, *a, **k)
    return call_run_command


class KeyValueClient(object):
    def __init__(self, address):
        address_components = address.split(':')
        if len(address_components) != 2:
            raise ValueError('The server address must contain a port.')
        self.location = address_components[0]
        try:
            self.port = int(address_components[1])
        except:
            raise ValueError('The server\'s port must be a number.')
        self.socket = None

    @contextmanager
    def connect(self):
        if self.socket is None:
            try:
                self.socket = create_connection((self.location, self.port))
                yield self.socket
            finally:
                if self.socket is not None:
                    self.socket.close()
                self.socket = None
        else:
            yield self.socket

    get = command_function(KVCommands.GET)
    put = command_function(KVCommands.PUT)
    delete = command_function(KVCommands.DELETE)
    shutdown = command_function(KVCommands.SHUTDOWN)

    def send_command(self, command_value, key=None, value=None):
        if command_value in [KVCommands.GET, KVCommands.DELETE]:
            data = struct.pack('b32s', command_value, key)
        elif command_value is KVCommands.PUT:
            data = struct.pack('b32s1024s', command_value, key, value)
        else:
            data = struct.pack('b', command_value)

        with self.connect() as socket:
            socket.sendall(data)
            code = self.receive_bytes(1)[0]
            if code is not KVSuccess:
                raise KeyValueError(code, key)
            if command_value is KVCommands.GET:
                received_data = self.receive_bytes(1024)
                return received_data

    def error_handler(self, response_status, key):
        if response_status[0] is not KVSuccess:
            error = KeyValueError(response_status[0], key)
            raise error

    def receive_bytes(self, size):
        data = bytearray()
        while len(data) < size:
            data.extend(self.socket.recv(size - len(data)))
        return data