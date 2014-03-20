from kvclient import KeyValueClient, InvalidKeyError, OutOfSpaceError, SystemOverloadError, ServerFailureError, \
    UnknownCommandError
from mock import patch
import pytest


class MockSocket(object):
    def __init__(self, response):
        self.response = response
        self.sent_value = None
        self.connection_info = None
        self.closed = None

    def __call__(self, connection_info):
        self.connection_info = connection_info
        self.closed = False
        return self

    def recv(self, size):
        if len(self.response) == 0:
            assert False, 'Test asked for more data than available'
        data = self.response.pop(0)
        if len(data) > size:
            self.response.insert(0, data[size:])
            data = data[:size]
        return data

    def sendall(self, value):
        self.sent_value = value

    def close(self):
        self.closed = True


class TestKVStore(object):
    def test_create_kvstore(self):
        kv_store = KeyValueClient('192.168.1.100:5665')

        assert kv_store.location == '192.168.1.100'
        assert kv_store.port == 5665

    def test_get(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x00', 'expected_value' + '\x00'*1010])
        with patch('kvclient.base.create_connection', socket):
            response = kv_store.get('test_key')

        assert socket.connection_info == ('192.168.1.100', 5665)
        assert socket.sent_value == '\x02' + 'test_key' + '\x00'*24
        assert response == 'expected_value' + '\x00'*1010
        assert socket.closed

    def test_put(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x00'])
        with patch('kvclient.base.create_connection', socket):
            response = kv_store.put('test_key', 'data_to_send')

        assert socket.connection_info == ('192.168.1.100', 5665)
        assert socket.sent_value == '\x01' + 'test_key' + '\x00'*24 + 'data_to_send' + '\x00'*1012
        assert response is None
        assert socket.closed

    def test_delete(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x00'])
        with patch('kvclient.base.create_connection', socket):
            response = kv_store.delete('test_key')

        assert socket.connection_info == ('192.168.1.100', 5665)
        assert socket.sent_value == '\x03' + 'test_key' + '\x00'*24
        assert response is None
        assert socket.closed

    def test_shutdown(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x00'])
        with patch('kvclient.base.create_connection', socket):
            response = kv_store.shutdown()

        assert socket.connection_info == ('192.168.1.100', 5665)
        assert socket.sent_value == '\x04'
        assert response is None
        assert socket.closed

    def test_run_command_invalid_key(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x01'])
        with patch('kvclient.base.create_connection', socket):
            with pytest.raises(InvalidKeyError):
                kv_store.get('test')

    def test_run_command_out_of_space(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x02'])
        with patch('kvclient.base.create_connection', socket):
            with pytest.raises(OutOfSpaceError):
                kv_store.get('test')

    def test_run_command_system_overload(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x03'])
        with patch('kvclient.base.create_connection', socket):
            with pytest.raises(SystemOverloadError):
                kv_store.get('test')

    def test_run_command_server_failure(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x04'])
        with patch('kvclient.base.create_connection', socket):
            with pytest.raises(ServerFailureError):
                kv_store.get('test')

    def test_run_command_unknown_command(self):
        kv_store = KeyValueClient('192.168.1.100:5665')
        socket = MockSocket(['\x05'])
        with patch('kvclient.base.create_connection', socket):
            with pytest.raises(UnknownCommandError):
                kv_store.get('test')


@pytest.mark.integration
class TestKVStoreLive(object):
    def test_life_cycle(self):
        kv_store = KeyValueClient('squirly.ca:9090')
        assert kv_store.put('my_value', 'test_data') is None
        assert kv_store.get('my_value').rstrip('\x00') == 'test_data'
        assert kv_store.put('my_value', 'another_test') is None
        assert kv_store.get('my_value').rstrip('\x00') == 'another_test'
        assert kv_store.delete('my_value') is None
        with pytest.raises(InvalidKeyError):
            kv_store.get('my_value')
        with pytest.raises(InvalidKeyError):
            kv_store.delete('my_value')
