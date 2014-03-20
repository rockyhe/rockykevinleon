kv_error_types = {}


class MetaKeyValueError(type):
    def __init__(self, name, inherits, attributes):
        error_code = attributes.get('ERROR_CODE', None)
        if error_code is not None:
            kv_error_types[error_code] = self


class KeyValueError(Exception):
    __metaclass__ = MetaKeyValueError

    def __new__(cls, *args):
        if cls is KeyValueError:
            code = args[0]
            cls = KeyValueError.get_type_from_code(code)
            return cls(*args[1:])
        return Exception.__new__(cls, *args)

    @staticmethod
    def get_type_from_code(code):
        if code in kv_error_types:
            return kv_error_types[code]
        else:
            return type('UnknownCode', (UnknownCode,), {'ERROR_CODE': code})

    def __str__(self):
        return self.MESSAGE.format(*self.args)


class UnknownCode(KeyValueError):
    MESSAGE = 'Received unknown error code "{0}"'


class InvalidKeyError(KeyValueError):
    ERROR_CODE = 0x01
    MESSAGE = 'The key "{1}" is not valid.'


class OutOfSpaceError(KeyValueError):
    ERROR_CODE = 0x02
    MESSAGE = 'The system is out of storage.'


class SystemOverloadError(KeyValueError):
    ERROR_CODE = 0x03
    MESSAGE = 'The system is overloaded.'


class ServerFailureError(KeyValueError):
    ERROR_CODE = 0x04
    MESSAGE = 'The system has failed.'


class UnknownCommandError(KeyValueError):
    ERROR_CODE = 0x05
    MESSAGE = 'The requested command is not recognised.'
