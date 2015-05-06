from future.utils import PY3
from cantrips.features import Feature
import json


binary = bytes if PY3 else str
string = str if PY3 else basestring
text = str if PY3 else unicode
integer = (int,) if PY3 else (int, long)


_32bits = (1 << 32) - 1


def split_command(command):
    """
    Breaks a command in two parts: namespace and code. Either a string or a 64bit integer
      are expected (in amd64 architectures, int is 64bit wide, althought in python3, all the
      integer numbers are of type int). Bits above the 63rd are ignored (the first is 0th).
    """
    if isinstance(command, string):
        return command.rsplit('.', 1)
    elif isinstance(command, integer):
        return (command >> 32) & _32bits, command & _32bits
    else:
        raise TypeError('`split_command(command)` expects either a string or an integer')


def join_command(namespace, code):
    """
    Joins namespace and code in a single command. Either two strings or two integer numbers
      are expected. Bits above the 31th in each number are ignored (the first is 0th).
    """
    if isinstance(namespace, text) and isinstance(code, text):
        return "%s.%s" % (namespace, code)
    elif isinstance(namespace, integer) and isinstance(code, integer):
        return ((namespace & _32bits) << 32) | code & _32bits
    else:
        raise TypeError('`join_command(namespace, code)` expects either two strings or two integer numbers')


class MsgPackFeature(Feature):

    @classmethod
    def _import_it(cls):
        """
        Imports msgpack library.
        """
        import msgpack
        return msgpack

    @classmethod
    def _import_error_message(cls):
        """
        Message error for msgpack not found.
        """
        return "You need to install msgpack for this to work (pip install msgpack-python>=0.4.6)"


def get_serializer(serializer_type):
    """
    Gets an appropiate serializer based on the specified type.
    """
    if serializer_type == 'json':
        return json
    elif serializer_type == 'msgpack':
        return MsgPackFeature.import_it()
    else:
        raise ValueError('Unexpected serializer type: %s' % serializer_type)