from future.utils import PY3
from cantrips.features import Feature
from collections import namedtuple
from enum import Enum
import json


_32bits = (1 << 32) - 1


class MsgPackFeature(Feature):

    @classmethod
    def _import_it(cls):
        """
        Imports msgpack library.
        """
        import msgpack
        import msgpack.exceptions
        return msgpack, msgpack.exceptions.UnpackException

    @classmethod
    def _import_error_message(cls):
        """
        Message error for msgpack not found.
        """
        return "You need to install msgpack for this to work (pip install msgpack-python>=0.4.6)"


def _json_serializer():
    """
    Returns an object with dumps() and loads() for json format.
    """
    return json


def _msgpack_serializer():
    """
    Returns an object with dumps() and loads() for msgpack format.
    """
    return MsgPackFeature.import_it()[0]


def _json_serializer_exceptions():
    """
    Returns a tuple containing only the json-related exceptions.
    """
    return TypeError, ValueError


def _msgpack_serializer_exceptions():
    """
    Returns a tuple containing only the MsgPack exceptions.
    """
    return MsgPackFeature.import_it()[1],


def _split_string_command(command):
    """
    Splits as dotted string. The code is the last part of the string.
    E.g. a.b.c is splitted as a.b, c
    """
    try:
        a, b = command.rsplit('.', 1)
        return a, b
    except:
        raise ValueError("Command to parse MUST be a dotted string")


def _split_integer_command(command):
    """
    Splits a 64bit integer into high 32 bits and low 32 bits.
    """
    return (command >> 32) & _32bits, command & _32bits


def _join_string_command(ns, code):
    """
    Joins two strings as a dot-separated string. E.g. a.b, c will be joined as a.b.c.
    """
    return "%s.%s" % (ns, code)


def _join_integer_command(ns, code):
    """
    Joins two integers as a 64bit integer, being the ns the 32 MSB and the code the 32 LSB.
    """
    return ((ns & _32bits) << 32) | code & _32bits


_JOINERS = (_join_string_command, _join_integer_command)
_SPLITTERS = (_split_string_command, _split_integer_command)
_BROKERS = (_json_serializer, _msgpack_serializer)
_EXCEPTIONS = (_json_serializer_exceptions, _msgpack_serializer_exceptions)
_MEMBER_NAMES = ('string', 'integer')


class Formats(int, Enum):
    """
    Parsing formats for messages. Intended:
    - 0 -> string -> JSON.
    - 1 -> integer -> MsgPack.
    """
    FORMAT_STRING = 0
    FORMAT_INTEGER = 1

    @property
    def split(self):
        if not hasattr(self, '__split'):
            self.__split = _SPLITTERS[self.value]
        return self.__split

    @property
    def join(self):
        if not hasattr(self, '__join'):
            self.__join = _JOINERS[self.value]
        return self.__join

    @property
    def member_name(self):
        if not hasattr(self, '__member'):
            self.__member = _MEMBER_NAMES[self.value]
        return self.__member

    @property
    def broker(self):
        if not hasattr(self, '__broker'):
            self.__broker = _BROKERS[self.value]()
        return self.__broker

    @property
    def exceptions(self):
        if not hasattr(self, '__exceptions'):
            self.__exceptions = _EXCEPTIONS[self.value]()
        return self.__exceptions


class CommandSpec(namedtuple('_CommandSpec', _MEMBER_NAMES)):
    """
    This class will be used to instantiate each namespace and code (they, together, conform a command),
      which can be specified by integer or by string (regardless the output format, either msgpack or json).

    However, a message *must* have *str* keyword arguments, so as_keyword() will always yield the string
      component.
    """

    def as_keyword(self):
        return self.string
ANY_COMMAND = CommandSpec(0xFFFFFFFF, '__any__')