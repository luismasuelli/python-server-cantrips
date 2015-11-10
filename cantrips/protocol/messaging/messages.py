from cantrips.types.exception import factory
from cantrips.types.arguments import Arguments
from cantrips.iteration import items
from cantrips.protocol.messaging.formats import split_command, join_command, integer, string



class Packet(Arguments):
    """
    A packet fetches args, kwargs, and a command code.
    The command code is stored under the property `code`.
    """

    def __init__(self, code, *args, **kwargs):
        super(Packet, self).__init__(*args, **kwargs)
        self.__code = code

    @property
    def code(self):
        return self.__code

    def __setattr__(self, key, value):
        if key == '_Packet__code':
            return object.__setattr__(self, key, value)
        return super(Packet, self).__setattr__(key, value)

    def __repr__(self):
        """
        Code representation.
        """
        return "%s(%r,*%r,**%r)" % (type(self).__name__, self.code, self.args, self.kwargs)


class Message(Packet):
    """
    A message is a packet with a namespace and a command.
    Both values, when compound, build the `code` property.
    """

    Error = factory({
        'CANNOT_REPRESENT_NONCLIENT_MESSAGE': 1,
        'CANNOT_RECONSTRUCT_NONCLIENT_MESSAGE': 2,
        'FACTORY_ALREADY_EXISTS': 3,
        'FACTORY_DOES_NOT_EXIST': 4,
        'NAMESPACE_ALREADY_EXISTS': 5,
        'NAMESPACE_DOES_NOT_EXIST': 6,
        'INVALID_FORMAT': 7
    })

    def __init__(self, namespace, command, direction, *args, **kwargs):
        super(Message, self).__init__(join_command(namespace, command), *args, **kwargs)
        self.__direction = direction

    @property
    def direction(self):
        return self.__direction

    def __setattr__(self, key, value):
        if key == '_Message__direction':
            return object.__setattr__(self, key, value)
        return super(Message, self).__setattr__(key, value)

    def to_representation(self, expect_clientwise=False):
        parts = {
            "code": self.code
        }

        if self.args:
            parts['args'] = self.args

        if self.kwargs:
            parts['kwargs'] = self.kwargs

        if expect_clientwise and not (self.direction & MessageFactory.DIRECTION_CLIENT):
            raise Message.Error("Message cannot be represented since it's not client-wise",
                                Message.Error.CANNOT_REPRESENT_NONCLIENT_MESSAGE,
                                parts=parts)
        else:
            return parts


class MessageFactory(object):
    """
    A message factory builds messages from a code and namespace.
    """

    DIRECTION_CLIENT = 1
    DIRECTION_SERVER = 2
    DIRECTION_BOTH = 3

    def __init__(self, namespace, code, direction):
        self.__namespace = namespace
        self.__code = code
        self.__direction = direction

    def build(self, *args, **kwargs):
        return Message(self.namespace.code, self.code, self.direction, *args, **kwargs)

    @property
    def code(self):
        return self.__code

    @property
    def namespace(self):
        return self.__namespace

    @property
    def direction(self):
        return self.__direction


class MessageNamespace(object):
    """
    A message namespace creates/registers commands.
    """

    def __init__(self, code):
        self.__code = code
        self.__messages = {}

    @property
    def code(self):
        return self.__code

    def register(self, code, direction, silent=False):
        try:
            x = self.__messages[code]
            if silent:
                return x
            else:
                raise Message.Error("Factory with that code already exists",
                                    Message.Error.FACTORY_ALREADY_EXISTS,
                                    factory_code=code)
        except KeyError:
            x = MessageFactory(self, code, direction)
            self.__messages[code] = x
            return x

    def find(self, code):
        try:
            return self.__messages[code]
        except KeyError:
            raise Message.Error("Message not registered",
                                Message.Error.FACTORY_DOES_NOT_EXIST,
                                factory_code=code)


class MessageNamespaceSet(object):
    """
    A message namespace set creates/registers message namespaces.
    """

    def __init__(self, namespaces):
        self.__namespaces = {}
        x = self.register("messaging")
        x.register("error", MessageFactory.DIRECTION_CLIENT)

        opts = {
            "server": MessageFactory.DIRECTION_SERVER,
            "client": MessageFactory.DIRECTION_CLIENT,
            "both": MessageFactory.DIRECTION_BOTH
        }
        for k, v in items(namespaces):
            x = self.register(k, True)
            for k2, d in items(v):
                x.register(k2, opts[d.lower()], True)

    def register(self, code, silent=False):
        try:
            x = self.__namespaces[code]
            if silent:
                return x
            else:
                raise Message.Error("Message namespace already registered",
                                    Message.Error.NAMESPACE_ALREADY_EXISTS,
                                    namespace_code=code)
        except KeyError:
            x = MessageNamespace(code)
            self.__namespaces[code] = x
            return x

    def find(self, code):
        try:
            return self.__namespaces[code]
        except KeyError:
            raise Message.Error("Message namespace not registered",
                                Message.Error.NAMESPACE_DOES_NOT_EXIST,
                                namespace_code=code)

    def from_representation(self, obj, expect_serverwise=False):
        if not isinstance(obj, dict) or isinstance(obj.get('code'), integer + (string,)) or not isinstance(obj.get('args', []), (tuple, list)) or not isinstance(obj.get('kwargs', {}), dict):
            raise Message.Error("Expected format message is {code:string, args:list, kwargs:dict}",
                                Message.Error.INVALID_FORMAT, parts=obj)
        else:
            code_parts = split_command(obj['code'])
            if len(code_parts) != 2:
                raise Message.Error("Message code must be in format `namespace.code` or a 64bit integer with 32 MSBs "
                                    "being the namespace and the 32 LSBs being the code. Current: " + obj['code'],
                                    Message.Error.INVALID_FORMAT, parts=obj)
            else:
                factory = self.find(code_parts[0]).find(code_parts[1])
                if expect_serverwise and not (factory.direction & MessageFactory.DIRECTION_SERVER):
                    raise Message.Error("Message cannot be unserialized since it's not server-wise",
                                        Message.Error.CANNOT_RECONSTRUCT_NONCLIENT_MESSAGE, parts=obj)
                return factory.build(*obj.get('args', []), **obj.get('kwargs', {}))

