import logging
from cantrips.types.exception import factory
from cantrips.types.arguments import Arguments
from cantrips.iteration import items
from cantrips.protocol.messaging.formats import split_command, join_command, integer, string, get_serializer, \
    get_serializer_exceptions

logger = logging.getLogger("cantrips.message.processor")


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
                raise Message.Error("Message code must be in format `namespace.code`. Current: " + obj['code'],
                                    Message.Error.INVALID_FORMAT, parts=obj)
            else:
                factory = self.find(code_parts[0]).find(code_parts[1])
                if expect_serverwise and not (factory.direction & MessageFactory.DIRECTION_SERVER):
                    raise Message.Error("Message cannot be unserialized since it's not server-wise",
                                        Message.Error.CANNOT_RECONSTRUCT_NONCLIENT_MESSAGE, parts=obj)
                return factory.build(*obj.get('args', []), **obj.get('kwargs', {}))


class MessageProcessor(object):

    def __init__(self, strict=False, serializer='json'):
        """
        Initializes the protocol, stating whether, upon
          the invalid messages can be processed by the
          user or must be processed automatically.

        Additionally, it lets the user specify whether
          the connection should use msgpack to encode
          the messages or json. Such are the values of
          `serializer` ('msgpack' => msgpack, 'json' => json).
        """

        self.serializer = serializer
        self.strict = strict
        self._setup_ns()

    @classmethod
    def _setup_ns(cls):
        if not hasattr(cls, '_ns_set'):
            cls._ns_set = MessageNamespaceSet(cls.setup())

    @classmethod
    def setup(cls):
        """
        Specifies the protocol messages to be delivered and
          received. This function must return a dictionary
          with key strings:
            { "name.space" : { "code": (direction), ... }, ... }
        
        Namespaces and codes may be strings or 32-bit integers.

        Where direction may be:
          MessageFactory.DIRECTION_SERVER : this message can go to the server
          MessageFactory.DIRECTION_CLIENT : this message can go to the client
          MessageFactory.DIRECTION_BOTH : this message can go in both directions
        """

        return {}

    def _conn_close(self, code, reason=''):
        raise NotImplementedError

    def _conn_send(self, data):
        raise NotImplementedError

    def terminate(self):
        """
        Terminates the connection by starting the goodbye handshake. A connection could
          be terminated as part of the normal client message processing, or by an external
          call (e.g. a broadcast or administrative decision).
        """

        try:
            self.goodbye()
        except Message.Error as e:
            self._close_protocol_violation(e.parts)
        except Exception as e:
            self._close_unknown(e)
        self._conn_close(1000)

    def send_message(self, ns, code, *args, **kwargs):
        """
        Sends a packet with a namespace, a message code, and arbitrary
          arguments. Messages must be checked for their direction whether
          they can be sent to the client.
        """
        representation = self._ns_set.find(ns).find(code).build_message(*args, **kwargs).to_representation(True)
        data = get_serializer(self.serializer).dumps(representation)
        self._conn_send(data)

    def _handlers(self):
        """
        A dictionary with the following structure:
          "namespace.code" => handler.

        Each handler expects exactly one parameter: the message
          to be processed.
        """

        return {}

    def _dispatch_message(self, message):
        """
        Processes a message by running a specific behavior. If
          this function returns a True-evaluable value, the
          connection is closed.
        """
        ns, code = split_command(message.code)
        h = self._handlers().get(ns, {}).get(code, lambda socket, message: None)
        return h(self, message)

    def invalid_message(self, error):
        """
        Processes an exception by running certain behavior. It is
          the same as processing a normal message: If this function
          returns a True-evaluable value, the connection is closed.
        """

        return True

    def hello(self):
        """
        Processes an on-connection behavior. It is completely safe to
          send messages to the other endpoint.
        """

        pass

    def goodbye(self):
        """
        Processes an on-disconnection behavior. It is completely safe to
          send messages to the other endpoint, since the closing reason is
          not the client already closed the connection, but a protocol error
          or an agreed connection-close command.
        """

        pass

    def _close_invalid_format(self, parts):
        logger.debug("Message format error for: " + repr(parts))
        #Cuando se apruebe el draft, 1003 sera usado para el formato de los datos.
        self._conn_close(3003, "Message format error")

    def _close_protocol_violation(self, parts):
        logger.debug("Unexistent or unavailable message: " + repr(parts))
        #Cuando se apruebe el draft, 1002 sera para mensaje no disponible o violacion de protocolo
        self._conn_close(3002, "Unexistent or unavailable message")

    def _close_unknown(self, exception):
        logger.debug("Cannot fullfill request: Exception triggered: %s - %s" % (type(exception).__name__, str(exception)))
        #Cuando se apruebe el draft, 1011 sera para notificar que la peticion no pudo realizarse
        self._conn_close(3011, "Cannot fullfill request: Internal server error")

    def _close_if(self, result):
        if result:
            self.terminate()

    def _conn_made(self):
        try:
            self.hello()
        except Message.Error as e:
            self._close_protocol_violation(e.parts)
        except Exception as e:
            self._close_unknown(e)

    def _conn_message(self, data):
        exceptions = get_serializer_exceptions(self.serializer) + (Message.Error,)
        try:
            self._close_if(self._dispatch_message(self._ns_set.from_representation(get_serializer(self.serializer).loads(data), True)))
        except exceptions as error:
            if self.strict:
                if isinstance(error, Message.Error):
                    if getattr(error, 'code', False) == "messaging:message:invalid":
                        self._close_invalid_format(error.parts)
                    else:
                        self._close_protocol_violation(error.parts)
                else:
                    self._close_invalid_format(error.value)
            else:
                self._close_if(self.invalid_message(error))
        except Exception as error:
            if self.strict:
                self._close_unknown(error)
            else:
                self._close_if(self.invalid_message(error))