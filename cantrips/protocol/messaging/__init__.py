import logging
from .formats import split_command, join_command, integer, string, get_serializer, \
    get_serializer_exceptions
from .messages import MessageNamespaceSet, Message

logger = logging.getLogger("cantrips.message.processor")


class MessageProcessor(object):

    class CloseConnection(Exception):
        """
        This exception is intended as a signal used to close
          the connection inside a handler. It is not an exception
          to be logged or treated in a normal way like critical
          ones.
        """
        pass

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
            cls._ns_set = MessageNamespaceSet(cls._protocol_config())

    @classmethod
    def _protocol_config(cls):
        """
        Specifies the protocol messages to be delivered and
          received. This function must return a dictionary
          with key strings:
            { "name.space" : { "code": (direction), ... }, ... }
        
        Namespaces and codes may be strings or 32-bit integers.

        Where direction may be:
          "server" : this message can go to the server
          "client" : this message can go to the client
          "both" : this message can go in both directions
        """

        return {}

    def terminate(self):
        """
        Terminates the connection by starting the goodbye handshake. A connection could
          be terminated as part of the normal client message processing, or by an external
          call (e.g. a broadcast or administrative decision).
        """

        try:
            self._on_goodbye()
            self._conn_close(1000)
        except Exception as e:
            self._unknown_exception(e, 'terminate')

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

    def _on_invalid_message(self, error):
        """
        Processes an exception by running certain behavior. It is
          the same as processing a normal message: If this function
          raises self.CloseConnection, the connection will be closed.
        """

    def _on_unknown_exception(self, error, context):
        """
        Processes an unknown exception, not necessarily related to the
          client message parting. If this function raises self.CloseConnection
          the connection will be closed.

        :param error: Exception being attended.
        :param context: A description of the moment where this error has been triggered.
        """

    def _on_hello(self):
        """
        Processes an on-connection behavior. It is completely safe to
          send messages to the other endpoint. If self.CloseConnection
          is triggered here, the connection will be gracefully closed.
        """

    def _on_goodbye(self):
        """
        Processes an on-disconnection behavior. It is completely safe to
          send messages to the other endpoint, since the closing reason is
          not the client already closed the connection, but a protocol error
          or an agreed connection-close command. No exception should be
          triggered here.

        It is recommended that totally safe code is implemented here.
        """

    def _on_forceful_close(self, code, reason):
        """
        Pre-process a forceful close. No exception should be triggered here.
        It is recommended that totally safe code is implemented here.

        :param code: A standard integer code of the reason (specially for websockets).
        :param reason: A string description of the reason.
        """

    #########################################################################
    ######################## Implementation details here ####################
    # Stuff here is not intended to be overridden by the users / inheritors #
    #########################################################################
    #########################################################################

    # Connection template functions

    def _conn_close(self, code, reason=''):
        raise NotImplementedError

    def _conn_send(self, data):
        raise NotImplementedError

    # Strict / forceful close functions.

    def _forceful_close(self, code, reason):
        """
        Forcefully closes a connection.

        :param code: A standard integer code of the reason (specially for websockets).
        :param reason: A string description of the reason.
        """

        self._on_forceful_close(code, reason)
        self._conn_close(code, reason)

    def _close_invalid_format(self, parts):
        logger.debug("Message format error for: " + repr(parts))
        # Cuando se apruebe el draft, 1003 sera usado para el formato de los datos.
        self._forceful_close(3003, "Message format error")

    def _close_protocol_violation(self, parts):
        logger.debug("Unexistent or unavailable message: " + repr(parts))
        # Cuando se apruebe el draft, 1002 sera para mensaje no disponible o violacion de protocolo
        self._forceful_close(3002, "Unexistent or unavailable message")

    def _close_unknown(self, error):
        logger.debug("Cannot fullfill request: Exception triggered: %s - %s" % (type(error).__name__, str(error)))
        # Cuando se apruebe el draft, 1011 sera para notificar que la peticion no pudo realizarse
        self._forceful_close(3011, "Cannot fullfill request: Internal server error")

    def _unknown_exception(self, error, context):
        """
        Processes an unkown exception.

        If this handler is set as strict, the connection is *forcefully* handled.
        Otherwise it is handled by .unknown_exception().
        """

        if self.strict:
            self._close_unknown(error)
        else:
            self._on_unknown_exception(error, context)

    def _conn_made(self):
        """
        Processes the event when a connection is created (i.e. this connection is opened).
          It invokes .hello() (no args).

        If .hello() raises an error, it is told that to the other end. If the error
          triggered is self.CloseConnection, the connection will be gracefully terminated.
          A use case for this is an echo server.
        """

        try:
            self._on_hello()
        except self.CloseConnection:
            self.terminate()
        except Exception as e:
            self._unknown_exception(e, '_conn_made')

    def _serializer_exceptions(self):
        """
        Returns the exceptions to be captured on message parsing.

        :return: Tuple of Exception subclasses.
        """

        return get_serializer_exceptions(self.serializer) + (Message.Error,)

    def _parse_client_message(self, data):
        """
        Using the current namespaces set, parses a client message as received by the socket.
        The message must be well-formed, with no extra data at all (neither leading nor trailing).

        :param data: raw data to be parsed (as received by the socket).
        :returns: A Message instance.
        :raises: Message.Error if data is of wrong format (other errors like ValueError, TypeError, and msgpack
          errors can also be triggered).
        """

        return self._ns_set.from_representation(get_serializer(self.serializer).loads(data), True)

    def _process_serializer_exception(self, error):
        """
        Processes the case when an error occurred serializing a client message.
        It will either *forcefully* close the connection (by invalid format or protocol violation),
          or attend the case when an invalid message should be processed. This depends on the value
          of the `strict` attribute.

        :param error: An exception that has occurred.
        """

        if self.strict:
            if isinstance(error, Message.Error):
                if getattr(error, 'code', False) == "messaging:message:invalid":
                    self._close_invalid_format(error.parts)
                else:
                    self._close_protocol_violation(error.parts)
            else:
                self._close_invalid_format(error.value)
        else:
            self._on_invalid_message(error)

    def _dispatch_message(self, message):
        """
        Processes a message by running a specific behavior. If this function raises
          self.CloseConnection, the connection is gracefully closed.

        :param message: A parsed message being processed.
        """

        ns, code = split_command(message.code)
        h = self._handlers().get(ns, {}).get(code, lambda socket, message: None)
        h(self, message)

    def _conn_message(self, data):
        """
        Processes a client message. It will parse it and dispatch it.
        Message dispatching could gracefully close a connection if it is told to do so in a handler.
        If an exception occurs when serializing a message, or another unexpected exception occurs
          when processing a message, such scenarios can also be handled.

        :param data: Data being parsed.
        """

        try:
            self._dispatch_message(self._parse_client_message(data))
        except self.CloseConnection:
            self.terminate()
        except self._serializer_exceptions() as error:
            self._process_serializer_exception(error)
        except Exception as error:
            self._unknown_exception(error, '_conn_message')