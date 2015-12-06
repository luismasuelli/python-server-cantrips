import six
import logging
from cantrips.protocol.messaging.layers import ProtocolLayer
from cantrips.protocol.messaging.formats import Translator, Formats, ANY_COMMAND

logger = logging.getLogger("cantrips.protocol.message.processor")


class MessageProcessorMetaClass(type):
    """
    Initializes the class object by instantiating the ProtocolLayer objects.
    """

    def __init__(cls, what, bases=None, dict=None):
        """
        Instantiates translators and ProtocolLayer classes.
        """

        def create_protocol_layer(layer_class, processor_class):
            if not issubclass(layer_class, ProtocolLayer):
                raise TypeError("Elements of LAYERS attribute must be classes -not instances- being derived")
            return layer_class(processor_class)

        super(MessageProcessorMetaClass, cls).__init__(what, bases=bases, dict=dict)
        # Translator - recognizing/instantiating
        if not hasattr(cls, 'TRANSLATOR'):
            raise TypeError("TRANSLATOR member must be defined as a subclass of"
                            " cantrips.protocol.messaging.formats.Translator, or an instance of"
                            " such acceptable classes")
        if not isinstance(cls.TRANSLATOR, Translator):
            if not isinstance(cls.TRANSLATOR, type) or not issubclass(cls.TRANSLATOR, Translator):
                raise TypeError("TRANSLATOR must be defined as either a subclass of"
                                " cantrips.protocol.messaging.formats.Translator, or an instance of"
                                " such acceptable classes")
            elif cls.TRANSLATOR == Translator:
                raise TypeError("Cannot specify cantrips.protocol.messaging.formats.Translator as translator class."
                                " A descendant of such class is expected")
            else:
                cls.TRANSLATOR = cls.TRANSLATOR()
        # Initializing layers...
        layers = getattr(cls, 'LAYERS', [])
        if not isinstance(layers, (list, tuple)):
            raise AttributeError('Message processors must define an attribute named LAYERS being list or tuple'
                                 ' with at least one element being a class derived from'
                                 ' cantrips.protocol.messaging.layers.ProtocolLayer')
        cls.LAYERS = tuple(create_protocol_layer(layer_class, cls) for layer_class in layers)

    def feed_translator(cls, namespace, code=ANY_COMMAND):
        """
        Adds entries to the underlying translator both for namespace and for
          code (in the case those are never fed with ANY_COMMAND).
        """
        if namespace is not ANY_COMMAND:
            ns_ = cls.TRANSLATOR.namespace(namespace)
            if code is not ANY_COMMAND:
                ns_.add_command(code)


class MessageProcessor(six.with_metaclass(MessageProcessorMetaClass)):
    """
    Processes the messages as they come. The lifecycle is tightly coupled to the underlying layers.
    """

    class CloseConnection(Exception):
        """
        This exception is intended as a signal used to close the connection inside a
          handler. It is not an exception to be logged or treated in a normal way
          like critical ones.
        """
        pass

    # ##################### Initialization ################################### #

    def __init__(self, strict=False):
        """
        Initializes whether it is strict or not.
        """
        self.strict = strict

    # ##################### Implementation-dependent ######################### #

    def _conn_close(self, code, reason=''):
        raise NotImplementedError

    def _conn_send(self, data, binary=None):
        raise NotImplementedError

    def _create_timeout(self, seconds, callback):
        raise NotImplementedError

    # ###################### Translation-related ############################# #

    def _trans_serialize(self, message):
        """
        Given a message object, it serializes it using the by-class translator.
        :param message: A message (Message instance) to serialize.
        :returns: (json|msgpack)-encoded raw data.
        """
        return self.TRANSLATOR.serialize(message)

    def _trans_parse(self, data, binary=None):
        """
        Given a raw message data, it unserializes it using the by-class translator.
        :param data: (json|msgpack)-encoded raw data.
        :param binary: Whether the received data has arrived as binary or frame.
          If the value is None, type is not strictly supported on a per-frame basis.
          Usually this will be non-None for Websockets only.
        :returns: A parsed and built message (Message instance).
        """
        return self.TRANSLATOR.parse_data(data, binary)

    # ################# Related to unexpected conditions ################# #

    def _forceful_close(self, code, reason):
        """
        Forcefully closes a connection.

        :param code: A standard integer code of the reason (specially for websockets).
        :param reason: A string description of the reason.
        """

        self._on_forceful_close(code, reason)
        self._conn_close(code, reason)

    def _close_invalid_format(self, data, binary=None):
        logger.debug("Message format error for: " + repr(data))
        # 1003 sera usado para el formato de los datos
        self._forceful_close(1003, "Message format error")

    def _close_protocol_violation(self, message):
        logger.debug("Unexistent or unavailable message: " + repr(message))
        # 1002 sera para mensaje no disponible o violacion de protocolo
        self._forceful_close(1002, "Unexistent or unavailable message")

    def _close_unknown(self, error):
        logger.debug("Cannot fullfill request: Exception triggered: %s - %s" % (type(error).__name__, str(error)))
        # 1011 sera para notificar que la peticion no pudo realizarse
        self._forceful_close(1011, "Cannot fullfill request: Internal server error")

    # ###################### Fully-Implemented ########################### #

    def send_message(self, message):
        """
        Takes a message and serializes it, according to the in-use translator.
        :param message: Message being sent.
        :returns: Whatever the implementation of _conn_send returns.
        """

        return self._conn_send(self._trans_serialize(message), self.TRANSLATOR.format == Formats.FORMAT_INTEGER)

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

    def start_timeout(self, seconds, callback):
        timeout = self._create_timeout(seconds, callback)
        timeout.start()
        return timeout

    def stop_timeout(self, timeout):
        timeout.force_stop()

    # ############################ Internal Events/Hooks ########################### #

    # Events from the client

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

    def _conn_message(self, data, binary=None):
        """
        Processes a client message. It will parse it and dispatch it to the layers.
        Message dispatching could gracefully close a connection if it is told to do so in a handler.
        If an exception occurs when serializing a message, or another unexpected exception occurs
          when processing a message, such scenarios can also be handled.

        :param data: Data being parsed.
        :param binary: Tells whether the incoming data is binary, text, or unspecified.
          Typically it is only specified for websockets.
        """

        try:
            message = self._trans_parse(data, binary)
            for layer in self.LAYERS:
                try:
                    # If no error occurs, processing this layer is enough.
                    # Other layers will be processed if one of those expected
                    #   errors occur.
                    layer.process_message(self, message)
                    return
                except ProtocolLayer.ICannotHandle:
                    # Iteration continues to the next layer.
                    pass
                except ProtocolLayer.NobodyCanHandle:
                    # Iteration gets out and It's assured that it will
                    # not be handled.
                    break
            # Since no layer could process it, we handle it as unknown message.
            self._unknown_message(message)
        except self.CloseConnection:
            self.terminate()
        except self._serializer_exceptions() as error:
            self._serializer_exception(error, data, binary)
        except Exception as error:
            self._unknown_exception(error, '_conn_message')

    # Something has happened!

    def _unknown_exception(self, error, context):
        """
        Processes an unkown exception.

        If this handler is set as strict, the connection is *forcefully* closed.
        Otherwise it is handled by ._on_unknown_exception.
        """

        if self.strict:
            self._close_unknown(error)
        else:
            self._on_unknown_exception(error, context)

    def _serializer_exception(self, error, data, binary=None):
        """
        Processes the case when an error occurred serializing a client message.
        It will either *forcefully* close the connection (by invalid format), or attend the case when an
          invalid message should be processed. This depends on the value of the `strict` attribute.

        :param error: An exception that has occurred.
        :param data: Tried data.
        :param binary: If the data was intended as binary. Intended only for websockets.
        """

        if self.strict:
            self._close_invalid_format(data, binary)
        else:
            self._on_serializer_exception(error)

    def _unknown_message(self, message):
        """
        Processes an unknown parsed message.

        If this handler is set as strict, the connection is *forcefully* closed.
        Otherwise it is handled by ._on_unknown_message.
        """

        if self.strict:
            self._close_protocol_violation(message)
        else:
            self._on_unknown_message(message)

    # ############################ Events ################################ #

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

    def _on_unknown_message(self, message):
        """
        Processes an exception by running certain behavior. It is
          the same as processing a normal message: If this function
          raises self.CloseConnection, the connection will be closed.

        :params message:
        """

    def _on_unknown_exception(self, error, context):
        """
        Processes an unknown exception, not related to the client message parsing.
        If this function raises self.CloseConnection the connection will be closed.

        :param error: Exception being attended.
        :param context: A description of the moment where this error has been triggered.
        """

    def _on_serializer_exception(self, error):
        """
        Processes an unknown exception, not related to the client message parsing.
        If this function raises self.CloseConnection the connection will be closed.

        :param error: Exception being attended.
        """

    def _on_forceful_close(self, code, reason):
        """
        Pre-process a forceful close. No exception should be triggered here.
        It is recommended that totally safe code is implemented here.

        :param code: A standard integer code of the reason (specially for websockets).
        :param reason: A string description of the reason.
        """