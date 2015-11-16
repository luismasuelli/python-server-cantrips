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

        super(MessageProcessorMetaClass, cls).__init__(what, bases=None, dict=None)
        # Popping format and initializing translator...
        cls.__TRANSLATOR = Translator(getattr(cls, 'FORMAT', Formats.FORMAT_STRING))
        delattr(cls, 'FORMAT')
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
            ns_ = cls.__TRANSLATOR.namespace(namespace)
            if code is not ANY_COMMAND:
                ns_.add_command(code)
