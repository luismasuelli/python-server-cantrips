from cantrips.types.arguments import Arguments


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

    def __init__(self, namespace, command, *args, **kwargs):
        super(Message, self).__init__((namespace, command), *args, **kwargs)