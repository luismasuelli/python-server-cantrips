from .formats import ANY_COMMAND


class ProtocolLayer(object):
    """
    A protocol layer is a processing layer which can process a received command or forward it to the next layer.
    The next layer could also forward it to the next layer or process it. Additionally, a layer can definitely
      reject a given message (the message would be processed as if no layer was able to handle it).

    Subclasses should redefine __init__(self, translator) like this:

        super(MySubclass, self).__init__(translator)
        self.add_namespace_handler(CommandSpec(...), self._doSomethingForCommandsInANamespace)
        self.add_command_handler(CommandSpec(...), ANY_COMMAND, self._doSomethingForCommandsInANamespace
        ... # We specify behavior to every command in a specific namespace.
        ... # HOWEVER the commands cannot be translated because they are not known beforehand:
        ... #   They must be added later.
        self.add_command_handler(CommandSpec(...), CommandSpec(...), self._doSomethingForACommand)
        ... # We specify behavior to a specific command in a specific namespace
        self.add_namespace_handler(ANY_COMMAND, self._doSomethingForEveryUnmatchedCommand)
        self.add_command_handler(ANY_COMMAND, ANY_COMMAND, self._doSomethingForEveryUnmatchedCommand)
        ... # In these examples, no namespace and/or command is specified.
        ... # HOWEVER the commands and namespaces cannot be translated because they are not known beforehand:
        ... #   They must be added later.
        ...
        self.translator.namespace(CommandSpec(...)).add_command(CommandSpec(...))
        ... # Explicitly add the namespaces and commands to the translators.
        ...
        ... # Handlers must be h(self, socket, message), where self is a layer object and the
        ... #   callable is meant to be an instance method in the same layer.
        ... # - They can call layer.i_cant_handle() to forward the message to the next layer.
        ... # - They can call layer.nobody_can_handle() to forward the message to the next layer.

    Methods:
    - __init__(self, translator): Override it to add custom handlers.
    - add_namespace_handler(namespace or ANY_COMMAND, handler_method): Call it inside __init__ to create
      a namespace handler.
    - add_command_handler(namespace or ANY_COMMAND, code or ANY_COMMAND, handler_method): Call it inside __init__ to
      create a command handler.
    - _<handler>(self, socket, message): Any handler to be added.
    - i_cannot_handle(): Use it inside a handler to make the next layer process the message.
    - nobody_can_handle(): Use it inside a handler to make the message unprocessable by any handler.
    - process_message(socket, message): You will never need to call this method. It is part of the core.
    """

    class Exception(Exception):
        """
        A standard ProtocolLayer exception.
        """
        pass

    class ICannotHandle(Exception):
        """
        Tells that the message must be forwarded to the next layer.
        """
        pass

    class NobodyCanHandle(Exception):
        """
        Tells that the message will not forwarded.
        """
        pass

    # ############################################################ #

    def __init__(self, translator):
        """
        Subclasses will initialize the protocol to specify handlers when overriding this method.
        A translator will be used to "feed" translations (e.g. when creating a command handler).
        """
        self.__handlers = {}
        self.__translator = translator

    @property
    def translator(self):
        return self.__translator

    def i_cannot_handle(self):
        """
        Raises an ICannotHandle exception.
        """
        raise self.ICannotHandle

    def nobody_can_handle(self):
        """
        Raises a NobodyCanHandle exception.
        """
        raise self.NobodyCanHandle

    def add_namespace_handler(self, namespace, handler):
        """
        Handles all the commands in a namespace. By passing namespace=ANY_COMMAND this will handle any
          command in any namespace, but will have the lowest precedence (i.e. will only work for unmatched
          messages).
        :param namespace: A `CommandSpec` instance.
        :param handler: A callable accepting (socket, message).
        :returns: @see handle_command
        """
        return self.add_command_handler(namespace, ANY_COMMAND, handler)

    def add_command_handler(self, namespace, command, handler):
        """
        Handles a specific command (by namespace and by command code). By passing command=ANY_COMMAND this
          will handle any command in the namespace. By passing also namespace=ANY_COMMAND this will handle
          any command in any namespace. It is an error (TypeError) to specify namespace=ANY_COMMAND
          without specifying command=ANY_COMMAND, or specify in either case a non-callable value for `handler`.
        :param namespace: A `CommandSpec` instance.
        :param command: A `CommandSpec` instance.
        :param handler: A callable accepting (socket, message).
        :returns: Nothing
        """
        if not callable(handler):
            raise TypeError("The specified value for a handler must be a callable accepting (socket, layer, message)")

        if namespace is ANY_COMMAND and command is not ANY_COMMAND:
            raise ValueError("The specified value for command must be ANY_COMMAND if the namespace is also ANY_COMMAND")

        if namespace is not ANY_COMMAND:
            ns_ = self.translator.namespace(namespace)
            if command is not ANY_COMMAND:
                ns_.add_command(command)

        self.__handlers[(namespace, command)] = handler

    def process_message(self, socket, message):
        """
        Processes a fully parsed message. Tries to match it by its namespace and code against the registered
          handlers here. Fully parsed messages have always a defined and well-translated namespace and code.
        :param socket: Processor which received the message.
        :param message: Message received.
        :returns: Nothing
        """

        def _get(ns, c):
            return self.__handlers.get((ns, c))

        handler = (_get(message.code[0], message.code[1]) or
                   _get(message.code[0], ANY_COMMAND) or
                   _get(ANY_COMMAND, ANY_COMMAND) or
                   (lambda socket, message: self.i_cannot_handle()))

        handler(socket, message)