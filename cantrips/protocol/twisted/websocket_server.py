try:
    from autobahn.twisted.websocket import WebSocketServerProtocol
except:
    raise ImportError("You need to install twisted (pip install twisted==14.0.2) AND Autobahn for Python "
                      "(pip install autobahn) for this to work. As an alternative, you can install both Autobahn "
                      "and Twisted by executing: pip install autobahn[twisted]")
from cantrips.protocol.messaging.processor import MessageProcessor


class MessageProtocol(WebSocketServerProtocol, MessageProcessor):
    """
    This handler formats the messages using json. Messages
      must match a certain specification defined in the
      derivated classes.
    """

    def __init__(self, strict=False):
        """
        Initializes the protocol, stating whether, upon
          the invalid messages can be processed by the
          user or must be processed automatically.
        """

        MessageProcessor.__init__(self, strict=strict)

    def _conn_close(self, code, reason=''):
        return self.failConnection(code, reason)

    def _conn_send(self, data, binary=None):
        if binary is None:
            raise TypeError("For web-socket implementations, binary argument must be set to send data")
        return self.transport.write(data, binary)

    def onOpen(self):
        self._conn_made()

    def onMessage(self, payload, isBinary):
        # Actually the payload will be unicode or str, and will match isBinary in that sense.
        # Unlike Tornado, here we have a redundant isBinary variable but under the hoods the
        #   same processing happened to identify unicode and str for text and binary, respectively.
        self._conn_message(payload, isBinary)