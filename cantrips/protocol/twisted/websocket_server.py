try:
    from autobahn.twisted.websocket import WebSocketServerProtocol
except:
    raise ImportError("You need to install twisted (pip install twisted==14.0.2) AND Autobahn for Python "
                      "(pip install autobahn) for this to work. As an alternative, you can install both Autobahn "
                      "and Twisted by executing: pip install autobahn[twisted]")
from cantrips.protocol.messaging import MessageProcessor
from future.utils import istext


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

    def _decode(self, payload, isBinary):
        try:
            return payload if isBinary else payload.decode('utf8')
        except UnicodeDecodeError:
            self._close_invalid_format(payload)

    def _conn_close(self, code, reason=''):
        return self.failConnection(code, reason)

    def _conn_send(self, data):
        return self.transport.write(data, not istext(data))

    def onOpen(self):
        self._conn_made()

    def onMessage(self, payload, isBinary):
        self._conn_message(self._decode(payload, isBinary))