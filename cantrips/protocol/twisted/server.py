try:
    from twisted.internet.protocol import Factory, Protocol, connectionDone
except:
    raise ImportError("You need to install twisted for this to work (pip install twisted==14.0.2)")
import json
from cantrips.protocol.messaging import MessageProcessor


class MessageProtocol(Protocol, MessageProcessor):
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
        self.transport.write(json.dumps({'code': code, 'reason': reason}))
        return self.transport.loseConnection()

    def _conn_send(self, data):
        return self.transport.write(data)

    def connectionMade(self):
        self._conn_made()

    def dataReceived(self, data):
        self._conn_message(data)