try:
    from tornado.websocket import WebSocketHandler
except:
    raise ImportError("You need to install tornado for this to work (pip install tornado==4.0.2)")
from cantrips.protocol.messaging import MessageProcessor


class MessageHandler(WebSocketHandler, MessageProcessor):
    """
    This handler formats the messages using json. Messages
      must match a certain specification defined in the
      derivated classes.
    """

    def initialize(self, strict=False):
        """
        Initializes the handler by specifying whether the
          error processing will be automatic (strict=True)
          or the user will be able to handle the error
          processing.
        """

        MessageProcessor.__init__(self, strict=strict)

    def _conn_send(self, data):
        """
        Both JSON and MSGPACK are, actually, binary connections.
        """
        return self.write_message(data, True)

    def _conn_close(self, code, reason=''):
        return self.close(code, reason)

    def open(self):
        self._conn_made()

    def on_message(self, message):
        self._conn_message(message)