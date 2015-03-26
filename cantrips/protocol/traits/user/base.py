from cantrips.protocol.traits.permcheck import PermCheck
from cantrips.patterns.broadcast import IBroadcast
from cantrips.patterns.identify import Identified, List


class UserEndpoint(Identified):
    """
    Base behavior for users. It bounds to a cantrips.protocol.messaging.MessageProcessor
      instance (socket), and "notifies" through it.
    """

    def __init__(self, key, socket, *args, **kwargs):
        super(UserEndpoint, self).__init__(key, socket=socket, *args, **kwargs)

    def notify(self, ns, code, *args, **kwargs):
        return self.socket.send_message(ns, code, *args, **kwargs)

    def slaves(self):
        """
        List of slaves the user is connected to.
        """
        raise NotImplementedError


class UserEndpointList(List):
    """
    Endpoint list. It can specify the class to be used as endpoint.
    """

    @classmethod
    def endpoint_class(cls):
        """
        Class to be used as endpoint class.
        """
        return UserEndpoint

    def __init__(self):
        super(UserEndpointList, self).__init__(self.endpoint_class())


class UserBroadcast(Identified, IBroadcast, PermCheck):
    """
    Broadcast implementation for such endpoint list. Implements registration
      by using an endpoint list (the endpoint list may be custom-instantiated
      by descending classes).

    If instantiated with master=True (default), it creates users (i.e. accepts key and
      additional arguments), otherwise it can only add users (additional arguments are
      inserted, and the first argument must be an already-created instance) which are
      already-created instances.
    """

    @classmethod
    def endpoint_list(cls):
        """
        Class to be used as endpoint list class.
        """
        return UserEndpointList()

    def __init__(self, key, *args, **kwargs):
        super(UserBroadcast, self).__init__(key, list=self.endpoint_list(), *args, **kwargs)

    def users(self):
        """
        Users list.
        """
        return self.list

    def register(self, user, *args, **kwargs):
        """
        Inserts a user instance (arguments are ignored) on non-master lists.
        Creates a user (arguments are considered) on master lists.
        """
        raise NotImplementedError

    def unregister(self, user, *args, **kwargs):
        """
        Removes (unregisters) a user (it may be either key or instance).
          More args may be supplied for overriding implementations.
        """
        return self.list.remove(user)

    def notify(self, user, command, *args, **kwargs):
        """
        Sends a notification to a user.
          The command must be a tuple (ns, code).
          The user must be a key or the corresponding instance.
          More args may be supplied for the commands or overriding implementations.
        """
        ns, code = command
        return self.list[user].notify(ns, code, *args, **kwargs)