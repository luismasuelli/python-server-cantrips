from cantrips.patterns.broadcast import IBroadcast
from base import UserBroadcast
from cantrips.patterns.actions import AccessControlledAction
from cantrips.protocol.traits.decorators.authcheck import IAuthCheck
from cantrips.protocol.traits.decorators.incheck import IInCheck
from cantrips.protocol.traits.provider import IProtocolProvider


class UserSlaveBroadcast(UserBroadcast, IAuthCheck, IInCheck, IProtocolProvider):
    """
    This broadcast adds an existing user. It does not support login features.
    """

    CHANNEL_RESPONSE_NS = 'notify'
    CHANNEL_RESPONSE_CODE_RESPONSE = 'response'

    CHANNEL_NS = 'channel'
    CHANNEL_CODE_JOIN = 'join'
    CHANNEL_CODE_FORCED_JOIN = 'forced-join'
    CHANNEL_CODE_JOINED = 'joined'
    CHANNEL_CODE_PART = 'part'
    CHANNEL_CODE_FORCED_PART = 'forced-part'
    CHANNEL_CODE_PARTED = 'parted'

    CHANNEL_RESULT_ALLOW_JOIN = 'join-accepted'
    CHANNEL_RESULT_DENY_JOIN = 'join-rejected'
    CHANNEL_RESULT_DENY_JOIN_ALREADY_IN = 'already-in-channel'
    CHANNEL_RESULT_ALLOW_PART = 'part-accepted'
    CHANNEL_RESULT_DENY_PART = 'part-rejected'
    CHANNEL_RESULT_DENY_PART_NOT_IN = 'not-in-channel'

    @classmethod
    def part_criteria(cls, user):
        return lambda u, command, *args, **kwargs: True

    @classmethod
    def join_criteria(cls, user):
        return lambda u, command, *args, **kwargs: True

    @classmethod
    def _part_criteria(cls, user):
        return IBroadcast.BROADCAST_FILTER_AND(IBroadcast.BROADCAST_FILTER_OTHERS(user), cls.part_criteria(user))

    @classmethod
    def _join_criteria(cls, user):
        return IBroadcast.BROADCAST_FILTER_AND(IBroadcast.BROADCAST_FILTER_OTHERS(user), cls.join_criteria(user))

    @classmethod
    def specification(cls):
        return {
            cls.CHANNEL_NS: {
                cls.CHANNEL_CODE_JOIN: 'server',
                cls.CHANNEL_CODE_FORCED_JOIN: 'client',
                cls.CHANNEL_CODE_PART: 'server',
                cls.CHANNEL_CODE_FORCED_PART: 'client',
                cls.CHANNEL_CODE_JOINED: 'client',
                cls.CHANNEL_CODE_PARTED: 'client',
            },
            cls.CHANNEL_RESPONSE_NS: {
                cls.CHANNEL_RESPONSE_CODE_RESPONSE: 'client'
            }
        }

    @classmethod
    def specification_handlers(cls, master_instance):
        return {
            cls.CHANNEL_NS: {
                cls.CHANNEL_CODE_JOIN: lambda socket, message: cls.route(master_instance, message, socket).command_join(),
                cls.CHANNEL_CODE_PART: lambda socket, message: cls.route(master_instance, message, socket).command_join(),
            },
        }

    def __init__(self, key, master, *args, **kwargs):
        """
        Instantiates a slave broadcast by specifying a master, and register/unregister list handlers.
        """
        super(UserSlaveBroadcast, self).__init__(key, master=master, *args, **kwargs)

        def register(list, instance):
            self.broadcast((self.CHANNEL_NS, self.CHANNEL_CODE_JOINED), filter=self._join_criteria(instance), user=instance.key)

        def unregister(list, instance, by_val):
            self.broadcast((self.CHANNEL_NS, self.CHANNEL_CODE_PARTED), filter=self._part_criteria(instance), user=instance.key)

        self.list.events.remove.register(unregister)
        self.list.events.insert.register(register)

    def auth_check(self, socket, state=True):
        """
        Delegates the auth check in the assigned-to master.
        """
        return self.master.auth_check(socket, state)

    def auth_get(self, socket):
        """
        Delegates the auth get in the assigned-to master.
        """
        return self.master.auth_get(socket)

    def in_check(self, socket, state=True):
        """
        Determines whether the socket is (logged-in and) in the channel.
        If the user is not logged in, None is returned. Otherwise, it is
          returned whether the in-state matches the passed state.
        """
        if not self.auth_check(socket):
            return None

        result = None
        user = self.auth_get(socket)
        user_in = user and user in self.users()

        if state and not user_in:
            result = self._result_deny(self.CHANNEL_RESULT_DENY_PART_NOT_IN)
        elif not state and user_in:
            result = self._result_deny(self.CHANNEL_RESULT_DENY_JOIN_ALREADY_IN)

        if result:
            socket.send_message(self.CHANNEL_RESPONSE_NS, self.CHANNEL_RESPONSE_CODE_RESPONSE, result=result)
            return False
        return True

    def register(self, user, *args, **kwargs):
        """
        Inserts a user instance (arguments are ignored).
        """
        return self.list.insert(user)

    def force_part(self, user, *args, **kwargs):
        """
        Forces a user to be removed from the slave.
        """
        if user in self.users():
            self.unregister(self.users()[user])
            user.socket.send_message(self.CHANNEL_NS, self.CHANNEL_CODE_FORCED_PART, *args, **kwargs)
            return True
        else:
            return False

    def force_join(self, user, *args, **kwargs):
        """
        Forces a user to be added to the slave.
        """
        if user in self.master.users() and user not in self.users():
            self.register(self.master.users()[user])
            user.socket.send_message(self.CHANNEL_NS, self.CHANNEL_CODE_FORCED_JOIN, *args, **kwargs)
            return True
        else:
            return False

    command_join = IInCheck.out_required(AccessControlledAction(
        lambda obj, socket: obj._command_is_allowed_join(socket),
        lambda obj, result: obj._accepts(result),
        lambda obj, result, socket: obj._command_accepted_join(result, socket),
        lambda obj, result, socket: obj._command_rejected_join(result, socket)
    ).as_method("""
    Lets a user join this slave.
    """))

    command_part = IInCheck.in_required(AccessControlledAction(
        lambda obj, socket: obj._command_is_allowed_part(socket),
        lambda obj, result: obj._accepts(result),
        lambda obj, result, socket: obj._command_accepted_part(result, socket),
        lambda obj, result, socket: obj._command_rejected_part(result, socket)
    ).as_method("""
    Lets a user leave this slave.
    """))

    def _command_is_allowed_join(self, socket):
        """
        Determines whether the current socket is allowed to join the slave.
        """
        return self._result_deny(self.CHANNEL_RESULT_DENY_JOIN)

    def _command_accepted_join(self, result, socket):
        """
        The join command was accepted.
        """
        self.register(self.auth_get(socket))
        socket.send_message(self.CHANNEL_RESPONSE_NS, self.CHANNEL_RESPONSE_CODE_RESPONSE, result=result, channel=self.key)

    def _command_rejected_join(self, result, socket):
        """
        The join command was rejected.
        """
        socket.send_message(self.CHANNEL_RESPONSE_NS, self.CHANNEL_RESPONSE_CODE_RESPONSE, result=result, channel=self.key)

    def _command_is_allowed_part(self, socket):
        """
        Determines whether the current socket is allowed to leave the slave.
        """
        return self._result_deny(self.CHANNEL_RESULT_DENY_PART)

    def _command_accepted_part(self, result, socket):
        """
        The part command was accepted.
        """
        self.unregister(self.auth_get(socket))
        socket.send_message(self.CHANNEL_RESPONSE_NS, self.CHANNEL_RESPONSE_CODE_RESPONSE, result=result, channel=self.key)

    def _command_rejected_part(self, result, socket):
        """
        The leave command was rejected.
        """
        socket.send_message(self.CHANNEL_RESPONSE_NS, self.CHANNEL_RESPONSE_CODE_RESPONSE, result=result, channel=self.key)