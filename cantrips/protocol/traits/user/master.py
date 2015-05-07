from cantrips.iteration import items
from cantrips.patterns.identify import List
from cantrips.patterns.actions import AccessControlledAction
from cantrips.protocol.messaging.formats import CommandSpec
from cantrips.protocol.traits.user.base import UserBroadcast
from cantrips.protocol.traits.provider import IProtocolProvider
from cantrips.protocol.traits.decorators.authcheck import IAuthCheck, IAuthHandle
from cantrips.functions import is_method, METHOD_BOUND


class ForwardNone(object):
    """
    This is a dummy object which does nothing when calls are performed on it.
    """

    class Dummy(object):
        """
        Dummy object. It should be considered null or empty.
        """

        def __call__(self, *args, **kwargs):
            pass

        def __bool__(self):
            return False

        def __nonzero__(self):
            return False

        def __int__(self):
            return 0

        def __long__(self):
            return 0

        def __str__(self):
            return ""

        def __unicode__(self):
            return u""

    def __getattr__(self, item):
        return self.Dummy()


class ForwardBroadcast(object):
    """
    Takes a slave and a socket. Calls are forwarded to the slave, proxy-passing
      the socket to the call. This means:

      master.forward(slave, socket).mycall(a, b, c=c)

      is equivalent to:

      slave.my_call(socket, a, b, c=c)

    If the instance returns a non-method value, then nothing is forwarded. This
      includes, but does not limit to, @staticmethod-decorated functions
    """

    def __init__(self, slave, socket):
        self.slave = slave
        self.socket = socket

    def __getattr__(self, item):
        value = getattr(self.slave, item, None)
        if is_method(value, METHOD_BOUND):
            return lambda *a, **kwa: value(self.socket, *a, **kwa)
        return value


class UserMasterBroadcast(UserBroadcast, IProtocolProvider, IAuthHandle):
    """
    This broadcast creates a user - it supports login features.
    """

    AUTHENTICATE_NS = CommandSpec('auth', 0x00000001)
    AUTHENTICATE_CODE_LOGIN = CommandSpec('login', 0x00000001)
    AUTHENTICATE_CODE_LOGOUT = CommandSpec('logout', 0x00000002)
    AUTHENTICATE_CODE_FORCED_LOGOUT = CommandSpec('forced-logout', 0x00010001)

    AUTHENTICATE_RESPONSE_NS = CommandSpec('notify', 0x80000001)
    AUTHENTICATE_RESPONSE_CODE_RESPONSE = CommandSpec('response', 0x00000001)

    AUTHENTICATE_RESULT_DENY_NO_ACTIVE_SESSION = CommandSpec('no-active-session', 0x00010001)
    AUTHENTICATE_RESULT_DENY_ALREADY_ACTIVE_SESSION = CommandSpec('already-active-session', 0x00010002)
    AUTHENTICATE_RESULT_DENY_INVALID = CommandSpec('invalid-login', 0x00010003)
    AUTHENTICATE_RESULT_ALLOW_LOGGED_IN = CommandSpec('logged-in', 0x00000001)
    AUTHENTICATE_RESULT_ALLOW_LOGGED_OUT = CommandSpec('logged-out', 0x00000002)

    CHANNEL_NS = CommandSpec('channel', 0x00000002)
    CHANNEL_CODE_CREATE = CommandSpec('create', 0x00000001)
    CHANNEL_CODE_CLOSE = CommandSpec('close', 0x00000002)

    CHANNEL_RESPONSE_NS = CommandSpec('notify', 0x80000001)
    CHANNEL_RESPONSE_CODE_RESPONSE = CommandSpec('response', 0x00000001)

    CHANNEL_RESULT_DENY_CREATE = CommandSpec('cannot-create-channel', 0x00010001)
    CHANNEL_RESULT_DENY_CLOSE = CommandSpec('cannot-close-channel', 0x00010002)
    CHANNEL_RESULT_DENY_UNEXISTENT = CommandSpec('unexistent-channel', 0x00010003)
    CHANNEL_RESULT_DENY_EXISTENT = CommandSpec('already-existent', 0x00010004)

    SPECIAL_SLAVE_UNREGISTER = CommandSpec('slave-unregister', 0x00010001)
    SPECIAL_USER_UNREGISTER = CommandSpec('user-unregister', 0x00010002)

    MASTER_TRAIT = True

    @classmethod
    def specification(cls):
        return {
            cls.formatted('AUTHENTICATE_NS'): {
                cls.formatted('AUTHENTICATE_CODE_LOGIN'): 'server',
                cls.formatted('AUTHENTICATE_CODE_LOGOUT'): 'server',
                cls.formatted('AUTHENTICATE_CODE_FORCED_LOGOUT'): 'client'
            },
            cls.formatted('AUTHENTICATE_RESPONSE_NS'): {
                cls.formatted('AUTHENTICATE_RESPONSE_CODE_RESPONSE'): 'client'
            },
            cls.formatted('CHANNEL_NS'): {
                cls.formatted('CHANNEL_CODE_CREATE'): 'server',
                cls.formatted('CHANNEL_CODE_CLOSE'): 'server',
            },
            cls.formatted('CHANNEL_RESPONSE_NS'): {
                cls.formatted('CHANNEL_RESPONSE_CODE_RESPONSE'): 'client'
            }
        }

    @classmethod
    def specification_handlers(cls, master_instance):
        return {
            cls.formatted('AUTHENTICATE_NS'): {
                cls.formatted('AUTHENTICATE_CODE_LOGIN'): lambda socket, message: cls.route(master_instance, message, socket).command_login(*message.args, **message.kwargs),
                cls.formatted('AUTHENTICATE_CODE_LOGOUT'): lambda socket, message: cls.route(master_instance, message, socket).command_logout(*message.args, **message.kwargs),
            },
            cls.formatted('CHANNEL_NS'): {
                cls.formatted('CHANNEL_CODE_CREATE'): lambda socket, message: cls.route(master_instance, message, socket).command_create_slave(message.args[0], *message.args[1:], **message.kwargs),
                cls.formatted('CHANNEL_CODE_CLOSE'): lambda socket, message: cls.route(master_instance, message, socket).command_close_slave(message.args[0], *message.args[1:], **message.kwargs),
            },
        }

    def __init__(self, key, slave_class, *args, **kwargs):
        """
        Instantiates a master broadcast by creating a slaves  list, and some list handlers.
        """
        super(UserMasterBroadcast, self).__init__(key, master=self, slaves=List(slave_class), *args, **kwargs)

        def unregister_slave(list, instance, by_val):
            for ukey, user in instance.users():
                instance.force_part(user, special=self.formatted('SPECIAL_SLAVE_UNREGISTER'))

        def unregister_user(list, instance, by_val):
            for skey, slave in items(instance.slaves()):
                slave.force_part(instance, special=self.formatted('SPECIAL_USER_UNREGISTER'))

        self.slaves.events.remove.register(unregister_slave)
        self.list.events.remove.register(unregister_user)

    #######################
    # Funciones utilitarias
    #######################

    def register(self, user, *args, **kwargs):
        """
        Creates a user (arguments are considered) on master lists.
        """
        return self.list.create(user, *args, **kwargs)

    def slave_register(self, key, *args, **kwargs):
        """
        Creates a slave, based on given arguments.
        """
        return self.slaves.create(key, self, *args, **kwargs)

    def slave_unregister(self, key, *args, **kwargs):
        """
        Destroys a slave, based on its arguments.
        """
        return self.slaves.remove(key)

    def auth_check(self, socket, state=True):
        """
        Determines whether the socket is logged in or not.
        """
        user = getattr(socket, 'end_point', None)
        user_in = user and user in self.users()
        result = None

        if state and not user_in:
            result = self._result_deny(self.formatted('AUTHENTICATE_RESULT_DENY_NO_ACTIVE_SESSION'))
        elif not state and user_in:
            result = self._result_deny(self.formatted('AUTHENTICATE_RESULT_DENY_ALREADY_ACTIVE_SESSION'))

        if result:
            socket.send_message(self.formatted('AUTHENTICATE_RESPONSE_NS'), self.formatted('AUTHENTICATE_RESPONSE_CODE_RESPONSE'), result=result)
            return False
        return True

    def auth_set(self, socket, *args, **kwargs):
        """
        Sets the end_point attribute on the socket to the current user.
        """
        socket.end_point = kwargs['end_point']

    def auth_get(self, socket):
        """
        Gets the auth state from the socket: the user object.
        """
        return getattr(socket, 'end_point', None)

    def auth_clear(self, socket):
        """
        Unsets the end_point attribute on the socket.
        """
        del socket.end_point

    ############################
    # Funciones semi-utilitarias
    ############################

    def force_logout(self, user, *args, **kwargs):
        """
        Logs a user out, by key. The user will be notified. Since this
          command is not executed by the client, it must return a boolean
          value indicating whether the user was logged in or not.
        """
        if user in self.users():
            self.unregister(self.users()[user], *args, **kwargs)
            user.socket.send_message(self.formatted('AUTHENTICATE_NS'), self.formatted('AUTHENTICATE_CODE_FORCED_LOGOUT'), *args, **kwargs)
            return True
        else:
            return False

    ################################################
    # Funciones de comando (emitidos por el usuario)
    ################################################

    command_login = IAuthCheck.logout_required(AccessControlledAction(
        lambda obj, socket, *args, **kwargs: obj._command_is_allowed_login(socket, *args, **kwargs),
        lambda obj, result: obj._accepts(result),
        lambda obj, result, socket, *args, **kwargs: obj._command_accepted_login(result, socket, *args, **kwargs),
        lambda obj, result, socket, *args, **kwargs: obj._command_rejected_login(result, socket, *args, **kwargs),
    ).as_method("""
    Allows sockets to log-in to the server. Its check (_login_command_is_allowed) MUST be implemented.
    """))

    command_logout = IAuthCheck.login_required(AccessControlledAction(
        lambda obj, socket, *args, **kwargs: obj._command_is_allowed_logout(socket, *args, **kwargs),
        lambda obj, result: obj._accepts(result),
        lambda obj, result, socket, *args, **kwargs: obj._command_accepted_logout(result, socket, *args, **kwargs),
        lambda obj, result, socket, *args, **kwargs: obj._command_rejected_logout(result, socket, *args, **kwargs),
    ).as_method("""
    Allows sockets to log-out from the server.
    """))

    command_create_slave = IAuthCheck.login_required(AccessControlledAction(
        lambda obj, socket, slave_name, *args, **kwargs: obj._command_is_allowed_create_slave(socket, slave_name, *args, **kwargs),
        lambda obj, result: obj.accepts(result),
        lambda obj, result, socket, slave_name, *args, **kwargs: obj._command_accepted_create_slave(result, socket, slave_name, *args, **kwargs),
        lambda obj, result, socket, slave_name, *args, **kwargs: obj._command_rejected_create_slave(result, socket, slave_name, *args, **kwargs)
    ).as_method("""
    Allows users to create slave broadcasts.
    """))

    command_close_slave = IAuthCheck.login_required(AccessControlledAction(
        lambda obj, socket, slave_name, *args, **kwargs: obj._command_is_allowed_close_slave(socket, slave_name, *args, **kwargs),
        lambda obj, result: obj.accepts(result),
        lambda obj, result, socket, slave_name, *args, **kwargs: obj._command_accepted_close_slave(result, socket, slave_name, *args, **kwargs),
        lambda obj, result, socket, slave_name, *args, **kwargs: obj._command_rejected_close_slave(result, socket, slave_name, *args, **kwargs)
    ).as_method("""
    Allows users to close/destroy broadcasts.
    """))

    def _command_login_impl(self, socket, *args, **kwargs):
        """
        Performs log-in. Should return a triple:
          (user key, user args, user kwargs).
        """
        raise NotImplementedError

    def _command_is_allowed_create_slave(self, socket, slave_name, *args, **kwargs):
        """
        States whether the user is allowed to create the slave.
        """
        return self._result_deny(self.formatted('CHANNEL_RESULT_DENY_CREATE'))

    def _command_accepted_create_slave(self, result, socket, slave_name, *args, **kwargs):
        """
        Handles when the slave creation succeeds.
        """
        self.slave_register(slave_name, *args, **kwargs)
        socket.send_message(self.formatted('CHANNEL_RESPONSE_NS'), self.formatted('CHANNEL_RESPONSE_CODE_RESPONSE'), result=result, channel=slave_name)

    def _command_rejected_create_slave(self, result, socket, slave_name, *args, **kwargs):
        """
        Handles when the slave creation fails.
        """
        socket.send_message(self.formatted('CHANNEL_RESPONSE_NS'), self.formatted('CHANNEL_RESPONSE_CODE_RESPONSE'), result=result, channel=slave_name)

    def _command_is_allowed_close_slave(self, socket, slave_name, *args, **kwargs):
        """
        States whether the user is allowed to close the slave.
        """
        return self._result_deny(self.formatted('CHANNEL_RESULT_DENY_CLOSE'))

    def _command_accepted_close_slave(self, result, socket, slave_name, *args, **kwargs):
        """
        Handles when the slave closure succeeds.
        """
        self.slave_unregister(slave_name, *args, **kwargs)
        socket.send_message(self.formatted('CHANNEL_RESPONSE_NS'), self.formatted('CHANNEL_RESPONSE_CODE_RESPONSE'), result=result, channel=slave_name)

    def _command_rejected_close_slave(self, result, socket, slave_name, *args, **kwargs):
        """
        Handles when the slave closure fails.
        """
        socket.send_message(self.formatted('CHANNEL_RESPONSE_NS'), self.formatted('CHANNEL_RESPONSE_CODE_RESPONSE'), result=result, channel=slave_name)

    def _command_is_allowed_login(self, socket, *args, **kwargs):
        """
        Checks whether a user must be allowed, or not, to log-in (e.g. bad user/password).
        """
        return self._command_login_impl(socket, *args, **kwargs) or self._result_deny(self.formatted('AUTHENTICATE_RESULT_DENY_INVALID'))

    def _command_accepted_login(self, result, socket, *args, **kwargs):
        """
        Accepts the login attempt and registers the user in the broadcast.
        """
        user_key, user_args, user_kwargs = result
        self.auth_set(socket, end_point=self.register(user_key, *user_args, **user_kwargs))
        socket.send_message(self.formatted('AUTHENTICATE_RESPONSE_NS'), self.formatted('AUTHENTICATE_RESPONSE_CODE_RESPONSE'), result=result)

    def _command_rejected_login(self, result, socket, *args, **kwargs):
        """
        Rejects the login attempt with the gotten result.
        """
        socket.send_message(self.formatted('AUTHENTICATE_RESPONSE_NS'), self.formatted('AUTHENTICATE_RESPONSE_CODE_RESPONSE'), result=result)

    def _command_is_allowed_logout(self, socket, *args, **kwargs):
        """
        Checks whether the socket should be allowed to logout.
        """
        return self._result_allow(self.formatted('AUTHENTICATE_RESULT_ALLOW_LOGGED_OUT'))

    def _command_accepted_logout(self, result, socket, *args, **kwargs):
        """
        Accepts the logout command and cleans the user_endpoint.
        """
        self.unregister(socket.user_endpoint, *args, **kwargs)
        self.auth_clear(socket)
        socket.send_message(self.formatted('AUTHENTICATE_RESPONSE_NS'), self.formatted('AUTHENTICATE_RESPONSE_CODE_RESPONSE'), result=result)

    def _command_rejected_logout(self, result, socket, *args, **kwargs):
        """
        Rejects the logout command, and cleans the user_endpoint (perhaps an expired session exists).
        """
        socket.send_message(self.formatted('AUTHENTICATE_RESPONSE_NS'), self.formatted('AUTHENTICATE_RESPONSE_CODE_RESPONSE'), result=result)

    #################################################################
    # Funciones auxiliares de comando (no lo resuelven por si mismas)
    #################################################################

    def forward(self, socket, channel=None):
        """
        Gets a forward object. Passed commands to such objects get the slave, and socket, proxied.
        If slave is not specified, the forward is for the current (master) object.

        This means:
           master.forward('my-room', socket).my_command(a, b, c=c)

        is equivalent to:
           master.slaves['my_room'].my_command(socket, a, b, c=c)

        with the exception that this utility considers the case where the slave does not exist, and
          process something according to such error. Calls done to the unexistent channel are ignored.
          You should watch out on not retrieving attributes other than to be called, since attributes
          are dummy functions or false values.
        """
        if channel is None:
            return ForwardBroadcast(self, socket)
        elif channel in self.slaves:
            return ForwardBroadcast(self.slaves[channel], socket)
        else:
            self._forward_invalid(socket, channel)
            return ForwardNone()

    def _forward_invalid(self, socket, channel):
        """
        What to do when a bad forward occurs (i.e. the channel does not exist).
        :param socket:
        :param channel:
        :return:
        """
        socket.send_message(self.formatted('CHANNEL_RESPONSE_NS'), self.formatted('CHANNEL_RESPONSE_CODE_RESPONSE'),
                            self._result_deny(self.formatted('CHANNEL_RESULT_DENY_UNEXISTENT')))