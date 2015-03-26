from cantrips.patterns.actions import AccessControlledAction
from cantrips.patterns.broadcast import IBroadcast
from cantrips.protocol.traits.decorators.authcheck import IAuthCheck
from cantrips.protocol.traits.permcheck import PermCheck
from cantrips.protocol.traits.provider import IProtocolProvider


class WhisperBroadcast(IBroadcast, PermCheck, IProtocolProvider, IAuthCheck):
    """
    This traits, applied to an existent broadcast, lets a
      user send a private message to another user. Both
      users must belong to the current broadcast.
    """

    WHISPER_NS = 'whisper'
    WHISPER_CODE_WHISPER = 'whisper'
    WHISPER_CODE_WHISPERED = 'whispered'

    WHISPER_RESPONSE_NS = 'notify'
    WHISPER_RESPONSE_CODE_RESPONSE = 'response'

    WHISPER_RESULT_DENY_TARGET_NOT_IN = 'target-not-in'
    WHISPER_RESULT_DENY_TARGET_ITS_YOU = 'target-its-you'
    WHISPER_RESULT_ALLOW = 'ok'

    @classmethod
    def specification(cls):
        return {
            cls.WHISPER_NS: {
                cls.WHISPER_CODE_WHISPER: 'server',
                cls.WHISPER_CODE_WHISPERED: 'client'
            },
            cls.WHISPER_RESPONSE_NS: {
                cls.WHISPER_RESPONSE_CODE_RESPONSE: 'client'
            }
        }

    @classmethod
    def specification_handlers(cls, master_instance):
        return {
            cls.WHISPER_NS: {
                cls.WHISPER_CODE_WHISPER: lambda socket, message: cls.route(master_instance, message, socket).command_whisper(message.target, message.message),
            }
        }

    command_whisper = IAuthCheck.login_required(AccessControlledAction(
        lambda obj, socket, target, message: obj._command_is_allowed_whisper(socket, target, message),
        lambda obj, result: obj._accepts(result),
        lambda obj, result, socket, target, message: obj._command_accepted_whisper(result, socket, target, message),
        lambda obj, result, socket, target, message: obj._command_rejected_whisper(result, socket, target, message),
    ).as_method("""
    A user (given by key or instance) can send a message to another user in the broadcast.
    This is restricted to users already subscribed to the broadcast (both must belong).

    To customize the protocol for this command, refer and override each WHISPER_* class member.

    Target user should not be an instance but a key, unless the instance implements an appropiate
      serialization mechanism.
    """))

    def _command_is_allowed_whisper(self, socket, target, message):
        """
        Determines whether the user is allowed to whisper a message to another user.

        Primitive check - allow only connected users (both user and target).
        """
        if target not in self.users():
            return self._result_deny(self.WHISPER_RESULT_DENY_TARGET_NOT_IN)
        if target == self.auth_get(socket).key:
            return self._result_deny(self.WHISPER_RESULT_DENY_TARGET_ITS_YOU)
        return self._result_allow(self.WHISPER_RESULT_ALLOW)

    def _command_accepted_whisper(self, result, socket, target, message):
        """
        User message was accepted. Notify the user AND broadcast the message to other users.
        """
        socket.send_message(self.WHISPER_RESPONSE_NS, self.WHISPER_RESPONSE_CODE_RESPONSE, result=result, target=target, message=message)
        self.notify(target, (self.WHISPER_NS, self.WHISPER_CODE_WHISPERED), sender=self.auth_get(socket).key, message=message)

    def _command_rejected_whisper(self, result, socket, target, message):
        """
        User message was rejected.
        """
        socket.send_message(self.WHISPER_RESPONSE_NS, self.WHISPER_RESPONSE_CODE_RESPONSE, result=result, target=target, message=message)