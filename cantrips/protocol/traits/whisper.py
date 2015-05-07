from cantrips.patterns.actions import AccessControlledAction
from cantrips.patterns.broadcast import IBroadcast
from cantrips.protocol.messaging.formats import CommandSpec
from cantrips.protocol.traits.decorators.authcheck import IAuthCheck
from cantrips.protocol.traits.permcheck import PermCheck
from cantrips.protocol.traits.provider import IProtocolProvider


class WhisperBroadcast(IBroadcast, PermCheck, IProtocolProvider, IAuthCheck):
    """
    This traits, applied to an existent broadcast, lets a
      user send a private message to another user. Both
      users must belong to the current broadcast.
    """

    WHISPER_NS = CommandSpec('whisper', 0x00000012)
    WHISPER_CODE_WHISPER = CommandSpec('whisper', 0x00000001)
    WHISPER_CODE_WHISPERED = CommandSpec('whispered', 0x00010001)

    WHISPER_RESPONSE_NS = CommandSpec('notify', 0x80000001)
    WHISPER_RESPONSE_CODE_RESPONSE = CommandSpec('response', 0x00000001)

    WHISPER_RESULT_DENY_TARGET_NOT_IN = CommandSpec('target-not-in', 0x00010001)
    WHISPER_RESULT_DENY_TARGET_ITS_YOU = CommandSpec('target-its-you', 0x00010002)
    WHISPER_RESULT_ALLOW = CommandSpec('ok', 0x00000001)

    @classmethod
    def specification(cls):
        return {
            cls.formatted('WHISPER_NS'): {
                cls.formatted('WHISPER_CODE_WHISPER'): 'server',
                cls.formatted('WHISPER_CODE_WHISPERED'): 'client'
            },
            cls.formatted('WHISPER_RESPONSE_NS'): {
                cls.formatted('WHISPER_RESPONSE_CODE_RESPONSE'): 'client'
            }
        }

    @classmethod
    def specification_handlers(cls, master_instance):
        return {
            cls.formatted('WHISPER_NS'): {
                cls.formatted('WHISPER_CODE_WHISPER'): lambda socket, message: cls.route(master_instance, message, socket).command_whisper(message.target, message.message),
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
            return self._result_deny(self.formatted('WHISPER_RESULT_DENY_TARGET_NOT_IN'))
        if target == self.auth_get(socket).key:
            return self._result_deny(self.formatted('WHISPER_RESULT_DENY_TARGET_ITS_YOU'))
        return self._result_allow(self.formatted('WHISPER_RESULT_ALLOW'))

    def _command_accepted_whisper(self, result, socket, target, message):
        """
        User message was accepted. Notify the user AND broadcast the message to other users.
        """
        socket.send_message(self.formatted('WHISPER_RESPONSE_NS'), self.formatted('WHISPER_RESPONSE_CODE_RESPONSE'), result=result, target=target, message=message)
        self.notify(target, (self.formatted('WHISPER_NS'), self.formatted('WHISPER_CODE_WHISPERED')), sender=self.auth_get(socket).key, message=message)

    def _command_rejected_whisper(self, result, socket, target, message):
        """
        User message was rejected.
        """
        socket.send_message(self.formatted('WHISPER_RESPONSE_NS'), self.formatted('WHISPER_RESPONSE_CODE_RESPONSE'), result=result, target=target, message=message)