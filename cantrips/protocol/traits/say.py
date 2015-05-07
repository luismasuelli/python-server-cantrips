from cantrips.patterns.actions import AccessControlledAction
from cantrips.patterns.broadcast import IBroadcast
from cantrips.protocol.messaging.formats import CommandSpec
from cantrips.protocol.traits.decorators.authcheck import IAuthCheck
from cantrips.protocol.traits.decorators.incheck import IInCheck
from cantrips.protocol.traits.permcheck import PermCheck
from cantrips.protocol.traits.provider import IProtocolProvider


class SayBroadcast(IBroadcast, PermCheck, IProtocolProvider, IAuthCheck, IInCheck):
    """
    This trait, applied to an existent broadcast, lets users
      to publish messages to the whole broadcast.
    """

    SAY_NS = CommandSpec('say', 0x00000011)
    SAY_CODE_SAY = CommandSpec('say', 0x00000001)
    SAY_CODE_SAID = CommandSpec('said', 0x00010001)

    SAY_RESPONSE_NS = CommandSpec('notify', 0x80000001)
    SAY_RESPONSE_CODE_RESPONSE = CommandSpec('response', 0x00000001)

    SAY_RESULT_ALLOW = CommandSpec('ok', 0x00000001)

    @classmethod
    def specification(cls):
        return {
            cls.formatted('SAY_NS'): {
                cls.formatted('SAY_CODE_SAY'): 'server',
                cls.formatted('SAY_CODE_SAID'): 'client'
            },
            cls.formatted('SAY_RESPONSE_NS'): {
                cls.formatted('SAY_RESPONSE_CODE_RESPONSE'): 'client'
            }
        }

    @classmethod
    def specification_handlers(cls, master_instance):
        return {
            cls.formatted('SAY_NS'): {
                cls.formatted('SAY_CODE_SAY'): lambda socket, message: cls.route(master_instance, message, socket).command_say(message.message),
            },
        }

    command_say = IInCheck.in_required(AccessControlledAction(
        lambda obj, socket, message: obj._command_is_allowed_say(socket, message),
        lambda obj, result: obj._accepts(result),
        lambda obj, result, socket, message: obj._command_accepted_say(result, socket, message),
        lambda obj, result, socket, message: obj._command_rejected_say(result, socket, message),
    ).as_method("""
    A user (given by key or instance) can send a message to the broadcast.
    This is restricted to users already subscribed to the broadcast.

    To customize the protocol for this command, refer and override each SAY_* class member.
    """))

    def _command_is_allowed_say(self, socket, message):
        """
        Determines whether the user is allowed to send a message.

        Primitive check - allow only connected users.
        """
        return self._result_allow(self.formatted('SAY_RESULT_ALLOW'))

    def _command_accepted_say(self, result, socket, message):
        """
        User message was accepted. Notify the user AND broadcast the message to other users.
        """
        socket.send_message(self.formatted('SAY_RESPONSE_NS'), self.formatted('SAY_RESPONSE_CODE_RESPONSE'), result=result, message=message)
        others = IBroadcast.BROADCAST_FILTER_OTHERS(self.users()[self.auth_get(socket)])
        self.broadcast((self.formatted('SAY_NS'), self.formatted('SAY_CODE_SAID')), user=self.auth_get(socket).key, message=message, filter=others)

    def _command_rejected_say(self, result, socket, message):
        """
        User message was rejected.
        """
        socket.send_message(self.formatted('SAY_RESPONSE_NS'), self.formatted('SAY_RESPONSE_CODE_RESPONSE'), result=result, message=message)