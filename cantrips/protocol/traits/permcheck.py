from cantrips.protocol.traits.formatteable import IFormatteable
from cantrips.protocol.messaging.formats import CommandSpec


class PermCheck(IFormatteable):
    """
    A default implementation and helpers for `accepts(result)` method
      for the AccessControlledAction user classes.
    """

    ALLOW = CommandSpec('allow', 0x00000001)
    DENY = CommandSpec('deny', 0x00000002)

    def _accepts(self, result):
        return self.formatted('ALLOW') in result

    def _result_allow(self, reason):
        """
        A result created with this method will be allowed by _accepts(result)
          in this class.
        """
        return {self.formatted('ALLOW'): reason}

    def _result_deny(self, reason):
        """
        A result created with this method will be denied by _accepts(result)
          in this class.
        """
        return {self.formatted('DENY'): reason}
