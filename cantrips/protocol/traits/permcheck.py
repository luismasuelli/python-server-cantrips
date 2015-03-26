class PermCheck(object):
    """
    A default implementation and helpers for `accepts(result)` method
      for the AccessControlledAction user classes.
    """

    def _accepts(self, result):
        return 'allow' in result

    def _result_allow(self, reason):
        """
        A result created with this method will be allowed by _accepts(result)
          in this class.
        """
        return {'allow': reason}

    def _result_deny(self, reason):
        """
        A result created with this method will be denied by _accepts(result)
          in this class.
        """
        return {'deny': reason}
