from cantrips.protocol.messaging.formats import FORMAT_STRING


class IFormatteable(object):
    """
    Determines the current format to be used in identifiers. Allows to specify a current
      format, and pick the appropiate format value for a specified CommandSpec.
    """

    COMMAND_FORMAT = FORMAT_STRING

    @classmethod
    def formatted(cls, prop):
        """
        Takes a property, expected to be a CommandSpec, and chooses one of its values, based on the
          value in COMMAND_FORMAT
        """
        return getattr(cls, prop)[cls.COMMAND_FORMAT]