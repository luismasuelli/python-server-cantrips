from cantrips.iteration import items


class IProtocolProvider(object):
    """
    Must implement a method returning the chunk of protocol
      operations to implement. It is intended that broadcast
      traits implement this interface as well, so they can
      build the protocol automatically.
    """

    @classmethod
    def specification(cls):
        """
        Should return dict {ns => {code: direction}}
        """

        raise NotImplementedError

    @classmethod
    def specification_handlers(cls, master_instance):
        """
        Should return dict {ns => {code: handler}}. Only has sense for (server|both)-direction
          codes. It takes a master broadcast instance to get the handlers from.
        """

        raise NotImplemented

    @staticmethod
    def specifications(*args):
        """
        Should return a specification iterating many given providers. Note:
          If one spec declares a namespace, and another spec declares the same
          namespace, the content of the namespace will be updated with such new
          content.
        """

        total_specs = {}
        for provider in args:
            for key, value in items(provider.specification()):
                total_specs.setdefault(key, {}).update(value)
        return total_specs

    @staticmethod
    def specifications_handlers(master_instance, *args):
        """
        Should return a specification handlers iterating many given providers.
        """

        total_specs = {}
        for provider in args:
            for key, value in items(provider.specification_handlers(master_instance)):
                total_specs.setdefault(key, {}).update(value)
        return total_specs

    @classmethod
    def route(cls, master, message, socket):
        """
        Determines, based on whether the trait is intended or not for slave/master, the
          broadcast to use: the master itself or a slave given by a key.
        """
        return master.forward(socket, message.kwargs.get(getattr(cls, 'SLAVE_KEY_ATTR', 'slave'), None) if not getattr(cls, 'MASTER_TRAIT', False) else None)