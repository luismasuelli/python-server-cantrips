from functools import wraps


class IInCheck(object):

    def in_check(self, socket, state=True):
        """
        Checks whether the user is in the specified belonging state.
        This means:
          It must check whether the user is in (if state=True)
          or not in (if state=False) the current channel.
        The implementation should return True if the user matches the
          specified belonging state.
        """
        raise NotImplementedError

    @staticmethod
    def in_required(f):
        """
        Wraps a method by ensuring that sockets (first argument)
          are in.
        """
        @wraps(f)
        def wrapped(self, socket, *args, **kwargs):
            if self.in_check(socket, True):
                f(self, socket, *args, **kwargs)
        return wrapped

    @staticmethod
    def out_required(f):
        """
        Wraps a method by ensuring that sockets (first argument)
          are not in.
        """
        @wraps(f)
        def wrapped(self, socket, *args, **kwargs):
            if self.in_check(socket, False):
                f(self, socket, *args, **kwargs)
        return wrapped