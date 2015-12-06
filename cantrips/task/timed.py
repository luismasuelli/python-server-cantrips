from six import integer_types
from cantrips.types.exception import factory
from .features import TornadoTimerFeature, TwistedTimerFeature, ThreadedTimerFeature


class Timeout(object):
    """
    Lets a timeout be triggered and cancelled. If the timeout is reached,
      an exception is triggered.

    Notes: Processors should be able to launch their own timeouts by using these implementations.
    """

    Error = factory(['ALREADY_RUNNING', 'STILL_RUNNING', 'NOT_RUNNING', 'COULDNT_RUN'])

    def __init__(self, seconds, on_reach):
        """
        Initializes the timeout.
        """
        self.__time = seconds if isinstance(seconds, integer_types + (float,)) else 15
        self.__reached = None
        self.__on_reach = on_reach if callable(on_reach) else lambda o: None

    def _set(self, seconds, callback):
        """
        Creates the timeout.
        """
        raise NotImplementedError

    def _unset(self):
        """
        Unsets the timeout. Must be overriden since it is
          framework dependent.
        """
        raise NotImplementedError

    def _reach(self, forced=False):
        """
        Terminates a timeout, if it is not already reached.
        """
        if self.__reached is not False:
            self.__reached = True
            self._unset()
            self.__on_reach(self, forced)

    def start(self):
        """
        Starts a timeout, with default implementation.
        """
        if self.__reached is False:
            # It is an error since the timeout is already running.
            raise self.Error("Timeout already running", self.Error.ALREADY_RUNNING)
        self.__reached = False
        self._set(self.__time, lambda: self._reach(False))

    def force_stop(self):
        """
        Forces the timeout to terminate.
        """
        if self.__reached is not False:
            # It is an error since the timeout is not running.
            raise self.Error("Timeout not running", self.Error.NOT_RUNNING)
        self._reach(True)

    def reset(self):
        """
        Resets the reached state to None.
        """
        if self.__reached is False:
            # It is an error since the timeout is still running.
            raise self.Error("Timeout still running", self.Error.STILL_RUNNING)
        self.__reached = None


class TornadoTimeout(Timeout):
    """
    Timeouts implemented in Tornado.
    """

    def __init__(self, ioloop, seconds, on_reach):
        self.__create_timeout, self.__cancel_timeout = TornadoTimerFeature.import_it()
        self.__ioloop = ioloop
        self.__timer = None
        super(TornadoTimeout, self).__init__(seconds, on_reach)

    def _unset(self):
        try:
            self.__cancel_timeout(self.__ioloop, self.__timer)
        except Exception as e:
            pass

    def _set(self, seconds, callback):
        try:
            self.__timer = self.__create_timeout(self.__ioloop, seconds, callback)
        except Exception as e:
            raise self.Error("Couldn't run timer", self.Error.COULDNT_RUN, e)


class TwistedTimeout(Timeout):
    """
    Timeouts implemented in Twisted.
    """

    def __init__(self, reactor, seconds, on_reach):
        self.__create_timeout, self.__cancel_timeout = TwistedTimerFeature.import_it()
        self.__reactor = reactor
        self.__timer = None
        super(TwistedTimeout, self).__init__(seconds, on_reach)

    def _unset(self):
        try:
            self.__cancel_timeout(self.__timer)
        except Exception as e:
            pass

    def _set(self, seconds, callback):
        try:
            self.__timer = self.__create_timeout(self.__reactor, seconds, callback)
        except Exception as e:
            raise self.Error("Couldn't run timer", self.Error.COULDNT_RUN, e)


class ThreadedTimeout(Timeout):
    """
    Timeouts implemented with Threads
    """

    def __init__(self, seconds, on_reach):
        self.__create_timeout, self.__cancel_timeout = ThreadedTimerFeature.import_it()
        self.__timer = None
        super(ThreadedTimeout, self).__init__(seconds, on_reach)

    def _unset(self):
        try:
            self.__cancel_timeout(self.__timer)
        except Exception as e:
            pass

    def _set(self, seconds, callback):
        try:
            self.__timer = self.__create_timeout(seconds, callback)
        except Exception as e:
            raise self.Error("Couldn't run timer", self.Error.COULDNT_RUN, e)