from threading import Lock, Event
from cantrips.types.exception import factory
from .features import TornadoFutureFeature, TwistedDeferredFeature, ConcurrentFutureFeature


class Toll(object):
    """
    This lock is a one-pass lock. When you "demand" a value, the task is blocked until
      someone unblocks it by providing a value.

    It consists of two services:
      demand() - which will freeze the task until it is supplied a value. returns such value.
      supply(value) - which will provide a value for the task so it can be resumed.

    It is an error to demand again while it is already being demanded.
    It is also an error to supply any value to
    """

    Error = factory(['NOT_DEMANDING', 'ALREADY_DEMANDING'])

    def demanding(self):
        raise NotImplementedError()

    def _supply(self, value):
        raise NotImplementedError()

    def _demand(self):
        raise NotImplementedError()

    def supply(self, value):
        if not self.demanding():
            raise self.Error("Current toll is not demanding", self.Error.NOT_DEMANDING)
        return self._supply(value)

    def demand(self):
        if self.demanding():
            raise self.Error("Current toll is already demanding", self.Error.ALREADY_DEMANDING)
        return self._demand()


class ConcurrentFuturesToll(Toll):
    """
    Toll implementation based on concurrent.futures.Future.

    IMPORTANT: you should call `demand()` as an asynchronous call.
      Many frameworks will use the syntax as `yield toll.demand()`.
      Call to `supply(value)` goes as normal.
    """

    def __init__(self):
        self._CLASS = ConcurrentFutureFeature.import_it()
        self._future = None

    def _demand(self):
        self._future = self._CLASS()
        return self._future

    def _supply(self, value):
        future = self._future
        self._future = None
        future.set_result(value)

    def demanding(self):
        return self._future is not None


class TornadoFuturesToll(Toll):
    """
    Toll implementation based on tornado.concurrent.Future.

    IMPORTANT: you should call `demand()` as an asynchronous call.
      Many frameworks will use the syntax as `yield toll.demand()`.
      Call to `supply(value)` goes as normal.
    """

    def __init__(self):
        self._CLASS = TornadoFutureFeature.import_it()
        self._future = None

    def _demand(self):
        self._future = self._CLASS()
        return self._future

    def _supply(self, value):
        future = self._future
        self._future = None
        future.set_result(value)

    def demanding(self):
        return self._future is not None


class TwistedDeferredToll(Toll):
    """
    Toll implementation based on twisted.internet.defer.Deferred.

    IMPORTANT: you should call `demand()` as an asynchronous call.
      Many frameworks will use the syntax as `yield toll.demand()`.
      Call to `supply(value)` goes as normal.
    """

    def __init__(self):
        self._CLASS = TwistedDeferredFeature.import_it()
        self._deferred = None

    def _demand(self):
        self._deferred = self._CLASS()
        return self._deferred

    def _supply(self, value):
        deferred = self._deferred
        self._deferred = None
        deferred.callback(value)

    def demanding(self):
        return self._deferred is not None


class ThreadEventToll(Toll):
    """
    Toll implementation based on twisted.internet.defer.Deferred.

    IMPORTANT: Do not use this in asynchronous environment. Your
      process will simply die if you demand anything.
    """

    def __init__(self):
        self._event = Event()
        self._value = None

    def _demand(self):
        self._event.clear()
        self._event.wait()
        return self._value

    def _supply(self, value):
        self._value = value
        self._event.set()

    def demanding(self):
        return not self._event.is_set
