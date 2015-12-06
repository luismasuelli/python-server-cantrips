from cantrips.features import Feature


class ConcurrentFutureFeature(Feature):
    """
    Feature - concurrent.futures.Future
    """

    @classmethod
    def _import_it(cls):
        """
        Imports Future from concurrent.futures.
        """
        from concurrent.futures import Future
        return Future

    @classmethod
    def _import_error_message(cls):
        """
        Message error for concurrent.futures.Future not found.
        """
        return "You need to install concurrent.futures for this to work (pip install futures==2.2.0)"


class TornadoFutureFeature(Feature):
    """
    Feature - tornado.concurrent.Future
    """

    @classmethod
    def _import_it(cls):
        """
        Imports Future from tornado.concurrent.
        """
        from tornado.concurrent import Future
        return Future

    @classmethod
    def _import_error_message(cls):
        """
        Message error for tornado.concurrent.Future not found.
        """
        return "You need to install tornado for this to work (pip install tornado==4.0.2)"


class TwistedDeferredFeature(Feature):
    """
    Feature - twisted.internet.defer.Deferred
    """

    @classmethod
    def _import_it(cls):
        """
        Imports Deferred from twisted.internet.defer.
        """
        from twisted.internet.defer import Deferred
        return Deferred

    @classmethod
    def _import_error_message(cls):
        """
        Message error for twisted.internet.defer.Deferred not found.
        """
        return "You need to install twisted framework for this to work (pip install twisted==14.0.2)"


class ThreadedEventFeature(Feature):
    """
    Feature - threading.Event
    """

    @classmethod
    def _import_it(cls):
        """
        Imports twisted.Event
        """
        from threading import Event
        return Event

    @classmethod
    def _import_error_message(cls):
        """
        This message error should never be seen since threads are standard stuff.
        """
        return "Your standard library is corrupted. Module `threading` cannot be imported. Please reinstall" \
               " your python distribution ASAP"


class TornadoTimerFeature(Feature):
    """
    Feature - Timeouts for Tornado.
    """

    @classmethod
    def _import_it(cls):
        """
        Imports stuff based on call_later and remove_callback.
        Returns a pair of functions
        """
        from tornado.ioloop import IOLoop

        def create_timeout(ioloop, seconds, callback):
            return ioloop.call_later(seconds, callback)

        def delete_timeout(ioloop, timeout):
            try:
                ioloop.remove_timeout(timeout)
            except Exception as e:
                pass

        return create_timeout, delete_timeout

    @classmethod
    def _import_error_message(cls):
        """
        Message error for tornado.concurrent.IOLoop not found.
        """
        return "You need to install tornado for this to work (pip install tornado==4.0.2)"


class TwistedTimerFeature(Feature):
    """
    Feature - Timeout for Twisted.
    """

    @classmethod
    def _import_it(cls):
        """
        Imports stuff related to callLater and cancel.
        Returns a pair of functions
        """
        from twisted.internet import reactor

        def create_timeout(reactor, seconds, callback):
            return reactor.callLater(seconds, callback)

        def delete_timeout(timeout):
            timeout.cancel()

        return create_timeout, delete_timeout

    @classmethod
    def _import_error_message(cls):
        """
        Message error for twisted.internet.reactor not found.
        """
        return "You need to install twisted framework for this to work (pip install twisted==14.0.2)"


class ThreadedTimerFeature(Feature):
    """
    Feature - Timeout for thread-based models.
    """

    @classmethod
    def _import_it(cls):
        """
        Imports stuff related to threads.
        Returns a pair of functions
        """
        from threading import Timer

        def create_timeout(seconds, callback):
            return Timer(seconds, callback)

        def delete_timeout(timeout):
            timeout.cancel()

        return create_timeout, delete_timeout

    @classmethod
    def _import_error_message(cls):
        """
        This message error should never be seen since threads are standard stuff.
        """
        return "Your standard library is corrupted. Module `threading` cannot be imported. Please reinstall" \
               " your python distribution ASAP"
