from cantrips.types.exception import factory
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

    def _import_error_message(cls):
        """
        Message error for tornado.concurrent.Future not found.
        """
        return "You need to install tornado for this to work (pip install tornado==4.0.2)"


class TwistedDeferredFeature(Feature):
    """
    Feature - twisted.internet.defer.Deferred
    """

    def _import_it(cls):
        """
        Imports Deferred from twisted.internet.defer.
        """
        from twisted.internet.defer import Deferred
        return Deferred

    def _import_error_message(cls):
        """
        Message error for twisted.internet.defer.Deferred not found.
        """
        return "You need to install twisted framework for this to work (pip install twisted==14.0.2)"