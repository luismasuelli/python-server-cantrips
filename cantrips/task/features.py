from cantrips.types.exception import factory


class Feature(object):
    """
    Tries to import a specific feature.
    """
    _CLASS = None
    Error = factory(['UNSATISFIED_IMPORT_REQ'])

    @classmethod
    def import_it(cls):
        """
        Performs the import only once.
        """
        if not cls._CLASS:
            try:
                cls._CLASS = cls._import_it()
            except ImportError:
                raise cls.Error(cls._import_error_message(), cls.Error.UNSATISFIED_IMPORT_REQ)
        return cls._CLASS

    @classmethod
    def _import_it(cls):
        """
        Internal method - performs the import and returns the imported object.
        """
        return None

    @classmethod
    def _import_error_message(cls):
        """
        Internal method - displays the exception message
        """
        return None


class ConcurrentFutureFeature(Feature):
    """
    Feature - concurrent.futures.Future
    """
    _CLASS = None

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
    _CLASS = None

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
    _CLASS = None

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