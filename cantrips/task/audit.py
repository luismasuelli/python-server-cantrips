#-*- coding: utf-8 -*-
from threading import Thread, Lock, Event
from cantrips.types.exception import factory
from .features import ConcurrentFutureFeature, TornadoFutureFeature, TwistedDeferredFeature


class AuditoryLock(object):
    """
    An auditory lock allows the administrators to set a control checkpoint over a task.
    If a task reaches that control checkpoint and there's at least one "auditor" checking it,
      the task will be put on hold (threads will block, Tornado tasks will return a Future).
      OTOH if no auditor is checking, the tasks continues (threads will not block. Tornado
      tasks will return None -instead of a future- to be yielded).

    By calling `audit_start(auditor_key)` passing any hashable value, a new auditor is
      checking the current lock. By calling `audit_end(auditor_key)`, the auditory ends.
      If you call `audit_start(audit_key)` again, for the same key, you will get an error
      since that key is already auditing. To avoid that, you must pass reentrant=True to
      the AuditoryLock constructor. Additionally, you must call `audit_end(auditor_key)`
      for each call to `audit_start(auditor_key)` since an internal counter tracks the
      current auditories for each key.

    Also, when you call `audit_start(another_auditor_key)` and ANOTHER (distinct) key is
      currently auditing, you will get an error because the lock is busy by another
      auditor. To avoid that, you must pass simultaneous=True to the constructor to allow
      simultaneous auditory of several keys (the checkpoint will be released when the count
      of current auditories becomes 0).

    You can safely specify both reentrant=True and simultaneous=True.
    """

    Error = factory(['ALREADY_AUDITING_SAME', 'ALREADY_AUDITING_OTHER', 'NOT_AUDITING', 'UNSATISFIED_IMPORT_REQ'])

    def __init__(self, reentrant=False, simultaneous=False):
        super(AuditoryLock, self).__init__()
        if not (reentrant in [True, False]):
            raise TypeError("1st/reentrant parameter must be a boolean value")
        if not (simultaneous in [True, False]):
            raise TypeError("2nd/simultaneous parameter must be a boolean value")
        self._audits = {}
        self._reentrant = reentrant
        self._simultaneous = simultaneous
        self._in_critical = False
        self._set()

    def _audit_start(self, auditor):
        """
        See audit_start(auditor)
        """

        if auditor in self._audits:
            #verificar si admite re-entrada
            if not self._reentrant:
                raise self.Error("This lock is not re-entrant, and auditor '{0}' is already auditing".format(auditor),
                                 self.Error.ALREADY_AUDITING_SAME, auditor=auditor)
            self._audits[auditor] += 1
        elif len(self._audits) > 0:
            #verificar si puede agregarse como nuevo auditor
            if not self._simultaneous:
                raise self.Error("This lock is not simultaneous, and another auditor is already auditing",
                                 self.Error.ALREADY_AUDITING_OTHER, auditor=auditor)
            self._audits[auditor] = 1
        self._clear()

    def _clear(self):
        return None

    def _set(self):
        return None

    def _wait(self):
        return None

    def _critical_start(self):
        self._in_critical = True

    def _critical_end(self):
        self._in_critical = False

    def _audit_end(self, auditor):
        """
        See audit_end(auditor)
        """

        if auditor in self._audits:
            #disminuir cantidad y controlar que si es 0 hay que sacarla
            self._audits[auditor] -= 1
            if self._audits[auditor] == 0:
                self._audits.pop(auditor)
            #controlar que si no hay auditorias, se suelte el evento
            if len(self._audits) == 0:
                self._set()
        else:
            raise self.Error("This lock is not holding a current auditory for '{0}'".format(auditor),
                             self.Error.NOT_AUDITING, auditor=auditor)

    def audit_start(self, auditor):
        """
        Starts an auditory for an auditor. On reentrant locks, increments the per-key counter.
        The checkpoints will be locked until the enough and right amount of calls to `audit_end(auditor)`
          are performed (considering reentrant and simultaneous locks).
        """

        try:
            self._critical_start()
            self._audit_start(auditor)
        except:
            raise
        finally:
            if self._in_critical:
                self._critical_end()

    def audit_end(self, auditor):
        """
        Ends the current auditory for an auditor. On reentrant locks, decrements the per-key counter.
        When the counter is zero, the auditor is removed.
        When no auditor is present, the checkpoints will be released.
        """

        try:
            self._critical_start()
            self._audit_end(auditor)
        except:
            raise
        finally:
            if self._in_critical:
                self._critical_end()

    def checkpoint(self):
        """
        Checks for current auditories, causing the task to block if there are.
        """

        return self._wait()


class AuditorySyncLock(AuditoryLock):
    """
    Implements the AudotiryLock using thread-oriented checks.
    Such checks, since intended for threads, are LOCKING and will FREEZE any asynchronous implementation.

    Implementations should put the "auditor" calls in different threads, and the checkpoint calls
      in another set of threads, to distinguish the auditing and audited threads. Several threads
      could safely use the same lock.
    """

    def __init__(self, reentrant=False, simultaneous=False):
        self._lock = Lock()
        self._event = Event()
        super(AuditorySyncLock, self).__init__(reentrant, simultaneous)

    def _clear(self):
        return self._event.clear()

    def _set(self):
        return self._event.set()

    def _wait(self):
        return self._event.wait()

    def _critical_start(self):
        x = self._lock.acquire()
        super(AuditorySyncLock, self)._critical_start()
        return x

    def _critical_end(self):
        super(AuditorySyncLock, self)._critical_end()
        return self._lock.release()


class AuditoryFutureLock(AuditoryLock):
    """
    Implements the AudotiryLock using Future-oriented checks. Using this class requires
      the installation of concurrent.futures (pip install futures==2.2.0).

    Such lock cannot be used in a multithread architecture since the calls are not blocking at all.

    Note that invoking the `checkpoint()` method in async architectures is not like in sync architectures.
    You must yield the value instead of returning it, so Future-oriented architectures can yield (properly-said)
      their processing to other tasks (`yield lock.checkpoint()` in contrast to just `lock.checkpoint()`).
    Additionally, procedures that yield the checkpoint will become generators and must be yielded from an upper
      generator or Tornado's @corroutine.

    Disclaimer: Cannot state whether any other version of `futures` will work or not.
    """

    _FUTURE_CLASS = None
    _FEATURE = ConcurrentFutureFeature

    def __init__(self, reentrant=False, simultaneous=False):
        self._FUTURE_CLASS = self._FEATURE.import_it()
        self._future = None
        super(AuditoryFutureLock, self).__init__(reentrant, simultaneous)

    def _wait(self):
        """
        This value will be returned by calling `checkpoint()`. Calls to `checkpoint()` MUST be yielded using
          this architecture (e.g. in Tornado's @coroutines). The return value will be either a Future (block)
          or None (don't-block).
        """
        return self._future

    def _set(self):
        """
        This call will set the result of the future to None (actually, the result of the future doesn't matter).
        Also, this call will unset the future, by assigning None.

        If the future is not set, this call will do nothing.
        """
        if self._future:
            future = self._future
            self._future = None
            future.set_result(None)
        return None

    def _clear(self):
        """
        This call will create a future if no future is set, thus making the `checkpoint()` calls blocking.
        """
        if not self._future:
            self._future = self._FUTURE_CLASS()
        return None


class AuditoryTornadoLock(AuditoryFutureLock):
    """
    Implements the AudotiryLock using Tornado's Future-oriented checks. Using this class requires
      the installation of tornado (pip install tornado==4.0.2).

    The implementation does not change from the one at AuditoryFutureLock. In fact, there's a high
      chance that you'll never make use one of these two classes since both Future classes are
      interchangeable in Tornado.

    Disclaimer: Cannot state whether this will work, or not, in other versions of tornado.
      However, I suggest limit the usage to versions 4+.
    """

    _FUTURE_CLASS = None
    _FEATURE = TornadoFutureFeature


class AuditoryTwistedLock(AuditoryLock):
    """
    Implements the AudotiryLock using Deferred-oriented checks. Using this class requires
      the installation of twisted framework (pip install twisted==14.0.2).

    Such lock cannot be used in a multithread architecture since the calls are not blocking at all.

    Note that invoking the `checkpoint()` method in async architectures is not like in sync architectures.
    You must yield the value instead of returning it, so Deferred-oriented architectures can yield (properly-said)
      their processing to other tasks (`yield lock.checkpoint()` in contrast to just `lock.checkpoint()`).
    Additionally, procedures that yield the checkpoint will become generators and must be yielded from an upper
      generator or Twisted's @inlineCallbacks.

    Disclaimer: Cannot state whether any other version of `twisted` will work or not.
    """

    _DEFERRED_CLASS = None

    def __init__(self, reentrant=False, simultaneous=False):
        self._DEFERRED_CLASS = TwistedDeferredFeature.import_it()
        self._deferred = None
        super(AuditoryTwistedLock, self).__init__(reentrant, simultaneous)

    def _wait(self):
        """
        This value will be returned by calling `checkpoint()`. Calls to `checkpoint()` MUST be yielded using
          this architecture (e.g. in Twisted's @inlineCallbacks). The return value will be either a Deferred (block)
          or None (don't-block).
        """
        return self._deferred

    def _set(self):
        """
        This call will set the result of the deferred to None (actually, the result of the deferred doesn't matter).
        Also, this call will unset the deferred, by assigning None.

        If the deferred is not set, this call will do nothing.
        """
        if self._deferred:
            deferred = self._deferred
            self._deferred = None
            deferred.callback(None)
        return None

    def _clear(self):
        """
        This call will create a future if no deferred is set, thus making the `checkpoint()` calls blocking.
        """
        if not self._deferred:
            self._deferred = self._DEFERRED_CLASS()
        return None


class AuditableThread(Thread):
    """
    A thread with an embedded AuditorySyncLock. It exposes it as read-only property, and
      also has proxy methods to it.
    """

    def __init__(self, reentrant, simultaneous, *args, **kwargs):
        """
        Crea un hilo y su bloqueo de auditoria
        """
        super(AuditableThread, self).__init__(*args, **kwargs)
        self._audit_lock = AuditorySyncLock(reentrant, simultaneous)

    @property
    def audit_lock(self):
        """
        El bloqueo de auditorias
        """
        return self._audit_lock

    def audit_start(self, auditor):
        """
        ver AuditoryLock::audit_start(auditor)
        """
        self._audit_lock.audit_start(auditor)

    def audit_end(self, auditor):
        """
        ver AuditoryLock::audit_end(auditor)
        """
        self._audit_lock.audit_end(auditor)

    def checkpoint(self):
        """
        ver AuditoryLock::audit_check()
        """
        self._audit_lock.checkpoint()