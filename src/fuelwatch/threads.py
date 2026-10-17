"""Worker-thread calls that shutdown can wait for.

``asyncio.to_thread`` keeps running its function after the awaiting task is
cancelled. If the app closes the Store right after the scheduler stops, a
write still in a worker could find its connection closed. ``ThreadCalls``
counts calls that have started in a worker, so shutdown can ``drain()``
them (bounded) before the store closes. A call still queued when its task
is cancelled never starts, so it is never waited for.
"""

from __future__ import annotations

import asyncio
import threading


class ThreadCalls:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._running = 0

    @property
    def running(self) -> int:
        with self._cond:
            return self._running

    async def __call__(self, fn, /, *args, **kwargs):
        def call():
            with self._cond:
                self._running += 1
            try:
                return fn(*args, **kwargs)
            finally:
                with self._cond:
                    self._running -= 1
                    self._cond.notify_all()

        return await asyncio.to_thread(call)

    def _wait_idle(self, timeout: float) -> bool:
        with self._cond:
            return self._cond.wait_for(lambda: self._running == 0, timeout=timeout)

    async def drain(self, timeout: float = 10.0) -> bool:
        """Wait until no call is running in a worker; False on timeout."""
        if self.running == 0:
            return True
        return await asyncio.to_thread(self._wait_idle, timeout)


def is_store_closed(e: BaseException) -> bool:
    """The Store raises RuntimeError("store is closed") for a call after
    close(). During shutdown that is expected: stop quietly. (Issue #15 may
    replace this with a StoreClosedError subclass.)"""
    return isinstance(e, RuntimeError) and str(e) == "store is closed"
