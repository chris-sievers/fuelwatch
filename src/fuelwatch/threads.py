"""Worker-thread store calls that shutdown waits for.

An ordering guarantee, not crash prevention: ``asyncio.to_thread`` keeps
running a function after its awaiting task is cancelled, so without this a
store write could still be in flight when ``Scheduler.run`` returns.
``ThreadCalls`` counts calls running in a worker, and ``drain()`` waits
(bounded) until they finish, so writes complete before the caller closes
the store. (Since #13, ``Store.close()`` lets in-flight calls finish and
later ones raise "store is closed", which the scheduler handles quietly.)

Limits: a call counts from the moment its worker starts it, so one queued
behind busy workers when the task is cancelled is not waited for (it then
meets a closed store and stops cleanly). ``drain()`` polls from the event
loop rather than using a worker of its own, so its timeout holds even when
every executor worker is busy.
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

    async def drain(self, timeout: float = 10.0, poll: float = 0.01) -> bool:
        """Wait until no call is running in a worker; False on timeout."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while self.running:
            if loop.time() >= deadline:
                return False
            await asyncio.sleep(poll)
        return True


def is_store_closed(e: BaseException) -> bool:
    """The Store raises RuntimeError("store is closed") for a call after
    close(). During shutdown that is expected: stop quietly. (Issue #15 may
    replace this with a StoreClosedError subclass.)"""
    return isinstance(e, RuntimeError) and str(e) == "store is closed"
