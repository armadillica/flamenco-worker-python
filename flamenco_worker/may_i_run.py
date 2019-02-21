"""Polls the /may-i-run/{task-id} endpoint on the Manager."""

import asyncio
import datetime
import typing

import attr

from . import attrs_extra
from . import documents
from . import worker
from . import upstream


@attr.s
class MayIRun:
    manager = attr.ib(validator=attr.validators.instance_of(upstream.FlamencoManager),
                      repr=False)
    worker = attr.ib(validator=attr.validators.instance_of(worker.FlamencoWorker),
                     repr=False)
    poll_interval = attr.ib(validator=attr.validators.instance_of(datetime.timedelta))
    loop = attr.ib(validator=attr.validators.instance_of(asyncio.AbstractEventLoop))

    _log = attrs_extra.log('%s.MayIRun' % __name__)

    async def work(self):
        try:
            while True:
                await self.one_iteration()
                await asyncio.sleep(self.poll_interval.total_seconds())
        except asyncio.CancelledError:
            self._log.warning('Shutting down.')
        except Exception:
            self._log.exception('May-I-Run service crashed!')
            raise

    async def one_iteration(self):
        task_id = self.worker.active_task_id

        if not task_id:
            # self._log.debug('No current task')
            return

        allowed = await self.may_i_run(task_id)
        if allowed is None:
            # Something has been logged already.
            return

        if allowed:
            self._log.debug('Current task %s may run', task_id)
            return

        self._log.warning('We have to stop task %s', task_id)
        await self.worker.stop_current_task(task_id)

    async def may_i_run(self, task_id: str) -> typing.Optional[bool]:
        """Asks the Manager whether we are still allowed to run the given task.

        Returns None if the Manager cannot be reached and thus no answer can be obtained.
        """

        try:
            resp = await self.manager.get('/may-i-run/%s' % task_id, loop=self.loop)
        except Exception as ex:
            self._log.warning('Unable to query may-i-run endpoint: %s', ex)
            return None

        may_keep_running = documents.MayKeepRunningResponse(**resp.json())

        if not may_keep_running.may_keep_running:
            self._log.warning('Not allowed to keep running task %s: %s',
                              task_id, may_keep_running.reason)
            if may_keep_running.status_requested:
                self.worker.change_status(may_keep_running.status_requested)

        return may_keep_running.may_keep_running
