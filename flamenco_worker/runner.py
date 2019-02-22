"""Task runner."""

import asyncio
import json
import typing

import attr

from . import attrs_extra
from . import json_encoder, timing, worker


@attr.s
class TaskRunner:
    """Runs tasks, sending updates back to the worker."""

    shutdown_future = attr.ib(validator=attr.validators.instance_of(asyncio.Future))
    subprocess_pid_file = attr.ib(validator=attr.validators.instance_of(str))
    last_command_idx = attr.ib(default=0, init=False)

    _log = attrs_extra.log('%s.TaskRunner' % __name__)

    def __attrs_post_init__(self):
        self.current_command = None
        self._aggr_timing_info = timing.Timing()

    async def execute(self, task: dict, fworker: worker.FlamencoWorker) -> bool:
        """Executes a task, returns True iff the entire task was run succesfully."""

        self._aggr_timing_info = timing.Timing()
        try:
            return await self._execute(task, fworker)
        finally:
            await self.log_recorded_timings(fworker)

    async def _execute(self, task: dict, fworker: worker.FlamencoWorker) -> bool:
        from .commands import command_handlers

        task_id = task['_id']

        for cmd_idx, cmd_info in enumerate(task['commands']):
            self.last_command_idx = cmd_idx

            # Figure out the command name
            cmd_name = cmd_info.get('name')
            if not cmd_name:
                raise ValueError('Command %i of task %s has no name' % (cmd_idx, task_id))

            cmd_settings = cmd_info.get('settings')
            if cmd_settings is None or not isinstance(cmd_settings, dict):
                raise ValueError('Command %i of task %s has malformed settings %r' %
                                 (cmd_idx, task_id, cmd_settings))

            # Find the handler class
            try:
                cmd_cls = command_handlers[cmd_name]
            except KeyError:
                raise ValueError('Command %i of task %s has unknown command name %r' %
                                 (cmd_idx, task_id, cmd_name))

            # Construct & execute the handler.
            cmd = cmd_cls(
                worker=fworker,
                task_id=task_id,
                command_idx=cmd_idx,
            )
            self.current_command = cmd
            success = await cmd.run(cmd_settings)

            # Add the timings of this command to the aggregated timing info.
            self._aggr_timing_info += cmd.timing

            if not success:
                self._log.warning('Command %i of task %s was not succesful, aborting task.',
                                  cmd_idx, task_id)
                return False

        self._log.info('Task %s completed succesfully.', task_id)
        self.current_command = None

        return True

    async def abort_current_task(self):
        """Aborts the current task by aborting the currently running command.

        Asynchronous, because a subprocess has to be wait()ed upon before returning.
        """

        if self.current_command is None:
            self._log.info('abort_current_task: no command running, nothing to abort.')
            return

        self._log.warning('abort_current_task: Aborting command %s', self.current_command)
        await self.current_command.abort()

    async def log_recorded_timings(self, fworker: worker.FlamencoWorker) -> None:
        """Send the timing to the task log."""

        timing_info = self._aggr_timing_info
        if not timing_info:
            return

        as_json = json.dumps(timing_info, cls=json_encoder.JSONEncoder)
        log_line = f'Aggregated task timing info: {as_json}'
        self._log.info(log_line)
        await fworker.register_log(log_line)

    @property
    def aggr_timing_info(self) -> timing.Timing:
        return self._aggr_timing_info
