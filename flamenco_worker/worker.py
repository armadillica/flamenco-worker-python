import asyncio
import datetime
import enum
import functools
import itertools
import pathlib
import tempfile
import time
import traceback
import typing

import attr
import requests.exceptions

from . import attrs_extra
from . import documents
from . import jwtauth
from . import upstream
from . import upstream_update_queue

# All durations/delays/etc are in seconds.
REGISTER_AT_MANAGER_FAILED_RETRY_DELAY = 30
FETCH_TASK_FAILED_RETRY_DELAY = 10  # when we failed obtaining a task
FETCH_TASK_EMPTY_RETRY_DELAY = 5  # when there are no tasks to perform
FETCH_TASK_DONE_SCHEDULE_NEW_DELAY = 3  # after a task is completed
ERROR_RETRY_DELAY = 600  # after the pre-task sanity check failed
UNCAUGHT_EXCEPTION_RETRY_DELAY = 60  # after single_iteration errored out

PUSH_LOG_MAX_ENTRIES = 1000
PUSH_LOG_MAX_INTERVAL = datetime.timedelta(seconds=30)
PUSH_ACT_MAX_INTERVAL = datetime.timedelta(seconds=15)

ASLEEP_POLL_STATUS_CHANGE_REQUESTED_DELAY = 30

# If there are more than this number of queued task updates, we won't ask
# the Manager for another task to execute. Task execution is delayed until
# the queue size is below this threshold.
QUEUE_SIZE_THRESHOLD = 10


class UnableToRegisterError(Exception):
    """Raised when the worker can't register at the manager.

    Will cause an immediate shutdown.
    """


class WorkerState(enum.Enum):
    STARTING = 'starting'
    AWAKE = 'awake'
    ASLEEP = 'asleep'
    ERROR = 'error'
    SHUTTING_DOWN = 'shutting-down'


@attr.s(auto_attribs=True)
class PreTaskCheckParams:
    pre_task_check_write: typing.Iterable[str] = []
    pre_task_check_read: typing.Iterable[str] = []


class PreTaskCheckFailed(PermissionError):
    """Raised when the pre-task sanity check fails."""


@attr.s
class FlamencoWorker:
    manager = attr.ib(validator=attr.validators.instance_of(upstream.FlamencoManager))
    trunner = attr.ib()  # Instance of flamenco_worker.runner.TaskRunner
    tuqueue = attr.ib(validator=attr.validators.instance_of(upstream_update_queue.TaskUpdateQueue))
    task_types = attr.ib(validator=attr.validators.instance_of(list))
    worker_id = attr.ib(validator=attr.validators.instance_of(str))
    worker_secret = attr.ib(validator=attr.validators.instance_of(str))

    loop = attr.ib(validator=attr.validators.instance_of(asyncio.AbstractEventLoop))
    shutdown_future = attr.ib(
        validator=attr.validators.optional(attr.validators.instance_of(asyncio.Future)))

    state = attr.ib(default=WorkerState.STARTING,
                    validator=attr.validators.instance_of(WorkerState))

    worker_registration_secret = attr.ib(validator=attr.validators.instance_of(str),
                                         default='')

    # Indicates the state in which the Worker should start
    initial_state = attr.ib(validator=attr.validators.instance_of(str), default='awake')
    run_single_task = attr.ib(validator=attr.validators.instance_of(bool), default=False)

    # When Manager tells us we may no longer run our current task, this is set to True.
    # As a result, the cancelled state isn't pushed to Manager any more. It is reset
    # to False when a new task is started.
    task_is_silently_aborting = attr.ib(default=False, init=False,
                                        validator=attr.validators.instance_of(bool))

    single_iteration_fut = attr.ib(
        default=None, init=False,
        validator=attr.validators.optional(attr.validators.instance_of(asyncio.Future)))
    asyncio_execution_fut = attr.ib(
        default=None, init=False,
        validator=attr.validators.optional(attr.validators.instance_of(asyncio.Future)))

    # See self.sleeping()
    sleeping_fut = attr.ib(
        default=None, init=False,
        validator=attr.validators.optional(attr.validators.instance_of(asyncio.Future)))

    task_id = attr.ib(
        default=None, init=False,
        validator=attr.validators.optional(attr.validators.instance_of(str))
    )
    current_task_status = attr.ib(
        default=None, init=False,
        validator=attr.validators.optional(attr.validators.instance_of(str))
    )
    _queued_log_entries = attr.ib(default=attr.Factory(list), init=False)  # type: typing.List[str]
    _queue_lock = attr.ib(default=attr.Factory(asyncio.Lock), init=False)

    # MyPy stumbles over the 'validator' argument here:
    last_log_push = attr.ib(  # type: ignore
        default=attr.Factory(datetime.datetime.now),
        validator=attr.validators.optional(attr.validators.instance_of(datetime.datetime)))
    last_activity_push = attr.ib(  # type: ignore
        default=attr.Factory(datetime.datetime.now),
        validator=attr.validators.optional(attr.validators.instance_of(datetime.datetime)))

    # Kept in sync with the task updates we send to upstream Manager, so that we can send
    # a complete Activity each time.
    last_task_activity = attr.ib(default=attr.Factory(documents.Activity))

    # Configuration
    push_log_max_interval = attr.ib(default=PUSH_LOG_MAX_INTERVAL,
                                    validator=attr.validators.instance_of(datetime.timedelta))
    push_log_max_entries = attr.ib(default=PUSH_LOG_MAX_ENTRIES,
                                   validator=attr.validators.instance_of(int))
    push_act_max_interval = attr.ib(default=PUSH_ACT_MAX_INTERVAL,
                                    validator=attr.validators.instance_of(datetime.timedelta))

    pretask_check_params = attr.ib(factory=PreTaskCheckParams,
                                   validator=attr.validators.instance_of(PreTaskCheckParams))

    # Futures that represent delayed calls to push_to_manager().
    # They are scheduled when logs & activities are registered but not yet pushed. They are
    # cancelled when a push_to_manager() actually happens for another reason. There are different
    # futures for activity and log pushing, as these can have different max intervals.
    _push_log_to_manager = attr.ib(
        default=None, init=False,
        validator=attr.validators.optional(attr.validators.instance_of(asyncio.Future)))
    _push_act_to_manager = attr.ib(
        default=None, init=False,
        validator=attr.validators.optional(attr.validators.instance_of(asyncio.Future)))

    # When the worker is shutting down, the currently running task will be
    # handed back to the manager for re-scheduling. In such a situation,
    # an abort is expected and acceptable.
    failures_are_acceptable = attr.ib(default=False, init=False,
                                      validator=attr.validators.instance_of(bool))

    _log = attrs_extra.log('%s.FlamencoWorker' % __name__)

    _last_output_produced = 0.0  # seconds since epoch

    @property
    def active_task_id(self) -> typing.Optional[str]:
        """Returns the task ID, but only if it is currently executing; returns None otherwise."""

        if self.asyncio_execution_fut is None or self.asyncio_execution_fut.done():
            return None
        return self.task_id

    async def startup(self, *, may_retry_loop=True):
        self._log.info('Starting up')

        do_register = not self.worker_id or not self.worker_secret
        if do_register:
            await self.register_at_manager(may_retry_loop=may_retry_loop)

        # Once we know our ID and secret, update the manager object so that we
        # don't have to pass our authentication info each and every call.
        self.manager.auth = (self.worker_id, self.worker_secret)

        # We only need to sign on if we didn't just register. However, this
        # can only happen after setting self.manager.auth.
        if not do_register:
            await self.signon(may_retry_loop=may_retry_loop)

        # If we're not supposed to start in 'awake' state, let the Manager know.
        if self.initial_state != 'awake':
            self._log.info('Telling Manager we are in state %r', self.initial_state)
            self.ack_status_change(self.initial_state)

        self.schedule_fetch_task()

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def hostname() -> str:
        import socket
        return socket.gethostname()

    @property
    def nickname(self) -> str:
        return self.hostname()

    @property
    def identifier(self) -> str:
        return f'{self.worker_id} ({self.nickname})'

    async def _keep_posting_to_manager(self, url: str, json: dict, *,
                                       use_auth=True,
                                       may_retry_loop: bool,
                                       extra_headers: typing.Optional[dict] = None,
                                       ) -> requests.Response:
        post_kwargs = {
            'json': json,
            'loop': self.loop,
        }
        if not use_auth:
            post_kwargs['auth'] = None
        if extra_headers:
            post_kwargs['headers'] = extra_headers

        while True:
            try:
                resp = await self.manager.post(url, **post_kwargs)
                resp.raise_for_status()
            except requests.RequestException as ex:
                # Somehow 'ex.response is not None' is really necessary; just 'ex.response'
                # is not working as expected.
                has_response = ex.response is not None
                is_unauthorized = has_response and ex.response.status_code == 401
                if not may_retry_loop or is_unauthorized:
                    self._log.debug('Unable to POST to manager %s: %s. %s.', url, ex)
                    raise

                if has_response:
                    body = f'; {ex.response.text.strip()}'
                else:
                    body = ''

                if has_response and ex.response.status_code == 403:
                    # This needs to stop the Worker completely, as the configuration
                    # is wrong. By letting systemd restart the worker a new config file
                    # is read, and maybe that now contains the correct secret.
                    self._log.error('Unable to POST to manager %s because we do not have '
                                    'the correct worker_registration_secret%s', url, body)
                    raise
                self._log.warning('Unable to POST to manager %s, retrying in %i seconds: %s%s',
                                  url, REGISTER_AT_MANAGER_FAILED_RETRY_DELAY, ex, body)
                await asyncio.sleep(REGISTER_AT_MANAGER_FAILED_RETRY_DELAY)
            else:
                return resp

    async def signon(self, *, may_retry_loop: bool,
                     autoregister_already_tried: bool = False):
        """Signs on at the manager.

        Only needed when we didn't just register.
        """

        self._log.info('Signing on at manager.')
        try:
            await self._keep_posting_to_manager(
                '/sign-on',
                json={
                    'supported_task_types': self.task_types,
                    'nickname': self.hostname(),
                },
                may_retry_loop=may_retry_loop,
            )
        except requests.exceptions.HTTPError as ex:
            if ex.response.status_code != 401:
                self._log.error('Unable to sign on at Manager: %s', ex)
                raise UnableToRegisterError()

            if autoregister_already_tried:
                self._log.error('Manager did not accept our credentials, and re-registration '
                                'was already attempted. Giving up.')
                raise UnableToRegisterError()

            self._log.warning('Manager did not accept our credentials, going to re-register')
            await self.register_at_manager(may_retry_loop=may_retry_loop)

            self._log.warning('Re-registration was fine, going to re-try sign-on')
            await self.signon(may_retry_loop=may_retry_loop, autoregister_already_tried=True)
        else:
            # Expected flow: no exception, manager accepts credentials.
            self._log.info('Manager accepted sign-on.')

    async def register_at_manager(self, *, may_retry_loop: bool):
        self._log.info('Registering at manager')

        self.worker_secret = generate_secret()
        platform = detect_platform()

        if self.worker_registration_secret:
            reg_token = jwtauth.new_registration_token(self.worker_registration_secret)
            headers = {
                'Authorization': f'Bearer {reg_token}'
            }
        else:
            headers = {}

        try:
            resp = await self._keep_posting_to_manager(
                '/register-worker',
                json={
                    'secret': self.worker_secret,
                    'platform': platform,
                    'supported_task_types': self.task_types,
                    'nickname': self.hostname(),
                },
                use_auth=False,  # cannot use authentication because we haven't registered yet
                may_retry_loop=may_retry_loop,
                extra_headers=headers,
            )
        except requests.exceptions.HTTPError:
            raise UnableToRegisterError()

        result = resp.json()
        self._log.info('Response: %s', result)
        self.worker_id = result['_id']
        self.manager.auth = (self.worker_id, self.worker_secret)

        self.write_registration_info()

    def write_registration_info(self):
        """Writes the current worker ID and secret to the home dir."""

        from . import config

        config.merge_with_home_config({
            'worker_id': self.worker_id,
            'worker_secret': self.worker_secret,
        })

    def mainloop(self):
        self._log.info('Entering main loop')

        # TODO: add "watchdog" task that checks the asyncio loop and ensures there is
        # always either a task being executed or a task fetch scheduled.
        self.loop.run_forever()

    def schedule_fetch_task(self, delay=0):
        """Schedules a task fetch.

        If a task fetch was already queued, that one is cancelled.

        :param delay: delay in seconds, after which the task fetch will be performed.
        """

        # The current task may still be running, as single_iteration() calls schedule_fetch_task() to
        # schedule a future run. This may result in the task not being awaited when we are
        # shutting down.
        if self.shutdown_future is not None and self.shutdown_future.done():
            self._log.warning('Shutting down, not scheduling another fetch-task task.')
            return

        self.single_iteration_fut = asyncio.ensure_future(self.single_iteration(delay),
                                                          loop=self.loop)
        self.single_iteration_fut.add_done_callback(self._single_iteration_done)

    def _single_iteration_done(self, future):
        """Called when self.single_iteration_fut is done."""

        try:
            ex = future.exception()
        except asyncio.CancelledError:
            self._log.debug('single iteration future was cancelled')
            return

        if ex is None:
            return

        if isinstance(ex, asyncio.CancelledError):
            return

        self._log.error('Unhandled %s running single iteration: %s', type(ex).__name__, ex)
        self._log.error('Bluntly going to reschedule another iteration in %d seconds',
                        UNCAUGHT_EXCEPTION_RETRY_DELAY)
        self.schedule_fetch_task(UNCAUGHT_EXCEPTION_RETRY_DELAY)

    async def stop_current_task(self, task_id: str):
        """Stops the current task by canceling the AsyncIO task.

        This causes a CancelledError in the self.single_iteration() function, which then takes care
        of the task status change and subsequent activity push.

        :param task_id: the task ID to stop. Will only perform a stop if it
            matches the currently executing task. This is to avoid race
            conditions.
        """

        if not self.asyncio_execution_fut or self.asyncio_execution_fut.done():
            self._log.warning('stop_current_task() called but no task is running.')
            return

        if self.task_id != task_id:
            self._log.warning('stop_current_task(%r) called, but current task is %r, not stopping',
                              task_id, self.task_id)
            return

        self._log.warning('Stopping task %s', self.task_id)
        self.task_is_silently_aborting = True

        try:
            await self.trunner.abort_current_task()
        except asyncio.CancelledError:
            self._log.info('asyncio task was canceled for task runner task %s', self.task_id)
        self.asyncio_execution_fut.cancel()

        await self.register_log('Worker %s stopped running this task,'
                                ' no longer allowed to run by Manager', self.identifier)
        await self.requeue_task_on_manager(task_id)

    async def requeue_task_on_manager(self, task_id: str):
        """Return a task to the Manager's queue for later execution."""

        self._log.info('Returning task %s to the Manager queue', task_id)

        await self.push_to_manager()
        await self.tuqueue.flush_and_report(loop=self.loop)

        url = f'/tasks/{task_id}/return'
        try:
            resp = await self.manager.post(url, loop=self.loop)
        except IOError as ex:
            self._log.exception('Exception POSTing to %s', url)
            return

        if resp.status_code != 204:
            self._log.warning('Error %d returning task %s to Manager: %s',
                              resp.status_code, resp.json())
            await self.register_log('Worker %s could not return this task to the Manager queue',
                                    self.identifier)
            return

        await self.register_log('Worker %s returned this task to the Manager queue.',
                                self.identifier)

    def shutdown(self):
        """Gracefully shuts down any asynchronous tasks."""

        self._log.warning('Shutting down')
        self.state = WorkerState.SHUTTING_DOWN
        self.failures_are_acceptable = True

        self.stop_fetching_tasks()
        self.stop_sleeping()

        # Stop the task runner
        self.loop.run_until_complete(self.trunner.abort_current_task())

        # Queue anything that should still be pushed to the Manager
        push_act_sched = self._push_act_to_manager is not None \
                         and not self._push_act_to_manager.done()
        push_log_sched = self._push_log_to_manager is not None \
                         and not self._push_log_to_manager.done()
        if push_act_sched or push_log_sched:
            # Try to push queued task updates to manager before shutting down
            self._log.info('shutdown(): pushing queued updates to manager')
            self.loop.run_until_complete(self.push_to_manager())

        # Try to do a final push of queued updates to the Manager.
        self.loop.run_until_complete(self.tuqueue.flush_and_report(loop=self.loop))

        # Let the Manager know we're shutting down
        self._log.info('shutdown(): signing off at Manager')
        try:
            self.loop.run_until_complete(self.manager.post('/sign-off', loop=self.loop))
        except Exception as ex:
            self._log.warning('Error signing off. Continuing with shutdown. %s', ex)

        # TODO(Sybren): do this in a finally-clause:
        self.failures_are_acceptable = False

    def stop_fetching_tasks(self):
        """Stops the delayed task-fetching from running.

        Used in shutdown and when we're going to status 'asleep'.
        """

        if self.single_iteration_fut is None or self.single_iteration_fut.done():
            return

        self._log.info('stopping task fetching task %s', self.single_iteration_fut)
        self.single_iteration_fut.cancel()

        # This prevents a 'Task was destroyed but it is pending!' warning on the console.
        # Sybren: I've only seen this in unit tests, so maybe this code should be moved
        # there, instead.
        try:
            if not self.loop.is_running():
                self.loop.run_until_complete(self.single_iteration_fut)
        except asyncio.CancelledError:
            pass

    async def single_iteration(self, delay: float):
        """Fetches a single task to perform from Flamenco Manager, and executes it.

        :param delay: waits this many seconds before fetching a task.
        """

        self.state = WorkerState.AWAKE
        self._cleanup_state_for_new_task()

        # self._log.debug('Going to fetch task in %s seconds', delay)
        await asyncio.sleep(delay)

        # Prevent outgoing queue overflowing by waiting until it's below the
        # threshold before starting another task.
        # TODO(sybren): introduce another worker state for this, and handle there.
        async with self._queue_lock:
            queue_size = self.tuqueue.queue_size()
        if queue_size > QUEUE_SIZE_THRESHOLD:
            self._log.info('Task Update Queue size too large (%d > %d), waiting until it shrinks.',
                           queue_size, QUEUE_SIZE_THRESHOLD)
            self.schedule_fetch_task(FETCH_TASK_FAILED_RETRY_DELAY)
            return

        try:
            self.pre_task_sanity_check()
        except PreTaskCheckFailed as ex:
            self._log.exception('Pre-task sanity check failed: %s, waiting until it succeeds', ex)
            self.go_to_state_error()
            return

        task_info = await self.fetch_task()
        if task_info is None:
            return

        await self.execute_task(task_info)

    async def fetch_task(self) -> typing.Optional[dict]:
        # TODO: use exponential backoff instead of retrying every fixed N seconds.
        log = self._log.getChild('fetch_task')
        log.debug('Fetching task')
        try:
            resp = await self.manager.post('/task', loop=self.loop)
        except requests.exceptions.RequestException as ex:
            log.warning('Error fetching new task, will retry in %i seconds: %s',
                        FETCH_TASK_FAILED_RETRY_DELAY, ex)
            self.schedule_fetch_task(FETCH_TASK_FAILED_RETRY_DELAY)
            return None

        if resp.status_code == 204:
            log.debug('No tasks available, will retry in %i seconds.',
                      FETCH_TASK_EMPTY_RETRY_DELAY)
            self.schedule_fetch_task(FETCH_TASK_EMPTY_RETRY_DELAY)
            return None

        if resp.status_code == 423:
            status_change = documents.StatusChangeRequest(**resp.json())
            log.info('status change to %r requested when fetching new task',
                     status_change.status_requested)
            self.change_status(status_change.status_requested)
            return None

        if resp.status_code != 200:
            log.warning('Error %i fetching new task, will retry in %i seconds.',
                        resp.status_code, FETCH_TASK_FAILED_RETRY_DELAY)
            self.schedule_fetch_task(FETCH_TASK_FAILED_RETRY_DELAY)
            return None

        task_info = resp.json()
        self.task_id = task_info['_id']
        log.info('Received task: %s', self.task_id)
        log.debug('Received task: %s', task_info)
        return task_info

    async def execute_task(self, task_info: dict) -> None:
        """Feed a task to the task runner and monitor for exceptions."""
        try:
            await self.register_task_update(task_status='active')
            self.asyncio_execution_fut = asyncio.ensure_future(
                self.trunner.execute(task_info, self),
                loop=self.loop)
            ok = await self.asyncio_execution_fut
            if ok:
                await self.register_task_update(
                    task_status='completed',
                    activity='Task completed',
                )
            elif self.failures_are_acceptable:
                self._log.warning('Task %s failed, but ignoring it since we are shutting down.',
                                  self.task_id)
            else:
                self._log.error('Task %s failed', self.task_id)
                await self.register_task_update(task_status='failed')
        except asyncio.CancelledError:
            if self.failures_are_acceptable:
                self._log.warning('Task %s was cancelled, but ignoring it since '
                                  'we are shutting down.', self.task_id)
            elif self.task_is_silently_aborting:
                self._log.warning('Task %s was cancelled, but ignoring it since '
                                  'we are no longer allowed to run it.', self.task_id)
            else:
                self._log.warning('Task %s was cancelled', self.task_id)
                await self.register_task_update(task_status='canceled',
                                                activity='Task was canceled')
        except Exception as ex:
            self._log.exception('Uncaught exception executing task %s' % self.task_id)
            try:
                # Such a failure will always result in a failed task, even when
                # self.failures_are_acceptable = True; only expected failures are
                # acceptable then.
                async with self._queue_lock:
                    self._queued_log_entries.append(traceback.format_exc())
                await self.register_task_update(
                    task_status='failed',
                    activity='Uncaught exception: %s %s' % (type(ex).__name__, ex),
                )
            except Exception:
                self._log.exception('While notifying manager of failure, another error happened.')
        finally:
            if self.run_single_task:
                self._log.info('Running in single-task mode, exiting.')
                self.go_to_state_shutdown()
                return

            if self.state == WorkerState.AWAKE:
                # Schedule a new task run unless shutting down or sleeping; after a little delay to
                # not hammer the world when we're in some infinite failure loop.
                self.schedule_fetch_task(FETCH_TASK_DONE_SCHEDULE_NEW_DELAY)

    def _cleanup_state_for_new_task(self):
        """Cleans up internal state to prepare for a new task to be executed."""

        self.last_task_activity = documents.Activity()
        self.task_is_silently_aborting = False
        self.current_task_status = ''

    async def push_to_manager(self, *, delay: datetime.timedelta = None):
        """Updates a task's status and activity.

        Uses the TaskUpdateQueue to handle persistent queueing.
        """

        if delay is not None:
            delay_sec = delay.total_seconds()
            self._log.debug('Scheduled delayed push to manager in %r seconds', delay_sec)
            await asyncio.sleep(delay_sec)

            assert self.shutdown_future is not None
            if self.shutdown_future.done():
                self._log.info('Shutting down, not pushing changes to manager.')

        self._log.info('Updating task %s with status %r and activity %r',
                       self.task_id, self.current_task_status, self.last_task_activity)

        payload: typing.MutableMapping[str, typing.Any] = {}
        if self.task_is_silently_aborting:
            self._log.info('push_to_manager: task is silently aborting, will only push logs')
        else:
            payload = attr.asdict(self.last_task_activity,
                                  # Prevent sending an empty metrics dict:
                                  filter=lambda attr, value: attr.name != 'metrics' or value)
            if self.current_task_status:
                payload['task_status'] = self.current_task_status

        now = datetime.datetime.now()
        self.last_activity_push = now

        # Cancel any pending push task, as we're pushing activities now.
        if self._push_act_to_manager is not None:
            self._push_act_to_manager.cancel()

        async with self._queue_lock:
            if self._queued_log_entries:
                payload['log'] = '\n'.join(self._queued_log_entries)
                self._queued_log_entries.clear()
                self.last_log_push = now

                # Cancel any pending push task, as we're pushing logs now.
                if self._push_log_to_manager is not None:
                    self._push_log_to_manager.cancel()

        if not payload:
            self._log.debug('push_to_manager: nothing to push')
            return

        self.tuqueue.queue('/tasks/%s/update' % self.task_id, payload)

    async def register_task_update(self, *,
                                   task_status: str = None,
                                   **kwargs):
        """Stores the task status and activity, and possibly sends to Flamenco Manager.

        If the last update to Manager was long enough ago, or the task status changed,
        the info is sent to Manager. This way we can update command progress percentage
        hundreds of times per second, without worrying about network overhead.
        """

        self._log.debug('Task update: task_status=%s, %s', task_status, kwargs)

        # Update the current activity
        for key, value in kwargs.items():
            setattr(self.last_task_activity, key, value)

        # If we have timing information about the current task, include that too.
        timing_metrics = self.trunner.aggr_timing_info
        if timing_metrics:
            self.last_task_activity.metrics['timing'] = timing_metrics.to_json_compat()
        else:
            self.last_task_activity.metrics.pop('timing', None)

        if task_status is None:
            task_status_changed = False
        else:
            task_status_changed = self.current_task_status != task_status
            self.current_task_status = task_status

        if task_status_changed:
            self._log.info('Task changed status to %s, pushing to manager', task_status)
            await self.push_to_manager()
        elif datetime.datetime.now() - self.last_activity_push > self.push_act_max_interval:
            self._log.info('More than %s since last activity update, pushing to manager',
                           self.push_act_max_interval)
            await self.push_to_manager()
        elif self._push_act_to_manager is None or self._push_act_to_manager.done():
            # Schedule a future push to manager.
            self._push_act_to_manager = asyncio.ensure_future(
                self.push_to_manager(delay=self.push_act_max_interval),
                loop=self.loop)

    async def register_log(self, log_entry: str, *fmt_args):
        """Registers a log entry, and possibly sends all queued log entries to upstream Manager.

        Supports variable arguments, just like the logger.{info,warn,error}(...) family
        of methods.
        """

        from . import tz

        if fmt_args:
            log_entry %= fmt_args

        now = datetime.datetime.now(tz.tzutc()).isoformat()
        async with self._queue_lock:
            self._queued_log_entries.append('%s: %s' % (now, log_entry))
            queue_size = len(self._queued_log_entries)

        if queue_size > self.push_log_max_entries:
            self._log.info('Queued up %i > %i log entries, pushing to manager',
                           queue_size, self.push_log_max_entries)
            await self.push_to_manager()
        elif datetime.datetime.now() - self.last_log_push > self.push_log_max_interval:
            self._log.info('More than %s since last log update, pushing to manager',
                           self.push_log_max_interval)
            await self.push_to_manager()
        elif self._push_log_to_manager is None or self._push_log_to_manager.done():
            # Schedule a future push to manager.
            self._push_log_to_manager = asyncio.ensure_future(
                self.push_to_manager(delay=self.push_log_max_interval),
                loop=self.loop)

    def output_produced(self, *paths: typing.Union[str, pathlib.PurePath]):
        """Registers a produced output (e.g. rendered frame) with the manager.

        This performs a HTTP POST in a background task, returning as soon as
        the task is scheduled.

        Only sends an update every X seconds, to avoid sending too many
        requests when we output frames rapidly.
        """

        now = time.time()
        if now - self._last_output_produced < 30:
            self._log.debug('Throttling POST to Manager /output-produced endpoint')
            return
        self._last_output_produced = now

        async def do_post():
            try:
                self._log.info('Sending %i path(s) to Manager', len(paths))
                resp = await self.manager.post('/output-produced',
                                               json={'paths': [str(p) for p in paths]},
                                               loop=self.loop)
                if resp.status_code == 204:
                    self._log.info('Manager accepted our output notification for %s', paths)
                else:
                    self._log.warning('Manager rejected our output notification: %d %s',
                                      resp.status_code, resp.text)
            except Exception:
                self._log.exception('error POSTing to manager /output-produced')

        self.loop.create_task(do_post())

    def change_status(self, new_status: str):
        """Called whenever the Flamenco Manager has a change in current status for us."""

        self._log.info('Manager requested we go to status %r', new_status)
        status_change_handlers = {
            'asleep': self.go_to_state_asleep,
            'awake': self.go_to_state_awake,
            'shutdown': self.go_to_state_shutdown,
            'error': self.go_to_state_error,
        }

        try:
            handler = status_change_handlers[new_status]
        except KeyError:
            self._log.error('We have no way to go to status %r, going to sleep instead', new_status)
            handler = self.go_to_state_asleep

        handler()

    def ack_status_change(self, new_status: str) -> typing.Optional[asyncio.Task]:
        """Confirm that we're now in a certain state.

        This ACK can be given without a request from the server, for example to support
        state changes originating from UNIX signals.
        """

        try:
            post = self.manager.post('/ack-status-change/%s' % new_status, loop=self.loop)
            return self.loop.create_task(post)
        except Exception:
            self._log.exception('unable to notify Manager')
            return None

    def go_to_state_asleep(self):
        """Starts polling for wakeup calls."""

        self._log.info('Going to sleep')
        self.state = WorkerState.ASLEEP
        self.stop_fetching_tasks()
        self.sleeping_fut = self.loop.create_task(self.sleeping())
        self._log.debug('Created task %s', self.sleeping_fut)
        self.ack_status_change('asleep')

    def go_to_state_awake(self):
        """Restarts the task-fetching asyncio task."""

        self._log.info('Waking up')
        self.state = WorkerState.AWAKE
        self.stop_sleeping()
        self.schedule_fetch_task(FETCH_TASK_DONE_SCHEDULE_NEW_DELAY)
        self.ack_status_change('awake')

    def go_to_state_shutdown(self):
        """Shuts down the Flamenco Worker.

        Whether it comes back up depends on the environment. For example,
        using systemd on Linux with Restart=always will do this.
        """

        self._log.info('Shutting down by request of the Manager or due to single-task mode')
        self.state = WorkerState.SHUTTING_DOWN

        # Don't bother acknowledging this status, as we'll push an "offline" status anyway.
        # This also makes sure that when we're asleep and told to shut down, the Manager
        # sees an asleep → offline status change, and can remember that we should go back
        # to asleep status when we come back online.
        self.loop.stop()

    def go_to_state_error(self):
        """Go to the error state and try going to active after a delay."""
        self.state = WorkerState.ERROR
        self._log.warning('Going to state %r', self.state.value)
        self.ack_status_change(self.state.value)
        self.sleeping_fut = self.loop.create_task(self.sleeping_for_error())

    def stop_sleeping(self):
        """Stops the asyncio task for sleeping."""
        if self.sleeping_fut is None or self.sleeping_fut.done():
            return
        self.sleeping_fut.cancel()
        try:
            self.sleeping_fut.result()
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass
        except Exception:
            self._log.exception('Unexpected exception in sleeping() task.')

    async def sleeping(self):
        """Regularly polls the Manager to see if we're allowed to wake up again."""

        while self.state != WorkerState.SHUTTING_DOWN and self.loop.is_running():
            try:
                await asyncio.sleep(ASLEEP_POLL_STATUS_CHANGE_REQUESTED_DELAY)
                resp = await self.manager.get('/status-change', loop=self.loop)

                if resp.status_code == 204:
                    # No change, don't do anything
                    self._log.debug('status the same, continuing sleeping')
                elif resp.status_code == 200:
                    # There is a status change
                    self._log.debug('/status-change: %s', resp.json())
                    new_status = resp.json()['status_requested']
                    self.change_status(new_status)
                    return
                else:
                    self._log.error(
                        'Error %d trying to fetch /status-change on Manager, will retry later.',
                        resp.status_code)
            except asyncio.CancelledError:
                self._log.info('Sleeping ended')
                return
            except:
                self._log.exception('problems while sleeping')

    async def sleeping_for_error(self):
        """After a delay go to active mode to see if any errors are now resolved."""

        try:
            await asyncio.sleep(ERROR_RETRY_DELAY)
        except asyncio.CancelledError:
            self._log.info('Error-sleeping ended')
            return
        except:
            self._log.exception('problems while error-sleeping')
            return

        self._log.warning('Error sleep is done, going to try to become active again')
        self.go_to_state_awake()

    def pre_task_sanity_check(self):
        """Perform readability and writability checks before fetching tasks."""

        self._pre_task_check_read()
        self._pre_task_check_write()
        self._log.getChild('sanity_check').debug('Pre-task sanity check OK')

    def _pre_task_check_read(self):
        pre_task_check_read = self.pretask_check_params.pre_task_check_read
        if not pre_task_check_read:
            return

        log = self._log.getChild('sanity_check')
        log.debug('Performing pre-task read check')
        for read_name in pre_task_check_read:
            read_path = pathlib.Path(read_name).absolute()
            log.debug('   - Read check on %s', read_path)
            if not read_path.exists():
                raise PreTaskCheckFailed('%s does not exist' % read_path) from None
            if read_path.is_dir():
                try:
                    (read_path / 'anything').stat()
                except PermissionError:
                    raise PreTaskCheckFailed('%s is not readable' % read_path) from None
                except FileNotFoundError:
                    # This is expected.
                    pass
                except:
                    log.exception('Unexpected shit happened')
                    raise SystemExit(44)
            else:
                try:
                    with read_path.open(mode='r') as the_file:
                        the_file.read(1)
                except IOError:
                    raise PreTaskCheckFailed('%s is not readable' % read_path) from None

    def _pre_task_check_write(self):
        pre_task_check_write = self.pretask_check_params.pre_task_check_write
        if not pre_task_check_write:
            return

        log = self._log.getChild('sanity_check')
        log.debug('Performing pre-task write check')
        for write_name in pre_task_check_write:
            write_path = pathlib.Path(write_name).absolute()
            log.debug('   - Write check on %s', write_path)

            post_delete = False
            try:
                if write_path.is_dir():
                    testfile = tempfile.TemporaryFile('w', dir=str(write_path), encoding='utf8')
                else:
                    post_delete = not write_path.exists()
                    testfile = write_path.open('a+', encoding='utf8')
                with testfile as outfile:
                    outfile.write('♥')
            except (PermissionError, FileNotFoundError):
                raise PreTaskCheckFailed('%s is not writable' % write_path) from None
            if post_delete:
                try:
                    write_path.unlink()
                except PermissionError:
                    log.warning('Unable to delete write-test-file %s', write_path)


def generate_secret() -> str:
    """Generates a 64-character secret key."""

    import random
    import string

    randomizer = random.SystemRandom()
    tokens = string.ascii_letters + string.digits
    secret = ''.join(randomizer.choice(tokens) for _ in range(64))

    return secret


def detect_platform() -> str:
    """Detects the platform, returning 'linux', 'windows' or 'darwin'.

    Raises an exception when the current platform cannot be detected
    as one of those three.
    """

    import platform

    plat = platform.system().lower()
    if not plat:
        raise EnvironmentError('Unable to determine platform.')

    if plat in {'linux', 'windows', 'darwin'}:
        return plat

    raise EnvironmentError('Unable to determine platform; unknown platform %r', plat)
