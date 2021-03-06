"""Commandline interface entry points."""

import argparse
import asyncio
import logging
import logging.config
import os
import pathlib
import platform
import typing

import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=pathlib.Path,
                        help='Load this configuration file instead of the default files.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Show configuration before starting, '
                             'and asyncio task status at shutdown.')
    parser.add_argument('-V', '--version', action='store_true',
                        help='Show the version of Flamenco Worker and stops.')
    parser.add_argument('-r', '--reregister', action='store_true',
                        help="Erases authentication information and re-registers this worker "
                             "at the Manager. WARNING: this can cause duplicate worker information "
                             "in the Manager's database.")
    parser.add_argument('-d', '--debug', action='store_true',
                        help="Enables debug logging for Flamenco Worker's own log entries. "
                             "Edit the logging config in flamenco-worker.cfg "
                             "for more powerful options.")
    parser.add_argument('-t', '--test', action='store_true',
                        help="Starts up in testing mode, in which only a handful of "
                             "test-specific task types are accepted. This overrides the task_types "
                             "in the configuration file.")
    parser.add_argument('-1', '--single', action='store_true',
                        help="Runs a single tasks, then exits.")
    args = parser.parse_args()

    if args.version:
        from . import __version__
        print(__version__)
        raise SystemExit()

    # Load configuration
    from . import config
    confparser = config.load_config(args.config, args.verbose, args.test)
    config.configure_logging(confparser, enable_debug=args.debug)

    log = logging.getLogger(__name__)
    log.debug('Starting, pid=%d', os.getpid())

    log_startup()

    if args.test:
        log.warning('Test mode enabled, overriding task_types=%r',
                    confparser.value('task_types'))

    if args.reregister:
        log.warning('Erasing worker_id and worker_secret so we can attempt re-registration.')
        confparser.erase('worker_id')
        confparser.erase('worker_secret')

    if args.single:
        log.info('Running in single-task mode, will stop after performing one task.')

    # Find the Manager using UPnP/SSDP if we have no manager_url.
    if not confparser.value('manager_url'):
        from . import ssdp_discover

        try:
            manager_url = ssdp_discover.find_flamenco_manager()
        except ssdp_discover.DiscoveryFailed:
            log.fatal('Unable to find Flamenco Manager via UPnP/SSDP.')
            raise SystemExit(1)

        log.info('Found Flamenco Manager at %s', manager_url)
        confparser.setvalue('manager_url', manager_url)

    # Patch AsyncIO
    from . import patch_asyncio
    patch_asyncio.patch_asyncio()

    # Construct the AsyncIO loop
    loop = construct_asyncio_loop()
    if args.verbose:
        log.debug('Enabling AsyncIO debugging')
        loop.set_debug(True)
    shutdown_future = loop.create_future()

    # Piece all the components together.
    from . import runner, worker, upstream, upstream_update_queue, may_i_run, __version__

    fmanager = upstream.FlamencoManager(
        manager_url=confparser.value('manager_url'),
        flamenco_worker_version=__version__,
    )

    tuqueue = upstream_update_queue.TaskUpdateQueue(
        db_fname=confparser.value('task_update_queue_db'),
        manager=fmanager,
        shutdown_future=shutdown_future,
    )
    trunner = runner.TaskRunner(
        shutdown_future=shutdown_future,
        subprocess_pid_file=confparser.value('subprocess_pid_file'),
    )

    pretask_check_params = parse_pretask_check_config(confparser, log)

    fworker = worker.FlamencoWorker(
        manager=fmanager,
        trunner=trunner,
        tuqueue=tuqueue,
        task_types=confparser.value('task_types').split(),
        worker_id=confparser.value('worker_id'),
        worker_secret=confparser.value('worker_secret'),
        worker_registration_secret=confparser.value('worker_registration_secret'),
        loop=loop,
        shutdown_future=shutdown_future,
        push_log_max_interval=confparser.interval_secs('push_log_max_interval_seconds'),
        push_log_max_entries=confparser.value('push_log_max_entries', int),
        push_act_max_interval=confparser.interval_secs('push_act_max_interval_seconds'),
        initial_state='testing' if args.test else 'awake',
        run_single_task=args.single,
        pretask_check_params=pretask_check_params,
    )

    mir = may_i_run.MayIRun(
        manager=fmanager,
        worker=fworker,
        poll_interval=confparser.interval_secs('may_i_run_interval_seconds'),
        loop=loop,
    )

    def shutdown(signum, stackframe):
        """Perform a clean shutdown."""

        # Raise an exception, so that the exception is bubbled upwards, until
        # the asyncio loop stops executing the current task. Only then can we
        # run things like loop.run_until_complete(mir_work_task).
        log.warning('Shutting down due to signal %i', signum)
        raise KeyboardInterrupt()

    def sleep(signum, stackframe):
        log.warning('Going asleep due to signal %i', signum)
        fworker.go_to_state_asleep()

    def wakeup(signum, stackframe):
        log.warning('Waking up due to signal %i', signum)
        fworker.go_to_state_awake()

    # Shut down cleanly upon TERM signal
    import signal
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    if hasattr(signal, 'SIGUSR1'):
        # Windows doesn't have USR1/2 signals.
        signal.signal(signal.SIGUSR1, sleep)
        signal.signal(signal.SIGUSR2, wakeup)

    if hasattr(signal, 'SIGPOLL'):
        # Not sure how cross-platform SIGPOLL is.
        signal.signal(signal.SIGPOLL, asyncio_report_tasks)

    # Start asynchronous tasks.
    asyncio.ensure_future(tuqueue.work(loop=loop))
    mir_work_task = asyncio.ensure_future(mir.work())

    def do_clean_shutdown():
        shutdown_future.cancel()
        mir_work_task.cancel()
        try:
            loop.run_until_complete(asyncio.wait_for(mir_work_task, 5))
        except requests.exceptions.ConnectionError:
            log.warning("Unable to connect to HTTP server, but that's fine as we're shutting down.")
        except asyncio.TimeoutError:
            log.debug("Timeout waiting for may-I-run task, "
                      "but that's fine as we're shutting down.")
        except KeyboardInterrupt:
            log.info('Keyboard interrupt while shutting down, ignoring as we are shutting down.')

        fworker.shutdown()

        async def stop_loop():
            log.info('Waiting to give tasks the time to stop gracefully')
            await asyncio.sleep(1)
            loop.stop()

        loop.run_until_complete(stop_loop())

    try:
        loop.run_until_complete(fworker.startup())
        fworker.mainloop()
    except worker.UnableToRegisterError:
        # The worker will have logged something, we'll just shut down cleanly.
        pass
    except KeyboardInterrupt:
        do_clean_shutdown()
    except:
        log.exception('Uncaught exception!')
    else:
        do_clean_shutdown()

    # Report on the asyncio task status
    if args.verbose:
        asyncio_report_tasks()

    log.warning('Closing asyncio loop')
    loop.close()
    log.warning('Flamenco Worker is shut down')


def parse_pretask_check_config(confparser, log):
    """Parse the [pre_task_check] config section.

    :rtype: flamenco.worker.PreTaskCheckParams
    """
    from . import worker

    check_read = []
    check_write = []
    for name, value in confparser.items(section='pre_task_check'):
        if name.startswith('write'):
            check_write.append(pathlib.Path(value))
        elif name.startswith('read'):
            check_read.append(pathlib.Path(value))
        else:
            log.fatal('Config section "pre_task_check" should only have keys starting with '
                      '"read" or "write"; found %r', value)
            raise SystemExit(47)
    pretask_check_params = worker.PreTaskCheckParams(
        pre_task_check_write=tuple(check_write),
        pre_task_check_read=tuple(check_read),
    )
    return pretask_check_params


def asyncio_report_tasks(signum=0, stackframe=None):
    """Runs the garbage collector, then reports all AsyncIO tasks on the log.

    Can be used as signal handler.
    """

    log = logging.getLogger('%s.asyncio_report_tasks' % __name__)
    log.info('Logging all asyncio tasks.')

    all_tasks = asyncio.Task.all_tasks()
    count_done = sum(task.done() for task in all_tasks)

    if not len(all_tasks):
        log.info('No scheduled tasks')
    elif len(all_tasks) == count_done:
        log.info('All %i tasks are done.', len(all_tasks))
    else:
        log.info('%i tasks, of which %i are done.', len(all_tasks), count_done)

    import gc
    import traceback

    # Clean up circular references between tasks.
    gc.collect()

    for task_idx, task in enumerate(all_tasks):
        if not task.done():
            log.info('   task #%i: %s', task_idx, task)
            continue

        # noinspection PyBroadException
        try:
            res = task.result()
            log.info('   task #%i: %s result=%r', task_idx, task, res)
        except asyncio.CancelledError:
            # No problem, we want to stop anyway.
            log.info('   task #%i: %s cancelled', task_idx, task)
        except Exception:
            log.info('%s: resulted in exception: %s', task, traceback.format_exc())

        # for ref in gc.get_referrers(task):
        #     log.info('      - referred by %s', ref)

    log.info('Done logging.')


def construct_asyncio_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()

    # On Windows, the default event loop is SelectorEventLoop which does
    # not support subprocesses. ProactorEventLoop should be used instead.
    # Source: https://docs.python.org/3.5/library/asyncio-subprocess.html
    if platform.system() == 'Windows':
        # Silly MyPy doesn't understand this only runs on Windows.
        if not isinstance(loop, asyncio.ProactorEventLoop):  # type: ignore
            loop = asyncio.ProactorEventLoop()  # type: ignore

    asyncio.set_event_loop(loop)
    return loop


def log_startup():
    """Log the version of Flamenco Worker."""

    from . import __version__

    log = logging.getLogger(__name__)
    old_level = log.level
    try:
        log.setLevel(logging.INFO)
        log.info('Starting Flamenco Worker %s', __version__)
    finally:
        log.setLevel(old_level)


if __name__ == '__main__':
    main()
