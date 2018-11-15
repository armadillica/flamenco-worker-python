"""Command implementations."""

import abc
import asyncio
import asyncio.subprocess
import datetime
import logging
import pathlib
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import typing
from pathlib import Path

import attr
import psutil

from . import worker

command_handlers = {}

# Timeout of subprocess.stdout.readline() call.
SUBPROC_READLINE_TIMEOUT = 3600  # seconds

MERGE_EXR_PYTHON = """\
import bpy
scene = bpy.context.scene
nodes = scene.node_tree.nodes
image1 = bpy.data.images.load("%(input1)s")
image2 = bpy.data.images.load("%(input2)s")

nodes["image1"].image = image1
nodes["image2"].image = image2
nodes["weight1"].outputs[0].default_value = %(weight1)i
nodes["weight2"].outputs[0].default_value = %(weight2)i

nodes["output"].base_path = "%(tmpdir)s"
scene.render.resolution_x, scene.render.resolution_y = image1.size
scene.render.tile_x, scene.render.tile_y = image1.size
scene.render.filepath = "%(tmpdir)s/preview.jpg"

bpy.ops.render.render(write_still=True)
"""


def command_executor(cmdname):
    """Class decorator, registers a command executor."""

    def decorator(cls):
        assert cmdname not in command_handlers

        command_handlers[cmdname] = cls
        cls.command_name = cmdname
        return cls

    return decorator


class CommandExecutionError(Exception):
    """Raised when there was an error executing a command."""
    pass


@attr.s
class AbstractCommand(metaclass=abc.ABCMeta):
    """Command executor.

    This class (or any of its subclasses) should not directly set the task status.
    This should be left to the Worker.
    """
    worker = attr.ib(validator=attr.validators.instance_of(worker.FlamencoWorker),
                     repr=False)
    task_id = attr.ib(validator=attr.validators.instance_of(str))
    command_idx = attr.ib(validator=attr.validators.instance_of(int))

    # Set by @command_executor
    command_name = ''

    # Set by __attr_post_init__()
    identifier = attr.ib(default=None, init=False,
                         validator=attr.validators.optional(attr.validators.instance_of(str)))
    _log = attr.ib(default=None, init=False,
                   validator=attr.validators.optional(attr.validators.instance_of(logging.Logger)))

    def __attrs_post_init__(self):
        self.identifier = '%s.(task_id=%s, command_idx=%s)' % (
            self.command_name,
            self.task_id,
            self.command_idx)
        self._log = logging.getLogger('%s.%s' % (__name__, self.identifier))

    async def run(self, settings: dict) -> bool:
        """Runs the command, parsing output and sending it back to the worker.

        Returns True when the command was succesful, and False otherwise.
        """

        verr = self.validate(settings)
        if verr is not None:
            self._log.warning('%s: Error in settings: %s', self.identifier, verr)
            await self.worker.register_log('%s: Error in settings: %s', self.identifier, verr)
            await self.worker.register_task_update(
                task_status='failed',
                activity='%s: Invalid settings: %s' % (self.identifier, verr),
            )
            return False

        await self.worker.register_log('%s: Starting' % self.command_name)
        await self.worker.register_task_update(
            activity='starting %s' % self.command_name,
            current_command_idx=self.command_idx,
            command_progress_percentage=0
        )

        try:
            await self.execute(settings)
        except CommandExecutionError as ex:
            # This is something we threw ourselves, and there is no need to log the traceback.
            self._log.warning('Error executing: %s', ex)
            await self._register_exception(ex)
            return False
        except asyncio.CancelledError as ex:
            self._log.warning('Command execution was canceled')
            raise
        except Exception as ex:
            # This is something unexpected, so do log the traceback.
            self._log.exception('Error executing.')
            await self._register_exception(ex)
            return False

        await self.worker.register_log('%s: Finished' % self.command_name)
        await self.worker.register_task_update(
            activity='finished %s' % self.command_name,
            current_command_idx=self.command_idx,
            command_progress_percentage=100
        )

        return True

    async def abort(self):
        """Aborts the command. This may or may not be actually possible.

        A subprocess that's started by this command will be killed.
        However, any asyncio coroutines that are not managed by this command
        (such as the 'run' function) should be cancelled by the caller.
        """

    async def _register_exception(self, ex: Exception):
        """Registers an exception with the worker, and set the task status to 'failed'."""

        await self.worker.register_log('%s: Error executing: %s' % (self.identifier, ex))
        await self.worker.register_task_update(
            activity='%s: Error executing: %s' % (self.identifier, ex),
        )

    @abc.abstractmethod
    async def execute(self, settings: dict):
        """Executes the command.

        An error should be indicated by an exception.
        """

    def validate(self, settings: dict):
        """Validates the settings for this command.

        If there is an error, a description of the error is returned.
        If the settings are valid, None is returned.

        By default all settings are considered valid.
        """

        return None

    def _setting(self, settings: dict, key: str, is_required: bool, valtype: typing.Type = str) -> (
            typing.Any, typing.Optional[str]):
        """Parses a setting, returns either (value, None) or (None, errormsg)"""

        try:
            value = settings[key]
        except KeyError:
            if is_required:
                return None, 'Missing "%s"' % key
            return None, None

        if value is None and not is_required:
            return None, None

        if not isinstance(value, valtype):
            return None, '"%s" must be a %s, not a %s' % (key, valtype, type(value))

        return value, None


@command_executor('echo')
class EchoCommand(AbstractCommand):
    def validate(self, settings: dict):
        try:
            msg = settings['message']
        except KeyError:
            return 'Missing "message"'

        if not isinstance(msg, str):
            return 'Message must be a string'

    async def execute(self, settings: dict):
        await self.worker.register_log(settings['message'])


@command_executor('log_a_lot')
class LogALotCommand(AbstractCommand):
    def validate(self, settings: dict):
        lines = settings.get('lines', 20000)
        if isinstance(lines, float):
            lines = int(lines)
        if not isinstance(lines, int):
            return '"lines" setting must be an integer, not %s' % type(lines)

    async def execute(self, settings: dict):
        lines = settings.get('lines', 20000)

        await self.worker.register_task_update(activity='logging %d lines' % lines)
        for idx in range(lines):
            await self.worker.register_log(30 * ('This is line %d' % idx))


@command_executor('sleep')
class SleepCommand(AbstractCommand):
    def validate(self, settings: dict):
        try:
            sleeptime = settings['time_in_seconds']
        except KeyError:
            return 'Missing "time_in_seconds"'

        if not isinstance(sleeptime, (int, float)):
            return 'time_in_seconds must be an int or float'

    async def execute(self, settings: dict):
        time_in_seconds = settings['time_in_seconds']
        await self.worker.register_log('Sleeping for %s seconds' % time_in_seconds)
        await asyncio.sleep(time_in_seconds)
        await self.worker.register_log('Done sleeping for %s seconds' % time_in_seconds)


def _timestamped_path(path: Path) -> Path:
    """Returns the path with its modification time appended to the name."""

    mtime = path.stat().st_mtime

    # Round away the milliseconds, as those aren't all that interesting.
    # Uniqueness is ensured by calling _unique_path().
    mdatetime = datetime.datetime.fromtimestamp(round(mtime))

    # Make the ISO-8601 timestamp a bit more eye- and filename-friendly.
    iso = mdatetime.isoformat().replace('T', '_').replace(':', '')
    dst = path.with_name('%s-%s' % (path.name, iso))

    return dst


def _unique_path(path: Path) -> Path:
    """Returns the path, or if it exists, the path with a unique suffix."""

    suf_re = re.compile(r'~([0-9]+)$')

    # See which suffixes are in use
    max_nr = 0
    for altpath in path.parent.glob(path.name + '~*'):
        m = suf_re.search(altpath.name)
        if not m:
            continue

        suffix = m.group(1)
        try:
            suffix = int(suffix)
        except ValueError:
            continue
        max_nr = max(max_nr, suffix)
    return path.with_name(path.name + '~%i' % (max_nr + 1))


@command_executor('move_out_of_way')
class MoveOutOfWayCommand(AbstractCommand):
    def validate(self, settings: dict):
        try:
            src = settings['src']
        except KeyError:
            return 'Missing "src"'

        if not isinstance(src, str):
            return 'src must be a string'

    async def execute(self, settings: dict):
        src = Path(settings['src'])
        if not src.exists():
            self._log.info('Render output path %s does not exist, not moving out of way', src)
            await self.worker.register_log('%s: Render output path %s does not exist, '
                                           'not moving out of way', self.command_name, src)
            return

        dst = _timestamped_path(src)
        if dst.exists():
            self._log.debug('Destination %s exists, finding one that does not', dst)
            dst = _unique_path(dst)
            self._log.debug('New destination is %s', dst)

        self._log.info('Moving %s to %s', src, dst)
        await self.worker.register_log('%s: Moving %s to %s', self.command_name, src, dst)
        src.rename(dst)


@command_executor('move_to_final')
class MoveToFinalCommand(AbstractCommand):
    def validate(self, settings: dict):
        _, err1 = self._setting(settings, 'src', True)
        _, err2 = self._setting(settings, 'dest', True)
        return err1 or err2

    async def execute(self, settings: dict):
        src = Path(settings['src'])
        if not src.exists():
            msg = 'Path %s does not exist, not moving' % src
            self._log.info(msg)
            await self.worker.register_log('%s: %s', self.command_name, msg)
            return

        dest = Path(settings['dest'])
        if dest.exists():
            backup = _timestamped_path(dest)
            self._log.debug('Destination %s exists, moving out of the way to %s', dest, backup)

            if backup.exists():
                self._log.debug('Destination %s exists, finding one that does not', backup)
                backup = _unique_path(backup)
                self._log.debug('New destination is %s', backup)

            self._log.info('Moving %s to %s', dest, backup)
            await self.worker.register_log('%s: Moving %s to %s', self.command_name, dest, backup)
            dest.rename(backup)

        self._log.info('Moving %s to %s', src, dest)
        await self.worker.register_log('%s: Moving %s to %s', self.command_name, src, dest)
        src.rename(dest)


@command_executor('copy_file')
class CopyFileCommand(AbstractCommand):
    def validate(self, settings: dict):
        src, err = self._setting(settings, 'src', True)
        if err:
            return err
        if not src:
            return 'src may not be empty'
        dest, err = self._setting(settings, 'dest', True)
        if err:
            return err
        if not dest:
            return 'dest may not be empty'

    async def execute(self, settings: dict):
        src = Path(settings['src'])
        if not src.exists():
            raise CommandExecutionError('Path %s does not exist, unable to copy' % src)

        dest = Path(settings['dest'])
        if dest.exists():
            msg = 'Destination %s exists, going to overwrite it.' % dest
            self._log.info(msg)
            await self.worker.register_log('%s: %s', self.command_name, msg)

        self._log.info('Copying %s to %s', src, dest)
        await self.worker.register_log('%s: Copying %s to %s', self.command_name, src, dest)

        if not dest.parent.exists():
            await self.worker.register_log('%s: Target directory %s does not exist; creating.',
                                           self.command_name, dest.parent)
            dest.parent.mkdir(parents=True)

        shutil.copy(str(src), str(dest))
        self.worker.output_produced(dest)


@command_executor('remove_tree')
class RemoveTreeCommand(AbstractCommand):
    def validate(self, settings: dict):
        path, err = self._setting(settings, 'path', True)
        if err:
            return err
        if not path:
            return "'path' may not be empty"

    async def execute(self, settings: dict):
        path = Path(settings['path'])
        if not path.exists():
            msg = 'Path %s does not exist, so not removing.' % path
            self._log.debug(msg)
            await self.worker.register_log(msg)
            return

        msg = 'Removing tree rooted at %s' % path
        self._log.info(msg)
        await self.worker.register_log(msg)

        if path.is_dir():
            shutil.rmtree(str(path))
        else:
            path.unlink()


@attr.s
class AbstractSubprocessCommand(AbstractCommand):
    readline_timeout = attr.ib(default=SUBPROC_READLINE_TIMEOUT)
    proc = attr.ib(validator=attr.validators.instance_of(asyncio.subprocess.Process),
                   init=False)

    @property
    def subprocess_pid_file(self) -> typing.Optional[pathlib.Path]:
        subprocess_pid_file = self.worker.trunner.subprocess_pid_file
        if not subprocess_pid_file:
            return None
        return pathlib.Path(subprocess_pid_file)

    def validate(self, settings: dict) -> typing.Optional[str]:
        supererr = super().validate(settings)
        if supererr:
            return supererr

        pidfile = self.subprocess_pid_file
        if pidfile is None:
            self._log.warning('No subprocess PID file configured; this is not recommended.')
            return None

        try:
            pid_str = pidfile.read_text()
        except FileNotFoundError:
            # This is expected, as it means no subprocess is running.
            return None
        if not pid_str:
            pidfile.unlink()
            return None

        pid = int(pid_str)
        self._log.warning('Found PID file %s with PID %r', pidfile, pid)

        try:
            proc = psutil.Process(pid)
        except psutil.NoSuchProcess:
            self._log.warning('Deleting pidfile %s for stale PID %r', pidfile, pid)
            pidfile.unlink()
            return None
        return 'Subprocess from %s is still running: %s' % (pidfile, proc)

    async def subprocess(self, args: list):
        cmd_to_log = ' '.join(shlex.quote(s) for s in args)
        self._log.info('Executing %s', cmd_to_log)
        await self.worker.register_log('Executing %s', cmd_to_log)

        self.proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        pid_path = self.subprocess_pid_file
        pid = self.proc.pid
        if pid_path:
            # Require exclusive creation to prevent race conditions.
            with pid_path.open('x') as pidfile:
                pidfile.write(str(pid))

        try:
            while not self.proc.stdout.at_eof():
                try:
                    line = await asyncio.wait_for(self.proc.stdout.readline(),
                                                  self.readline_timeout)
                except asyncio.TimeoutError:
                    raise CommandExecutionError('Command pid=%d timed out after %i seconds' %
                                                (pid, self.readline_timeout))

                if len(line) == 0:
                    # EOF received, so let's bail.
                    break

                try:
                    line = line.decode('utf8')
                except UnicodeDecodeError as ex:
                    await self.abort()
                    raise CommandExecutionError(
                        'Command pid=%d produced non-UTF8 output, aborting: %s' % (pid, ex))

                line = line.rstrip()
                self._log.debug('Read line pid=%d: %s', pid, line)
                line = await self.process_line(line)
                if line is not None:
                    await self.worker.register_log(line)

            retcode = await self.proc.wait()
            self._log.info('Command %r (pid=%d) stopped with status code %s', args, pid, retcode)

            if retcode:
                raise CommandExecutionError('Command pid=%d failed with status %s' % (pid, retcode))
        except asyncio.CancelledError:
            self._log.info('asyncio task got canceled, killing subprocess pid=%d', pid)
            await self.abort()
            raise
        finally:
            if pid_path:
                pid_path.unlink()

    async def process_line(self, line: str) -> typing.Optional[str]:
        """Processes the line, returning None to ignore it."""

        return 'pid=%d > %s' % (self.proc.pid, line)

    async def abort(self):
        """Aborts the command by killing the subprocess."""

        if self.proc is None or self.proc == attr.NOTHING:
            self._log.debug("No process to kill. That's ok.")
            return

        self._log.info('Terminating subprocess pid=%d', self.proc.pid)

        try:
            self.proc.terminate()
        except ProcessLookupError:
            self._log.debug("The process was already stopped, aborting is impossible. That's ok.")
            return
        except AttributeError:
            # This can happen in some race conditions, it's fine.
            self._log.debug("The process was not yet started, aborting is impossible. That's ok.")
            return

        timeout = 5
        try:
            retval = await asyncio.wait_for(self.proc.wait(), timeout,
                                            loop=asyncio.get_event_loop())
        except asyncio.TimeoutError:
            pass
        else:
            self._log.info('The process pid=%d aborted with status code %s', self.proc.pid, retval)
            return

        self._log.warning('The process pid=%d did not stop in %d seconds, going to kill it',
                          self.proc.pid, timeout)
        try:
            self.proc.kill()
        except ProcessLookupError:
            self._log.debug("The process pid=%d was already stopped, aborting is impossible. "
                            "That's ok.", self.proc.pid)
            return
        except AttributeError:
            # This can happen in some race conditions, it's fine.
            self._log.debug("The process pid=%d was not yet started, aborting is impossible. "
                            "That's ok.", self.proc.pid)
            return

        retval = await self.proc.wait()
        self._log.info('The process %d aborted with status code %s', self.proc.pid, retval)


@command_executor('exec')
class ExecCommand(AbstractSubprocessCommand):
    def validate(self, settings: dict):
        try:
            cmd = settings['cmd']
        except KeyError:
            return 'Missing "cmd"'

        if not isinstance(cmd, str):
            return '"cmd" must be a string'
        if not cmd:
            return '"cmd" may not be empty'
        return super().validate(settings)

    async def execute(self, settings: dict):
        await self.subprocess(shlex.split(settings['cmd']))


@command_executor('blender_render')
class BlenderRenderCommand(AbstractSubprocessCommand):
    re_global_progress = attr.ib(init=False)
    re_time = attr.ib(init=False)
    re_remaining = attr.ib(init=False)
    re_status = attr.ib(init=False)
    re_path_not_found = attr.ib(init=False)
    re_file_saved = attr.ib(init=False)

    # These lines are produced by Cycles (and other rendering engines) for each
    # object, choking the Manager with logs when there are too many objects.
    # For now we have some custom code to swallow those lines, in lieu of a
    # logging system that can handle those volumes properly.
    substring_synchronizing = {'| Synchronizing object |', ' | Syncing '}
    seen_synchronizing_line = False

    def __attrs_post_init__(self):
        super().__attrs_post_init__()

        # Delay regexp compilation until a BlenderRenderCommand is actually constructed.
        self.re_global_progress = re.compile(
            r"^Fra:(?P<fra>\d+) Mem:(?P<mem>[^ ]+) \(.*?, Peak (?P<peakmem>[^ ]+)\)")
        self.re_time = re.compile(
            r'\| Time:((?P<hours>\d+):)?(?P<minutes>\d+):(?P<seconds>\d+)\.(?P<hunds>\d+) ')
        self.re_remaining = re.compile(
            r'\| Remaining:((?P<hours>\d+):)?(?P<minutes>\d+):(?P<seconds>\d+)\.(?P<hunds>\d+) ')
        self.re_status = re.compile(r'\| (?P<status>[^\|]+)\s*$')
        self.re_path_not_found = re.compile(r"Warning: Path '.*' not found")
        self.re_file_saved = re.compile(r"Saved: '(?P<filename>.*)'")

    def validate(self, settings: dict):
        blender_cmd, err = self._setting(settings, 'blender_cmd', True)
        if err:
            return err
        cmd = shlex.split(blender_cmd)
        if not Path(cmd[0]).exists():
            return 'blender_cmd %r does not exist' % cmd[0]
        settings['blender_cmd'] = cmd

        render_output, err = self._setting(settings, 'render_output', False)
        if err:
            return err
        if render_output:
            outpath = Path(render_output).parent
            try:
                outpath.mkdir(parents=True, exist_ok=True)
            except Exception as ex:
                return '"render_output": dir %s cannot be created: %s' % (outpath, ex)

        _, err = self._setting(settings, 'frames', False)
        if err:
            return err
        _, err = self._setting(settings, 'render_format', False)
        if err:
            return err

        filepath, err = self._setting(settings, 'filepath', True)
        if err:
            return err
        if not Path(filepath).exists():
            # Let's just wait a few seconds for the file to appear. Networks can be async.
            self._log.warning('file %s does not exist, waiting for a bit to see if it appears',
                              filepath)
            time.sleep(5)
        if not Path(filepath).exists():
            # Ok, now it's fatal.
            return 'filepath %r does not exist' % filepath

        return super().validate(settings)

    async def execute(self, settings: dict):
        cmd = self._build_blender_cmd(settings)

        await self.worker.register_task_update(activity='Starting Blender')
        await self.subprocess(cmd)

    def _build_blender_cmd(self, settings):
        cmd = settings['blender_cmd'][:]
        cmd += [
            '--enable-autoexec',
            '-noaudio',
            '--background',
            settings['filepath'],
        ]
        if settings.get('python_expr'):
            cmd.extend(['--python-expr', settings['python_expr']])
        if settings.get('render_output'):
            cmd.extend(['--render-output', settings['render_output']])
        if settings.get('format'):
            cmd.extend(['--render-format', settings['format']])
        if settings.get('frames'):
            cmd.extend(['--render-frame', settings['frames']])
        return cmd

    def parse_render_line(self, line: str) -> typing.Optional[dict]:
        """Parses a single line of render progress.

        Returns None if this line does not contain render progress.
        """

        m = self.re_global_progress.search(line)
        if not m:
            return None
        info = m.groupdict()
        info['fra'] = int(info['fra'])

        m = self.re_time.search(line)
        if m:
            info['time_sec'] = (3600 * int(m.group('hours') or 0) +
                                60 * int(m.group('minutes')) +
                                int(m.group('seconds')) +
                                int(m.group('hunds')) / 100)

        m = self.re_remaining.search(line)
        if m:
            info['remaining_sec'] = (3600 * int(m.group('hours') or 0) +
                                     60 * int(m.group('minutes')) +
                                     int(m.group('seconds')) +
                                     int(m.group('hunds')) / 100)

        m = self.re_status.search(line)
        if m:
            info['status'] = m.group('status')

        return info

    def _is_sync_line(self, line: str) -> bool:
        return any(substring in line
                   for substring in self.substring_synchronizing)

    async def process_line(self, line: str) -> typing.Optional[str]:
        """Processes the line, returning None to ignore it."""

        # See if there are any warnings about missing files. If so, we update the activity for it.
        if 'Warning: Unable to open' in line or self.re_path_not_found.search(line):
            await self.worker.register_task_update(activity=line)

        if self._is_sync_line(line):
            if self.seen_synchronizing_line:
                return None
            self.seen_synchronizing_line = True
            return '> %s  (NOTE FROM WORKER: only logging this line; skipping the rest of ' \
                   'the Synchronizing Objects lines)' % line

        render_info = self.parse_render_line(line)
        if render_info:
            # Render progress. Not interesting to log all of them, but we do use
            # them to update the render progress.
            # TODO: For now we return this as a string, but at some point we may want
            # to consider this as a subdocument.
            if 'remaining_sec' in render_info:
                fmt = 'Fra:{fra} Mem:{mem} | Time:{time_sec} | Remaining:{remaining_sec} | {status}'
                activity = fmt.format(**render_info)
            else:
                self._log.debug('Unable to find remaining time in line: %s', line)
                activity = line
            await self.worker.register_task_update(activity=activity)

        # See if this line logs the saving of a file.
        m = self.re_file_saved.search(line)
        if m:
            self.worker.output_produced(m.group('filename'))

        # Not a render progress line; just log it for now.
        return 'pid=%d > %s' % (self.proc.pid, line)


@command_executor('blender_render_progressive')
class BlenderRenderProgressiveCommand(BlenderRenderCommand):
    def validate(self, settings: dict):
        err = super().validate(settings)
        if err: return err

        cycles_num_chunks, err = self._setting(settings, 'cycles_num_chunks', True, int)
        if err: return err
        if cycles_num_chunks < 1:
            return '"cycles_num_chunks" must be a positive integer'

        cycles_chunk, err = self._setting(settings, 'cycles_chunk', True, int)
        if err: return err
        if cycles_chunk < 1:
            return '"cycles_chunk" must be a positive integer'

    def _build_blender_cmd(self, settings):
        cmd = super()._build_blender_cmd(settings)

        return cmd + [
            '--',
            '--cycles-resumable-num-chunks', str(settings['cycles_num_chunks']),
            '--cycles-resumable-current-chunk', str(settings['cycles_chunk']),
        ]


@command_executor('merge_progressive_renders')
class MergeProgressiveRendersCommand(AbstractSubprocessCommand):
    def validate(self, settings: dict):
        blender_cmd, err = self._setting(settings, 'blender_cmd', True)
        if err:
            return err
        cmd = shlex.split(blender_cmd)
        if not Path(cmd[0]).exists():
            return 'blender_cmd %r does not exist' % cmd[0]
        settings['blender_cmd'] = cmd

        input1, err = self._setting(settings, 'input1', True, str)
        if err: return err
        if '"' in input1:
            return 'Double quotes are not allowed in filenames: %r' % input1
        if not Path(input1).exists():
            return 'Input 1 %r does not exist' % input1
        input2, err = self._setting(settings, 'input2', True, str)
        if err: return err
        if '"' in input2:
            return 'Double quotes are not allowed in filenames: %r' % input2
        if not Path(input2).exists():
            return 'Input 2 %r does not exist' % input2

        output, err = self._setting(settings, 'output', True, str)
        if err: return err
        if '"' in output:
            return 'Double quotes are not allowed in filenames: %r' % output

        _, err = self._setting(settings, 'weight1', True, int)
        if err: return err

        _, err = self._setting(settings, 'weight2', True, int)
        if err: return err

        return super().validate(settings)

    async def execute(self, settings: dict):
        blendpath = Path(__file__).with_name('merge-exr.blend')

        cmd = settings['blender_cmd'][:]
        cmd += [
            '--factory-startup',
            '--enable-autoexec',
            '-noaudio',
            '--background',
            str(blendpath),
        ]

        # set up node properties and render settings.
        output = Path(settings['output'])
        output.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(dir=str(output.parent)) as tmpdir:
            tmppath = Path(tmpdir)
            assert tmppath.exists()

            settings['tmpdir'] = tmpdir
            cmd += [
                '--python-expr', MERGE_EXR_PYTHON % settings
            ]

            await self.worker.register_task_update(activity='Starting Blender to merge EXR files')
            await self.subprocess(cmd)

            # move output files into the correct spot.
            await self.move(tmppath / 'merged0001.exr', output)
            # await self.move(tmppath / 'preview.jpg', output.with_suffix('.jpg'))

        # See if this line logs the saving of a file.
        self.worker.output_produced(output)

    async def move(self, src: Path, dst: Path):
        """Moves a file to another location."""

        self._log.info('Moving %s to %s', src, dst)

        assert src.is_file()
        assert not dst.exists() or dst.is_file()
        assert dst.exists() or dst.parent.exists()

        await self.worker.register_log('Moving %s to %s', src, dst)
        shutil.move(str(src), str(dst))
