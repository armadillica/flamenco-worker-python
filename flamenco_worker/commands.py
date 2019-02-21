"""Command implementations."""

import abc
import asyncio
import asyncio.subprocess
import collections
import contextlib
import datetime
import logging
import pathlib
import platform
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

command_handlers = {}  # type: typing.Mapping[str, typing.Type['AbstractCommand']]

# Some type declarations.
Settings = typing.MutableMapping[str, typing.Any]
# This is the type of the 2nd arg for instanceof(a, b)
InstanceOfType = typing.Union[type, typing.Tuple[typing.Union[type, typing.Tuple[typing.Any, ...]],
                                                 ...]]

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

scene.render.resolution_x, scene.render.resolution_y = image1.size
scene.render.tile_x, scene.render.tile_y = image1.size
scene.render.filepath = "%(tmpdir)s/merged0001.exr"

scene.frame_start = 1
scene.frame_end = 1
scene.frame_set(1)
bpy.ops.render.render(write_still=True)
"""

MERGE_EXR_SEQUENCE_PYTHON = """\
import bpy
scene = bpy.context.scene
nodes = scene.node_tree.nodes

image1 = bpy.data.images.load("%(input1)s")
image2 = bpy.data.images.load("%(input2)s")
image1.source = 'SEQUENCE'
image2.source = 'SEQUENCE'

node_img1 = nodes["image1"]
node_img2 = nodes["image2"]
node_img1.image = image1
node_img2.image = image2
node_img1.frame_duration = %(frame_end)d - %(frame_start)d + 1
node_img2.frame_duration = %(frame_end)d - %(frame_start)d + 1
node_img1.frame_start = %(frame_start)d
node_img2.frame_start = %(frame_start)d
node_img1.frame_offset = %(frame_start)d - 1
node_img2.frame_offset = %(frame_start)d - 1

nodes["weight1"].outputs[0].default_value = %(weight1)i
nodes["weight2"].outputs[0].default_value = %(weight2)i

scene.render.resolution_x, scene.render.resolution_y = image1.size
scene.render.tile_x, scene.render.tile_y = image1.size
scene.render.filepath = "%(output)s"

scene.frame_start = %(frame_start)d
scene.frame_end = %(frame_end)d

bpy.ops.render.render(animation=True)
"""

HASHES_RE = re.compile('#+')

log = logging.getLogger(__name__)


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
    _log = attr.ib(init=False, default=logging.getLogger('AbstractCommand'),
                   validator=attr.validators.instance_of(logging.Logger))

    # Mapping from 'thing' name to how long it took to do that 'thing' (in seconds).
    timing: typing.MutableMapping[str, float] = attr.ib(
        init=False, default=collections.OrderedDict())

    _last_timing_event = ''
    _last_timing_checkpoint = 0.0

    def __attrs_post_init__(self):
        self.identifier = '%s.(task_id=%s, command_idx=%s)' % (
            self.command_name,
            self.task_id,
            self.command_idx)
        self._log = log.getChild(self.identifier)

    @contextlib.contextmanager
    def record_duration(self, name: str):
        """Records the duration of the context in self._timing[name]."""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            assert name not in self.timing, \
                f'{name} not expected in {self.timing}'

            self.timing[name] = duration

    def _timing_checkpoint(self, checkpoint_name: str):
        now = time.time()

        if self._last_timing_event:
            duration = now - self._last_timing_checkpoint
            assert self._last_timing_event not in self.timing, \
                f'{self._last_timing_event} not expected in {self.timing}'
            self.timing[self._last_timing_event] = duration

        self._last_timing_event = checkpoint_name
        self._last_timing_checkpoint = now

    async def run(self, settings: Settings) -> bool:
        """Runs the command, parsing output and sending it back to the worker.

        Returns True when the command was succesful, and False otherwise.
        """

        verr = self.validate(settings)
        if verr is not None:
            self._log.warning('Error in settings: %s', verr)
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

        self.timing.clear()
        try:
            with self.record_duration('total'):
                try:
                    await self.execute(settings)
                except CommandExecutionError as ex:
                    # This is something we threw ourselves; need to log the traceback.
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
        finally:
            await self.log_recorded_timings()

        await self.worker.register_log('%s: Finished' % self.command_name)
        await self.worker.register_task_update(
            activity='finished %s' % self.command_name,
            current_command_idx=self.command_idx,
            command_progress_percentage=100
        )

        return True

    async def log_recorded_timings(self) -> None:
        """Send the timing recorded in self._timing to the task log."""
        if not self.timing:
            return

        log_lines = [f'{self.command_name}: command timing information:']
        for name, duration in self.timing.items():
            delta = datetime.timedelta(seconds=duration)
            log_lines.append(f'    - {name}: {delta}')

        await self.worker.register_log('\n'.join(log_lines))

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
    async def execute(self, settings: Settings) -> None:
        """Executes the command.

        An error should be indicated by an exception.
        """

    def validate(self, settings: Settings) -> typing.Optional[str]:
        """Validates the settings for this command.

        If there is an error, a description of the error is returned.
        If the settings are valid, None is returned.

        By default all settings are considered valid.
        """

        return None

    def _setting(self, settings: Settings, key: str, is_required: bool,
                 valtype: InstanceOfType = str,
                 default: typing.Any = None) \
            -> typing.Tuple[typing.Any, typing.Optional[str]]:
        """Parses a setting, returns either (value, None) or (None, errormsg)"""

        try:
            value = settings[key]
        except KeyError:
            if is_required:
                return None, 'Missing "%s"' % key
            settings.setdefault(key, default)
            return default, None

        if value is None and not is_required:
            settings.setdefault(key, default)
            return default, None

        if not isinstance(value, valtype):
            return None, '"%s" must be a %s, not a %s' % (key, valtype, type(value))

        return value, None

    async def _mkdir_if_not_exists(self, dirpath: Path):
        """Create a directory if it doesn't exist yet.

        Also logs a message to the Worker to indicate the directory was created.
        """
        if dirpath.exists():
            return

        await self.worker.register_log('%s: Directory %s does not exist; creating.',
                                       self.command_name, dirpath)
        dirpath.mkdir(parents=True)


@command_executor('echo')
class EchoCommand(AbstractCommand):
    def validate(self, settings: Settings):
        try:
            msg = settings['message']
        except KeyError:
            return 'Missing "message"'

        if not isinstance(msg, str):
            return 'Message must be a string'

    async def execute(self, settings: Settings):
        await self.worker.register_log(settings['message'])


@command_executor('log_a_lot')
class LogALotCommand(AbstractCommand):
    def validate(self, settings: Settings):
        lines = settings.get('lines', 20000)
        if isinstance(lines, float):
            lines = int(lines)
        if not isinstance(lines, int):
            return '"lines" setting must be an integer, not %s' % type(lines)

    async def execute(self, settings: Settings):
        lines = settings.get('lines', 20000)

        await self.worker.register_task_update(activity='logging %d lines' % lines)
        for idx in range(lines):
            await self.worker.register_log(30 * ('This is line %d' % idx))


@command_executor('sleep')
class SleepCommand(AbstractCommand):
    def validate(self, settings: Settings):
        try:
            sleeptime = settings['time_in_seconds']
        except KeyError:
            return 'Missing "time_in_seconds"'

        if not isinstance(sleeptime, (int, float)):
            return 'time_in_seconds must be an int or float'

    async def execute(self, settings: Settings):
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
            suffix_value = int(suffix)
        except ValueError:
            continue
        max_nr = max(max_nr, suffix_value)
    return path.with_name(path.name + '~%i' % (max_nr + 1))


def _numbered_path(directory: Path, fname_prefix: str, fname_suffix: str) -> Path:
    """Return a unique Path with a number between prefix and suffix.

    :return: directory / '{fname_prefix}001{fname_suffix}' where 001 is
        replaced by the highest number + 1 if there already is a file with
        such a prefix & suffix.
    """

    # See which suffixes are in use
    max_nr = 0
    len_prefix = len(fname_prefix)
    len_suffix = len(fname_suffix)
    for altpath in directory.glob(f'{fname_prefix}*{fname_suffix}'):
        num_str: str = altpath.name[len_prefix:-len_suffix]

        try:
            num = int(num_str)
        except ValueError:
            continue
        max_nr = max(max_nr, num)
    return directory / f'{fname_prefix}{max_nr + 1:03}{fname_suffix}'


def _hashes_to_glob(path: Path) -> Path:
    """Transform bla-#####.exr to bla-*.exr.

    >>> _hashes_to_glob(Path('/path/to/bla-####.exr'))
    Path('/path/to/bla-*.exr')
    """
    return path.with_name(HASHES_RE.sub('*', path.name))


@command_executor('move_out_of_way')
class MoveOutOfWayCommand(AbstractCommand):
    def validate(self, settings: Settings):
        try:
            src = settings['src']
        except KeyError:
            return 'Missing "src"'

        if not isinstance(src, str):
            return 'src must be a string'

    async def execute(self, settings: Settings):
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
    def validate(self, settings: Settings):
        _, err1 = self._setting(settings, 'src', True)
        _, err2 = self._setting(settings, 'dest', True)
        return err1 or err2

    async def execute(self, settings: Settings):
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
    def validate(self, settings: Settings):
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

    async def execute(self, settings: Settings):
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

        await self._mkdir_if_not_exists(dest.parent)

        shutil.copy(str(src), str(dest))
        self.worker.output_produced(dest)


@command_executor('remove_tree')
class RemoveTreeCommand(AbstractCommand):
    def validate(self, settings: Settings):
        path, err = self._setting(settings, 'path', True)
        if err:
            return err
        if not path:
            return "'path' may not be empty"

    async def execute(self, settings: Settings):
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


@command_executor('remove_file')
class RemoveFileCommand(AbstractCommand):
    def validate(self, settings: Settings):
        path, err = self._setting(settings, 'path', True)
        if err:
            return err
        if not path:
            return "Parameter 'path' cannot be empty."

    async def execute(self, settings: Settings):
        path = Path(settings['path'])
        if not path.exists():
            msg = 'Path %s does not exist, so not removing.' % path
            self._log.debug(msg)
            await self.worker.register_log(msg)
            return

        if path.is_dir():
            raise CommandExecutionError(f'Path {path} is a directory. Cannot remove with '
                                        'this command; use remove_tree instead.')

        msg = 'Removing file %s' % path
        self._log.info(msg)
        await self.worker.register_log(msg)

        path.unlink()


@attr.s
class AbstractSubprocessCommand(AbstractCommand, abc.ABC):
    readline_timeout = attr.ib(default=SUBPROC_READLINE_TIMEOUT)
    proc = attr.ib(validator=attr.validators.instance_of(asyncio.subprocess.Process),
                   init=False)

    @property
    def subprocess_pid_file(self) -> typing.Optional[pathlib.Path]:
        subprocess_pid_file = self.worker.trunner.subprocess_pid_file
        if not subprocess_pid_file:
            return None
        return pathlib.Path(subprocess_pid_file)

    def validate(self, settings: Settings) -> typing.Optional[str]:
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
            # This could be an indication that a PID file is being written right now
            # (already opened, but the content hasn't been written yet).
            return 'Empty PID file %s, refusing to create new subprocess just to be sure' % pidfile

        pid = int(pid_str)
        self._log.warning('Found PID file %s with pid=%d', pidfile, pid)

        try:
            proc = psutil.Process(pid)
        except psutil.NoSuchProcess:
            self._log.warning('Deleting pidfile %s for stale pid=%d', pidfile, pid)
            pidfile.unlink()
            return None
        return 'Subprocess from %s is still running: %s' % (pidfile, proc)

    async def subprocess(self, args: typing.List[str]):
        cmd_to_log = ' '.join(shlex.quote(s) for s in args)
        self._log.info('Executing %s', cmd_to_log)
        await self.worker.register_log('Executing %s', cmd_to_log)

        line_logger = log.getChild(f'line.{self.identifier}')

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
            try:
                with pid_path.open('x') as pidfile:
                    pidfile.write(str(pid))
            except FileExistsError:
                self._log.error('PID file %r already exists, killing just-spawned process pid=%d',
                                pid_path, pid)
                await self.abort()
                raise
        try:
            assert self.proc.stdout is not None

            while not self.proc.stdout.at_eof():
                try:
                    line_bytes = await asyncio.wait_for(self.proc.stdout.readline(),
                                                        self.readline_timeout)
                except asyncio.TimeoutError:
                    raise CommandExecutionError('Command pid=%d timed out after %i seconds' %
                                                (pid, self.readline_timeout))

                if len(line_bytes) == 0:
                    # EOF received, so let's bail.
                    break

                try:
                    line = line_bytes.decode('utf8')
                except UnicodeDecodeError as ex:
                    await self.abort()
                    raise CommandExecutionError(
                        'Command pid=%d produced non-UTF8 output, aborting: %s' % (pid, ex))

                line = line.rstrip()
                line_logger.debug('Read line pid=%d: %s', pid, line)
                processed_line = await self.process_line(line)
                if processed_line is not None:
                    await self.worker.register_log(processed_line)

            retcode = await self.proc.wait()
            self._log.info('Command %s (pid=%d) stopped with status code %s',
                           cmd_to_log, pid, retcode)

            if retcode:
                raise CommandExecutionError('Command %s (pid=%d) failed with status %s' %
                                            (cmd_to_log, pid, retcode))
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

        if getattr(self, 'proc', None) is None or self.proc == attr.NOTHING:
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
    def validate(self, settings: Settings):
        try:
            cmd = settings['cmd']
        except KeyError:
            return 'Missing "cmd"'

        if not isinstance(cmd, str):
            return '"cmd" must be a string'
        if not cmd:
            return '"cmd" may not be empty'
        return super().validate(settings)

    async def execute(self, settings: Settings):
        await self.subprocess(shlex.split(settings['cmd']))


class AbstractBlenderCommand(AbstractSubprocessCommand):
    re_global_progress = attr.ib(init=False)
    re_time = attr.ib(init=False)
    re_remaining = attr.ib(init=False)
    re_status = attr.ib(init=False)
    re_path_not_found = attr.ib(init=False)
    re_file_saved = attr.ib(init=False)
    _last_activity_time: float = 0.0

    _TIMING_STARTING_BLENDER = 'starting blender'
    _TIMING_LOADING_BLENDFILE = 'loading blendfile'
    _TIMING_RENDERING = 'rendering'

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

        self._last_activity_time = 0.0

    def validate(self, settings: Settings):
        blender_cmd, err = self._setting(settings, 'blender_cmd', True)
        if err:
            return err
        cmd = shlex.split(blender_cmd)
        if not Path(cmd[0]).exists():
            return 'blender_cmd %r does not exist' % cmd[0]
        settings['blender_cmd'] = cmd

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

    async def execute(self, settings: Settings):
        cmd = await self._build_blender_cmd(settings)

        await self.worker.register_task_update(activity='Starting Blender')

        self._timing_checkpoint(self._TIMING_STARTING_BLENDER)
        try:
            await self.subprocess(cmd)
        finally:
            # This writes the final duration to self._timing
            self._timing_checkpoint('')

    async def _build_blender_cmd(self, settings: Settings) -> typing.List[str]:
        filepath = settings['filepath']

        cmd = settings['blender_cmd'][:]
        cmd += [
            '--enable-autoexec',
            '-noaudio',
            '--background',
            filepath,
        ]

        # See if there is an override file to load.
        try:
            index = filepath.lower().rindex('.blend')
        except ValueError:
            # No '.blend' in the filepath. Weird.
            pass
        else:
            override_filename = filepath[:index] + '-overrides.py'
            override_filepath = Path(override_filename)
            if override_filepath.exists():
                msg = f'Override file found in {override_filepath}'
                self._log.info(msg)
                await self.worker.register_log(msg)

                await self.worker.register_log(
                    f'Override file contains:\n{override_filepath.read_text("utf-8")}')

                cmd.extend([
                    '--python-exit-code', '42',
                    '--python', override_filepath.as_posix(),
                ])

        return cmd

    def parse_render_line(self, line: str) -> typing.Optional[dict]:
        """Parses a single line of render progress.

        Returns None if this line does not contain render progress.
        """

        m = self.re_global_progress.search(line)
        if not m:
            return None
        info: typing.Dict[str, typing.Any] = m.groupdict()
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

    async def process_line(self, line: str) -> typing.Optional[str]:
        """Processes the line, returning None to ignore it."""

        # See if there are any warnings about missing files. If so, we update the activity for it.
        if 'Warning: Unable to open' in line or self.re_path_not_found.search(line):
            await self.worker.register_task_update(activity=line)

        if self._last_timing_event == self._TIMING_STARTING_BLENDER and (
                line.startswith('Read blend:') or line.startswith('Info: Read library:')):
            self._timing_checkpoint(self._TIMING_LOADING_BLENDFILE)

        render_info = self.parse_render_line(line)

        if render_info and self._last_timing_event != self._TIMING_RENDERING:
            self._timing_checkpoint(self._TIMING_RENDERING)

        now = time.time()
        # Only update render info every this many seconds, and not for every line Blender produces.
        if render_info and now - self._last_activity_time < 30:
            self._last_activity_time = now
            # Render progress. Not interesting to log all of them, but we do use
            # them to update the render progress.
            # TODO: For now we return this as a string, but at some point we may want
            # to consider this as a subdocument.
            if 'remaining_sec' in render_info:
                fmt = 'Fra:{fra} Mem:{mem} | Time:{time_sec} | Remaining:{remaining_sec} | {status}'
                activity = fmt.format(**render_info)
            else:
                # self._log.debug('Unable to find remaining time in line: %s', line)
                activity = line
            await self.worker.register_task_update(activity=activity)

        # See if this line logs the saving of a file.
        m = self.re_file_saved.search(line)
        if m:
            self.worker.output_produced(m.group('filename'))

        # Not a render progress line; just log it for now.
        return 'pid=%d > %s' % (self.proc.pid, line)


@command_executor('blender_render')
class BlenderRenderCommand(AbstractBlenderCommand):

    def validate(self, settings: Settings):
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

        return super().validate(settings)

    async def _build_blender_cmd(self, settings) -> typing.List[str]:
        cmd = await super()._build_blender_cmd(settings)

        if settings.get('python_expr'):
            cmd.extend(['--python-expr', settings['python_expr']])
        if settings.get('render_output'):
            cmd.extend(['--render-output', settings['render_output']])
        if settings.get('format'):
            cmd.extend(['--render-format', settings['format']])
        if settings.get('frames'):
            cmd.extend(['--render-frame', settings['frames']])
        return cmd


@command_executor('blender_render_progressive')
class BlenderRenderProgressiveCommand(BlenderRenderCommand):
    def validate(self, settings: Settings):
        if 'cycles_chunk' in settings:
            return '"cycles_chunk" is an obsolete setting. Recreate the job using Flamenco ' \
                   'Server 2.2 or newer, or use an older Worker.'

        err = super().validate(settings)
        if err:
            return err

        cycles_num_chunks, err = self._setting(settings, 'cycles_num_chunks', True, int)
        if err:
            return err
        if cycles_num_chunks < 1:
            return '"cycles_num_chunks" must be a positive integer'

        cycles_chunk_start, err = self._setting(settings, 'cycles_chunk_start', True, int)
        if err:
            return err
        if cycles_chunk_start < 1:
            return '"cycles_chunk_start" must be a positive integer'

        cycles_chunk_end, err = self._setting(settings, 'cycles_chunk_end', True, int)
        if err:
            return err
        if cycles_chunk_end < 1:
            return '"cycles_chunk_end" must be a positive integer'

    async def _build_blender_cmd(self, settings) -> typing.List[str]:
        cmd = await super()._build_blender_cmd(settings)

        return cmd + [
            '--',
            '--cycles-resumable-num-chunks', str(settings['cycles_num_chunks']),
            '--cycles-resumable-start-chunk', str(settings['cycles_chunk_start']),
            '--cycles-resumable-end-chunk', str(settings['cycles_chunk_end']),
        ]


@command_executor('merge_progressive_renders')
class MergeProgressiveRendersCommand(AbstractSubprocessCommand):
    script_template = MERGE_EXR_PYTHON

    def validate(self, settings: Settings):
        blender_cmd, err = self._setting(settings, 'blender_cmd', True)
        if err:
            return err
        cmd = shlex.split(blender_cmd)
        if not Path(cmd[0]).exists():
            return 'blender_cmd %r does not exist' % cmd[0]
        settings['blender_cmd'] = cmd

        input1, err = self._setting(settings, 'input1', True, str)
        if err:
            return err
        if '"' in input1:
            return 'Double quotes are not allowed in filenames: %r' % input1
        if not Path(input1).exists():
            return 'Input 1 %r does not exist' % input1
        input2, err = self._setting(settings, 'input2', True, str)
        if err:
            return err
        if '"' in input2:
            return 'Double quotes are not allowed in filenames: %r' % input2
        if not Path(input2).exists():
            return 'Input 2 %r does not exist' % input2

        output, err = self._setting(settings, 'output', True, str)
        if err:
            return err
        if '"' in output:
            return 'Double quotes are not allowed in filenames: %r' % output

        settings['input1'] = Path(input1).as_posix()
        settings['input2'] = Path(input2).as_posix()
        settings['output'] = Path(output).as_posix()

        _, err = self._setting(settings, 'weight1', True, int)
        if err:
            return err

        _, err = self._setting(settings, 'weight2', True, int)
        if err:
            return err

        return super().validate(settings)

    async def execute(self, settings: Settings):
        cmd = self._base_blender_cli(settings)

        # set up node properties and render settings.
        output = Path(settings['output'])
        await self._mkdir_if_not_exists(output.parent)

        with tempfile.TemporaryDirectory(dir=str(output.parent)) as tmpdir:
            tmppath = Path(tmpdir)
            assert tmppath.exists()

            settings['tmpdir'] = tmppath.as_posix()
            cmd += [
                '--python-expr', self.script_template % settings
            ]

            await self.worker.register_task_update(activity='Starting Blender to merge EXR files')
            await self.subprocess(cmd)

            # move output files into the correct spot.
            await self.move(tmppath / 'merged0001.exr', output)

        # See if this line logs the saving of a file.
        self.worker.output_produced(output)

    def _base_blender_cli(self, settings):
        blendpath = Path(__file__).parent / 'resources/merge-exr.blend'

        cmd = settings['blender_cmd'] + [
            '--factory-startup',
            '--enable-autoexec',
            '-noaudio',
            '--background',
            blendpath.as_posix(),
            '--python-exit-code', '47',
        ]
        return cmd

    async def move(self, src: Path, dst: Path):
        """Moves a file to another location."""

        self._log.info('Moving %s to %s', src, dst)

        assert src.exists()
        assert src.is_file()
        assert not dst.exists() or dst.is_file()
        assert dst.exists() or dst.parent.exists()

        await self.worker.register_log('Moving %s to %s', src, dst)
        shutil.move(str(src), str(dst))


@command_executor('merge_progressive_render_sequence')
class MergeProgressiveRenderSequenceCommand(MergeProgressiveRendersCommand):
    script_template = MERGE_EXR_SEQUENCE_PYTHON

    def validate(self, settings: Settings):
        err = super().validate(settings)
        if err:
            return err

        if '##' not in settings['output']:
            return 'Output filename should contain at least two "##" marks'

        _, err = self._setting(settings, 'frame_start', True, int)
        if err:
            return err

        _, err = self._setting(settings, 'frame_end', True, int)
        if err:
            return err

    async def execute(self, settings: Settings):
        cmd = self._base_blender_cli(settings)

        # set up node properties and render settings.
        output = Path(settings['output'])
        await self._mkdir_if_not_exists(output.parent)

        cmd += [
            '--python-expr', self.script_template % settings
        ]
        await self.worker.register_task_update(activity='Starting Blender to merge EXR sequence')
        await self.subprocess(cmd)

        as_glob = _hashes_to_glob(output)
        for fpath in as_glob.parent.glob(as_glob.name):
            self.worker.output_produced(fpath)


# TODO(Sybren): maybe subclass AbstractBlenderCommand instead?
@command_executor('blender_render_audio')
class BlenderRenderAudioCommand(BlenderRenderCommand):
    def validate(self, settings: Settings):
        err = super().validate(settings)
        if err:
            return err

        render_output, err = self._setting(settings, 'render_output', True)
        if err:
            return err
        if not render_output:
            return "'render_output' is a required setting"

        _, err = self._setting(settings, 'frame_start', False, int)
        if err:
            return err
        _, err = self._setting(settings, 'frame_end', False, int)
        if err:
            return err

    async def _build_blender_cmd(self, settings: Settings) -> typing.List[str]:
        frame_start = settings.get('frame_start')
        frame_end = settings.get('frame_end')
        render_output = settings.get('render_output')

        py_lines = [
            "import bpy"
        ]
        if frame_start is not None:
            py_lines.append(f'bpy.context.scene.frame_start = {frame_start}')
        if frame_end is not None:
            py_lines.append(f'bpy.context.scene.frame_end = {frame_end}')

        py_lines.append(f"bpy.ops.sound.mixdown(filepath={render_output!r}, "
                        f"codec='FLAC', container='FLAC', "
                        f"accuracy=128)")
        py_lines.append('bpy.ops.wm.quit_blender()')
        py_script = '\n'.join(py_lines)

        return [
            *settings['blender_cmd'],
            '--enable-autoexec',
            '-noaudio',
            '--background',
            settings['filepath'],
            '--python-exit-code', '47',
            '--python-expr', py_script
        ]


@command_executor('exr_sequence_to_jpeg')
class EXRSequenceToJPEGCommand(BlenderRenderCommand):
    """Convert an EXR sequence to JPEG files.

    This assumes the EXR files are named '{frame number}.exr', where the
    frame number may have any number of leading zeroes.
    """
    pyscript = Path(__file__).parent / 'resources/exr_sequence_to_jpeg.py'

    def validate(self, settings: Settings) -> typing.Optional[str]:
        if not self.pyscript.exists():
            raise FileNotFoundError(f'Resource script {self.pyscript} cannot be found')

        exr_glob, err = self._setting(settings, 'exr_glob', False)
        if err:
            return err

        # Only for backward compatibility. Should not be used.
        exr_directory, err = self._setting(settings, 'exr_directory', False)
        if err:
            return err

        if not exr_glob and not exr_directory:
            return '"exr_glob" may not be empty'
        if exr_glob and exr_directory:
            # Normally I would say 'use either one or the other, not both', but
            # in this case 'exr_directory' is deprecated and shouldn't be used.
            return 'Just pass "exr_glob", do not use "exr_directory"'

        if exr_directory:
            settings['exr_glob'] = str(Path(exr_directory) / '*.exr')

        output_pattern, err = self._setting(settings, 'output_pattern', False,
                                            default='preview-######.jpg')
        if not output_pattern:
            return '"output_pattern" may not be empty'
        return super().validate(settings)

    async def _build_blender_cmd(self, settings) -> typing.List[str]:
        cmd = await super()._build_blender_cmd(settings)

        return cmd + [
            '--python-exit-code', '32',
            '--python', str(self.pyscript),
            '--',
            '--exr-glob', settings['exr_glob'],
            '--output-pattern', settings['output_pattern'],
        ]


class AbstractFFmpegCommand(AbstractSubprocessCommand, abc.ABC):
    index_file: typing.Optional[pathlib.Path] = None

    def validate(self, settings: Settings) -> typing.Optional[str]:
        # Check that FFmpeg can be found and shlex-split the string.
        ffmpeg_cmd, err = self._setting(settings, 'ffmpeg_cmd', is_required=False, default='ffmpeg')
        if err:
            return err

        cmd = shlex.split(ffmpeg_cmd)
        executable_path: typing.Optional[str] = shutil.which(cmd[0])
        if not executable_path:
            return f'FFmpeg command {ffmpeg_cmd!r} not found on $PATH'
        settings['ffmpeg_cmd'] = cmd
        self._log.debug('Found FFmpeg command at %r', executable_path)
        return None

    async def execute(self, settings: Settings) -> None:
        cmd = self._build_ffmpeg_command(settings)
        await self.subprocess(cmd)

        if self.index_file is not None and self.index_file.exists():
            try:
                self.index_file.unlink()
            except IOError:
                msg = f'unable to unlink file {self.index_file}, ignoring'
                await self.worker.register_log(msg)
                self._log.warning(msg)

    def _build_ffmpeg_command(self, settings: Settings) -> typing.List[str]:
        assert isinstance(settings['ffmpeg_cmd'], list), \
            'run validate() before _build_ffmpeg_command'
        cmd = [
            *settings['ffmpeg_cmd'],
            *self.ffmpeg_args(settings),
        ]
        return cmd

    @abc.abstractmethod
    def ffmpeg_args(self, settings: Settings) -> typing.List[str]:
        """Construct the FFmpeg arguments to execute.

        Does not need to include the FFmpeg command itself, just
        its arguments.
        """
        pass

    def create_index_file(self, input_files: pathlib.Path) -> pathlib.Path:
        """Construct a list of filenames for ffmpeg to process.

        The filenames are stored in a file 'ffmpeg-input.txt' that sits in the
        same directory as the input files.

        It is assumed that 'input_files' contains a glob pattern in the file
        name, and not in any directory parts.

        The index file will be deleted after successful execution of the ffmpeg
        command.
        """

        # The index file needs to sit next to the input files, as
        # ffmpeg checks for 'unsafe paths'.
        self.index_file = input_files.absolute().with_name('ffmpeg-input.txt')

        with self.index_file.open('w') as outfile:
            for file_path in sorted(input_files.parent.glob(input_files.name)):
                escaped = str(file_path.name).replace("'", "\\'")
                print("file '%s'" % escaped, file=outfile)

        return self.index_file


@command_executor('create_video')
class CreateVideoCommand(AbstractFFmpegCommand):
    """Create a video from individual frames.

    Requires FFmpeg to be installed and available with the 'ffmpeg' command.
    """

    codec_video = 'h264'

    # Select some settings that are useful for scrubbing through the video.
    constant_rate_factor = 23
    keyframe_interval = 18  # GOP size
    max_b_frames: typing.Optional[int] = 0

    def validate(self, settings: Settings) -> typing.Optional[str]:
        err = super().validate(settings)
        if err:
            return err

        # Check that we know our input and output image files.
        input_files, err = self._setting(settings, 'input_files', is_required=True)
        if err:
            return err
        self._log.debug('Input files: %s', input_files)
        output_file, err = self._setting(settings, 'output_file', is_required=True)
        if err:
            return err
        self._log.debug('Output file: %s', output_file)

        fps, err = self._setting(settings, 'fps', is_required=True, valtype=(int, float))
        if err:
            return err
        self._log.debug('Frame rate: %r fps', fps)
        return None

    def ffmpeg_args(self, settings: Settings) -> typing.List[str]:
        input_files = Path(settings['input_files'])

        args = [
            '-r', str(settings['fps']),
        ]

        if platform.system() == 'Windows':
            # FFMpeg on Windows doesn't support globbing, so we have to do
            # that in Python instead.
            index_file = self.create_index_file(input_files)
            args += [
                '-f', 'concat',
                '-i', index_file.as_posix(),
            ]
        else:
            args += [
                '-pattern_type', 'glob',
                '-i', input_files.as_posix(),
            ]

        args += [
            '-c:v', self.codec_video,
            '-crf', str(self.constant_rate_factor),
            '-g', str(self.keyframe_interval),
            '-vf', 'pad=ceil(iw/2)*2:ceil(ih/2)*2',
            '-y',
        ]
        if self.max_b_frames is not None:
            args.extend(['-bf', str(self.max_b_frames)])
        args += [
            settings['output_file']
        ]
        return args


@command_executor('concatenate_videos')
class ConcatenateVideosCommand(AbstractFFmpegCommand):
    """Create a video by concatenating other videos.

    Requires FFmpeg to be installed and available with the 'ffmpeg' command.
    """

    def validate(self, settings: Settings) -> typing.Optional[str]:
        err = super().validate(settings)
        if err:
            return err

        # Check that we know our input and output image files.
        input_files, err = self._setting(settings, 'input_files', is_required=True)
        if err:
            return err
        self._log.debug('Input files: %s', input_files)
        output_file, err = self._setting(settings, 'output_file', is_required=True)
        if err:
            return err
        self._log.debug('Output file: %s', output_file)

        return None

    def ffmpeg_args(self, settings: Settings) -> typing.List[str]:
        index_file = self.create_index_file(Path(settings['input_files']))

        output_file = Path(settings['output_file'])
        self._log.debug('Output file: %s', output_file)

        args = [
            '-f', 'concat',
            '-i', index_file.as_posix(),
            '-c', 'copy',
            '-y',
            output_file.as_posix(),
        ]
        return args


@command_executor('mux_audio')
class MuxAudioCommand(AbstractFFmpegCommand):

    def validate(self, settings: Settings) -> typing.Optional[str]:
        err = super().validate(settings)
        if err:
            return err

        # Check that we know our input and output image files.
        audio_file, err = self._setting(settings, 'audio_file', is_required=True)
        if err:
            return err
        if not Path(audio_file).exists():
            return f'Audio file {audio_file} does not exist'
        self._log.debug('Audio file: %s', audio_file)

        video_file, err = self._setting(settings, 'video_file', is_required=True)
        if err:
            return err
        if not Path(video_file).exists():
            return f'Video file {video_file} does not exist'
        self._log.debug('Video file: %s', video_file)

        output_file, err = self._setting(settings, 'output_file', is_required=True)
        if err:
            return err
        self._log.debug('Output file: %s', output_file)

        return None

    def ffmpeg_args(self, settings: Settings) -> typing.List[str]:
        audio_file = Path(settings['audio_file']).absolute()
        video_file = Path(settings['video_file']).absolute()
        output_file = Path(settings['output_file']).absolute()

        args = [
            '-i', str(audio_file),
            '-i', str(video_file),
            '-c', 'copy',
            '-y',
            str(output_file),
        ]
        return args


@command_executor('encode_audio')
class EncodeAudioCommand(AbstractFFmpegCommand):

    def validate(self, settings: Settings) -> typing.Optional[str]:
        err = super().validate(settings)
        if err:
            return err

        # Check that we know our input and output image files.
        input_file, err = self._setting(settings, 'input_file', is_required=True)
        if err:
            return err
        if not Path(input_file).exists():
            return f'Audio file {input_file} does not exist'
        self._log.debug('Audio file: %s', input_file)

        output_file, err = self._setting(settings, 'output_file', is_required=True)
        if err:
            return err
        self._log.debug('Output file: %s', output_file)

        _, err = self._setting(settings, 'bitrate', is_required=True)
        if err:
            return err
        _, err = self._setting(settings, 'codec', is_required=True)
        if err:
            return err
        return None

    def ffmpeg_args(self, settings: Settings) -> typing.List[str]:
        input_file = Path(settings['input_file']).absolute()
        output_file = Path(settings['output_file']).absolute()

        args = [
            '-i', str(input_file),
            '-c:a', settings['codec'],
            '-b:a', settings['bitrate'],
            '-y',
            str(output_file),
        ]
        return args


@command_executor('move_with_counter')
class MoveWithCounterCommand(AbstractCommand):
    # Split '2018_12_06-spring.mkv' into a '2018_12_06' prefix and '-spring.mkv' suffix.
    filename_parts = re.compile(r'(?P<prefix>^[0-9_]+)(?P<suffix>.*)$')

    def validate(self, settings: Settings):
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

    async def execute(self, settings: Settings):
        src = Path(settings['src'])
        if not src.exists():
            raise CommandExecutionError('Path %s does not exist, unable to move' % src)

        dest = Path(settings['dest'])
        fname_parts = self.filename_parts.match(dest.name)
        if fname_parts:
            prefix = fname_parts.group('prefix') + '_'
            suffix = fname_parts.group('suffix')
        else:
            prefix = dest.stem + '_'
            suffix = dest.suffix
        self._log.debug('Adding counter to output name between %r and %r', prefix, suffix)
        dest = _numbered_path(dest.parent, prefix, suffix)

        self._log.info('Moving %s to %s', src, dest)
        await self.worker.register_log('%s: Moving %s to %s', self.command_name, src, dest)
        await self._mkdir_if_not_exists(dest.parent)

        shutil.move(str(src), str(dest))
        self.worker.output_produced(dest)


@command_executor('create_python_file')
class CreatePythonFile(AbstractCommand):
    def validate(self, settings: Settings):
        filepath, err = self._setting(settings, 'filepath', True)
        if err:
            return err
        if not filepath:
            return 'filepath may not be empty'
        if not filepath.endswith('.py'):
            return 'filepath must end in .py'

        dest, err = self._setting(settings, 'contents', True)
        if err:
            return err

    async def execute(self, settings: Settings):
        filepath = Path(settings['filepath'])
        await self._mkdir_if_not_exists(filepath.parent)

        if filepath.exists():
            msg = f'Overwriting Python file {filepath}'
        else:
            msg = f'Creating Python file {filepath}'

        self._log.info(msg)
        await self.worker.register_log('%s: %s', self.command_name, msg)
        await self.worker.register_log('%s: contents:\n%s', self.command_name, settings['contents'])

        filepath.write_text(settings['contents'], encoding='utf-8')
