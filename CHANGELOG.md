# Flamenco Worker changelog

This file logs the changes that are actually interesting to users (new features,
changed functionality, fixed bugs).

## Version 2.1.1 (in development)

- Always log the version of Flamenco Worker.
- Include missing merge-exr.blend, required for progressive rendering, in the distribution bundle.
- Include `exr-merge` task type in default configuration, which is required for progressive
  rendering.
- Prevent outgoing queue saturation by not fetching a new task when the queue is too large.


## Version 2.1.0 (2018-01-04)

- Python 3.5.4 is required as minimum Python version.
- Worker can now be told to go to sleep by the Manager. In that case task execution
  stops (because /may-i-run/{task-id} returns 'no') and new tasks are no longer given.
  This is done via a request to change its internal state. This state change must be
  acknowleged by the Worker before new tasks will be given.
- Fixed sending task status updates after the task may no longer be run.
- Worker goes to sleep when receiving signal USR1 and wakes up after signal USR2.
  This is only supported on POSIX platforms that have those signals.
- Worker can be told to shut down by the Manager. The environment (for example systemd
  on Linux) is responsible for restarting Flamenco Worker after such a shutdown.
- Added `--version` CLI option to show the version of Flamenco Worker and quit.
- Added `--single` or `-1` CLI option to shut down the Worker after executing a single task.
- Added `--test` or `-t` CLI option to start in testing mode. See Flamenco documentation
  for more details. Requires Flamenco Manager 2.1.0+.
- Added support for passing Python scripts to Blender in task definitions.
- When a blend file does not exist when a render command starts, waits 5 seconds and test
  again. This allows for some slight network lag when the job storage resides on a networked
  file system.


## Version 2.0.8 (released 2017-09-07)

- Fixed parsing of `--config` CLI param on Python 3.5
- Added `--debug` CLI parameter to easily enable debug logging without having
  to edit `flamenco-worker.cfg`.
- Only fail UPnP/SSDP discovery when it fails to send on both IPv4 and IPv6.
- Creating distribution files using [PyInstaller](http://www.pyinstaller.org/).
- Fixed UPnP/SSDP discovery issues on Windows.


## Version 2.0.7 (released 2017-07-04)

- Use UPnP/SSDP to automatically find Manager when manager_url is empty.
  This is now also the new default, since we can't provide a sane default URL anyway.
  Requires Flamenco Manager 2.0.13 or newer.
- Fixed Windows incompatibilities.


## Version 2.0.6 (released 2017-06-23)

- Fixed incompatibility with attrs version 17.1+.
- Added `--reregister` CLI option to re-register this worker at its Manager.
  WARNING: this can cause duplicate worker information in the Manager's database.


## Version 2.0.5 (released 2017-05-09)

- Vacuum SQLite database at startup.
- Removed `--factory-startup` from the default Blender command. This can be passed
  to the Worker using the {blender} variable definition instead.


## Version 2.0.4 (released 2017-05-09)

- Fixed bug in writing ~/.flamenco-worker.cfg


## Version 2.0.3 (released 2017-04-07)

- Made the `flamenco-worker.cfg` file mandatory, as this makes debugging configuration
  issues easier. When the file does not exist, the absolute path of its expected
  location is logged.


## Version 2.0.2 (released 2017-04-07)

- Added support for task types. Workers only get tasks of the types they support.
  This also adds signing on at every start, to send the current hostname as nickname,
  and the configured list of task types, to the Manager. Requires Manager version
  2.0.4 or newer.


## Version 2.0.1 (released 2017-03-31)

- Registers rendered and copied files with the Manager, so that they can be
  shown as "latest render".


## Version 2.0 (released 2017-03-29)

- First release of Pillar-based Flamenco, including this Worker.
