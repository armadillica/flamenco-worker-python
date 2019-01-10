# Flamenco Worker

This is the Flamenco Worker implemented in Python 3.

Author: Sybren A. Stüvel <sybren@blender.studio>

## Installation

- Make sure you have Flamenco Manager up and running.
- Install [FFmpeg](https://ffmpeg.org/) and make sure the `ffmpeg` binary is on `$PATH`.
- Install Flamenco Worker in one of two ways:
    - If you have a distributable zip file (see
      [Packaging for distribution](#packaging-for-distribution)) unzip it, `cd` into it,
      then run `./flamenco-worker` (or `flamenco-worker.exe` on Windows).
    - If you have a copy of the source files, run `pipenv install` then run `flamenco-worker`.
      This requires Python 3.7 or newer.


## Upgrading

Upgrading from a previous version of Flamenco Worker is simple:

- Unpack the package you downloaded from [flamenco.io](https://flamenco.io/download).
- Copy the `flamenco-worker.cfg` and `flamenco-worker.db` files from your previous installation.
- Start Flamenco Worker.


## Configuration

Configuration is read from three locations:

- A hard-coded default in the Python source code.
- `flamenco-worker.cfg` in the current working directory, i.e. the directory that your shell
  is in when you invoke the `flamenco-worker` command.
- `$HOME/.flamenco-worker.cfg`; this file is optional.

The configuration files should be in INI format, as specified by the
[configparser documentation](https://docs.python.org/3/library/configparser.html)


### Configuration contents:

All configuration keys should be placed in the `[flamenco-worker]` section of the
config files. At least take a look at:

- `manager_url`: Flamenco Manager URL. Leave blank to auto-discover Flamenco Manager
  on your network using UPnP/SSDP.
- `task_types`: Space-separated list of task types this worker may execute.
- `task_update_queue_db`: filename of the SQLite3 database holding the queue of task
  updates to be sent to the Master.

These configuration keys are also required, but are created automatically in
`$HOME/.flamenco-worker.cfg` when they don't exist yet:

- `worker_id`: ID of the worker, handed out by the Manager upon registration (see
  Registration below) and used for authentication with the Manager.
- `worker_secret`: Secret key of the worker, generated by the Worker and given to the
  Manager upon registration and authentication.


### TODO

- Certain settings are currently only settable by editing constants in the Python source code.
  It might be nice to read them from the config file too, at some point.


## Invocation

Install using `pip3 install -e .` for development, or `python3 setup.py install` for production.
This creates a command `flamenco-worker`, which can be run with `--help` to obtain
a list of possible CLI arguments.


## Registration

If the configuration file does not contain both a `worker_id` and `worker_secret`, at startup
the worker will attempt to register itself at the Master.
Once registered via a POST to the manager's `/register-worker` endpoint, the `worker_id` and
`worker_secret` will be written to `$HOME/.flamenco-worker.cfg`

## Task fetch & execution

1. A task is obtained by the FlamencoWorker from the manager via a POST to its `/task` endpoint.
   If this fails (for example due to a connection error), the worker will retry every few seconds
   until a task fetch is succesful.
2. The task is given to a TaskRunner object.
3. The TaskRunner object iterates over the commands and executes them.
4. At any time, the FlamencoWorker can be called upon to register activities and log lines,
   and forward them to the Manager. These updates are queued in a SQLite3 database, such that
   task execution isn't interrupted when the Manager cannot be reached.
5. A separate coroutine of TaskUpdateQueue fetches updates from the queue, and forwards them to
   the Master, using a POST to its `/tasks/{task-id}/update` endpoint.
   **TODO:** the response to this endpoint may indicate a request to abort the currently running
   task. This should be implemented.


## Shutdown

Pressing [CTRL]+[C] will cause a clean shutdown of the worker. If there is a task currently running,
it will be aborted without changing its status. Any pending task updates are sent to the Manager,
and then the Manager's `/sign-off` URL is POSTed to, to indicate a clean shutdown of the worker. Any
active task that is still assigned to the worker is given status "claimed-by-manager" so that it can
be re-activated by another worker.


## Systemd integration

To run Flamenco Worker as a systemd-managed service, copy `flamenco-worker.service` to
`/etc/systemd/system/flamenco-worker.service`, then run `systemctl daemon-reload`.

After installation of this service, `systemctl {start,stop,status,restart} flamenco-worker`
commands can be used to manage it. To ensure that the Flamenco Worker starts at system boot,
use `systemctl enable flamenco-worker`.


## Signals

Flamenco Worker responds to the following POSIX signals:

- `SIGINT`, `SIGTERM`: performs a clean shutdown, as described in the Shutdown section above.
- `SIGUSR1`: logs the currently scheduled asyncio tasks.


## Packaging for distribution

First run `pipenv install --dev` to fetch developer dependencies. On Windows,
download the [Microsoft Visual C++ 2010 Redistributable Package](https://www.microsoft.com/en-us/download/details.aspx?id=13523).
On Ubuntu/Debian, make sure you have the 'binutils' package installed.

Run `mkdistfile.py` to create a distributable archive (`.zip` on Windows, `.tar.gz` on Linux and
macOS) containing a runnable Flamenco Worker. This build doesn't require installing Python or any
dependencies, and can be directly run on a target machine of the same OS.

NOTE: pyinstaller must be run on each supported platform, to create files for that platform. It
cannot cross-build.
