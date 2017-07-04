# Flamenco Worker changelog

This file logs the changes that are actually interesting to users (new features,
changed functionality, fixed bugs).

## Version 2.0.7 (in development)

- Use UPnP/SSDP to automatically find Manager when manager_url is empty.
  This is now also the new default, since we can't provide a sane default URL anyway.
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
