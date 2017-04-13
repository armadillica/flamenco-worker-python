# Flamenco Worker changelog

This file logs the changes that are actually interesting to users (new features,
changed functionality, fixed bugs).


## Version 2.0.3 (under development)

- Added optional creation and removal of a PID file. This only happens when the `pid`
  configuration option is given, pointing to the location of the PID file.


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
