=============
Release 0.223
=============

General Changes
---------------

* Fix a bug where a connector may not optimize query plan if the corresponding
  plugin is installed after the server is started.
* Fix incorrect optimizations that might remove necessary CAST (:pr:`13117`).
* Improve coordinator CPU utilization by introducing an option to use long
  polling for task information update.
  ``experimental.task.info-update-refresh-max-wait`` is the configuration
  property to enable this.
* Add support for multibyte characters in :func:`date_format`.

Web UI Changes
--------------

* Fix splits timeline view.

Kafka Changes
-------------

* Update Kafka version from 0.8.2.2 to 1.1.1.
