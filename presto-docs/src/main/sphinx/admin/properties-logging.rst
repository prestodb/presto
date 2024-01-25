==================
Logging Properties
==================

``log.path``
^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``var/log/server.log``

The path to the log file used by Presto. The path is relative to the data
directory, configured by the launcher script.

``log.max-history``
^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``30``

The maximum number of general application log files to use, before log
rotation replaces old content.

``log.max-size``
^^^^^^^^^^^^^^^^
* **Type:** ``data size``
* **Default value:** ``100MB``

The maximum file size for the general application log file.

``http-server.log.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Flag to enable or disable logging for the HTTP server.

``http-server.log.compression.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Flag to enable or disable compression of the log files of the HTTP server.

``http-server.log.path``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``var/log/http-request.log``

The path to the log file used by the HTTP server. The path is relative to
the data directory, configured by the launcher script as detailed in
:ref:`running_presto`.

``http-server.log.max-history``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``15``

The maximum number of log files for the HTTP server to use, before
log rotation replaces old content.

``http-server.log.max-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``data size``
* **Default value:** ``100MB``

The maximum file size for the log file of the HTTP server.
