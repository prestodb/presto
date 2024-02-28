==============
Atop connector
==============

The Atop connector supports reading disk utilization statistics from the `Atop <https://www.atoptool.nl/>`_
(Advanced System and Process Monitor) Linux server performance analysis tool.

Requirements
------------

In order to use this connector, the host on which the Presto worker is running
needs to have the ``atop`` tool installed locally.

Connector configuration
-----------------------

The connector can read disk utilization statistics on the Presto cluster.
Create a catalog properties file that specifies the Atop connector by
setting the ``connector.name`` to ``atop``.

For example, create the file ``etc/catalog/system_monitor.properties``
and replace the connector properties as appropriate for your setup:

.. code-block:: text

    connector.name=atop
    atop.executable-path=/usr/bin/atop

Configuration properties
------------------------

.. list-table::
  :widths: 42, 18, 5, 35
  :header-rows: 1

  * - Property name
    - Default value
    - Required
    - Description
  * - ``atop.concurrent-readers-per-node``
    - ``1``
    - Yes
    - The number of concurrent read operations allowed per node.
  * - ``atop.executable-path``
    - (none)
    - Yes
    - The file path on the local file system for the ``atop`` utility.
  * - ``atop.executable-read-timeout``
    - ``1ms``
    - Yes
    - The timeout when reading from the atop process.
  * - ``atop.max-history-days``
    - ``30``
    - Yes
    - The maximum number of days in the past to take into account for statistics.
  * - ``atop.security``
    - ``ALLOW_ALL``
    - Yes
    - The :doc:`access control </security/built-in-system-access-control>` for the connector.
  * - ``atop.time-zone``
    - System default
    - Yes
    - The time zone identifier in which the atop data is collected. Generally the timezone of the host.
      Sample time zone identifiers: ``Europe/Vienna``, ``+0100``, ``UTC``.

Usage
-----

The Atop connector provides a ``default`` schema.

The tables exposed by this connector can be retrieved by running ``SHOW TABLES``::

    SHOW TABLES FROM system_monitor.default;

.. code-block:: text

      Table
    ---------
     disks
     reboots
    (2 rows)


The ``disks`` table offers disk utilization statistics recorded on the Presto node.

.. list-table:: Disks columns
  :widths: 30, 30, 40
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``host_ip``
    - ``varchar``
    - Presto worker IP
  * - ``start_time``
    - ``timestamp(3) with time zone``
    - Interval start time for the statistics
  * - ``end_time``
    - ``timestamp(3) with time zone``
    - Interval end time for the statistics
  * - ``device_name``
    - ``varchar``
    - Logical volume/hard disk name
  * - ``utilization_percent``
    - ``double``
    - The percentage of time the unit was busy handling requests
  * - ``io_time``
    - ``interval day to second``
    - Time spent for I/O
  * - ``read_requests``
    - ``bigint``
    - Number of reads issued
  * - ``sectors_read``
    - ``bigint``
    - Number of sectors transferred for reads
  * - ``write_requests``
    - ``bigint``
    - Number of writes issued
  * - ``sectors_written``
    - ``bigint``
    - Number of sectors transferred for write

The ``reboots`` table offers information about the system reboots performed on the Presto node.

.. list-table:: Reboots columns
  :widths: 30, 30, 40
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``host_ip``
    - ``varchar``
    - Presto worker IP
  * - ``power_on_time``
    - ``timestamp(3) with time zone``
    - The boot/reboot timestamp
