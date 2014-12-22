============
Release 0.78
============

ARRAY and MAP Types in Hive Connector
-------------------------------------

The Hive connector now returns arrays and maps instead of json encoded strings,
for columns whose underlying type is array or map. Please note that this is a backwards
incompatible change, and the :doc:`/functions/json` will no longer work on these columns,
unless you :func:`cast` them to the ``json`` type.

Session Properties
------------------

The Presto session can now contain properties, which can be used by the Presto
engine or connectors to customize the query execution. There is a separate
namespace for the Presto engine and each catalog. A property for a catalog is
simplify prefixed with the catalog name followed by ``.`` (dot). A connector
can retrieve the properties for the catalog using
``ConnectorSession.getProperties()``.

Session properties can be set using the ``--session`` command line argument to
the Presto CLI. For example:

.. code-block:: none

    presto-cli --session color=red --session size=large

For JDBC, the properties can be set by unwrapping the ``Connection`` as follows:

.. code-block:: java

    connection.unwrap(PrestoConnection.class).setSessionProperty("name", "value");

.. note::
    This feature is a work in progress and will change in a future release.
    Specifically, we are planning to require preregistration of properties so
    the user can list available session properties and so the engine can verify
    property values. Additionally, the Presto grammar will be extended to
    allow setting properties via a query.

Hive Changes
------------

* Add ``storage_format`` session property to override format used for creating tables.
* Add write support for ``VARBINARY``, ``DATE`` and ``TIMESTAMP``.
* Add support for partition keys of type ``TIMESTAMP``.
* Add support for partition keys with null values (``__HIVE_DEFAULT_PARTITION__``).
* Fix ``hive.storage-format`` option (see :doc:`release-0.76`).

General Changes
---------------

* Fix expression optimizer, so that it runs in linear time instead of exponential time.
* Add :func:`cardinality` for maps.
* Fix race condition in SqlTask creation which can cause queries to hang.
* Fix ``node-scheduler.multiple-tasks-per-node-enabled`` option.
* Fix an exception when planning a query with a UNION under a JOIN.
