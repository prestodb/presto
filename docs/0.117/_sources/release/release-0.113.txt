=============
Release 0.113
=============

.. warning::

    The ORC reader in the Hive connector is broken in this release.

Cluster Resource Management
---------------------------

The cluster resource manager announced in :doc:`/release/release-0.103` is now enabled by default.
You can disable it with the ``experimental.cluster-memory-manager-enabled`` flag.
Memory limits can now be configured via ``query.max-memory`` which controls the total distributed
memory a query may use and ``query.max-memory-per-node`` which limits the amount
of memory a query may use on any one node. On each worker, the
``resources.reserved-system-memory`` config property controls how much memory is reserved
for internal Presto data structures and temporary allocations.

Session Properties
------------------

All session properties now have a SQL type, default value and description.  The
value for :doc:`/sql/set-session` can now be any constant expression, and the
:doc:`/sql/show-session` command prints the current effective value and default
value for all session properties.

This type safety extends to the :doc:`SPI </develop/spi-overview>` where properties
can be validated and converted to any Java type using
``SessionPropertyMetadata``. For an example, see ``HiveSessionProperties``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector that uses session properties, you will need
    to update your code to declare the properties in the ``Connector``
    implementation and callers of ``ConnectorSession.getProperty()`` will now
    need the expected Java type of the property.

General Changes
---------------

* Allow using any type with value window functions :func:`first_value`,
  :func:`last_value`, :func:`nth_value`, :func:`lead` and :func:`lag`.
* Add :func:`element_at` function.
* Add :func:`url_encode` and :func:`url_decode` functions.
* :func:`concat` now allows arbitrary number of arguments.
* Fix JMX connector. In the previous release it always returned zero rows.
* Fix handling of literal ``NULL`` in ``IS DISTINCT FROM``.
* Fix an issue that caused some specific queries to fail in planning.

Hive Changes
------------

* Fix the Hive metadata cache to properly handle negative responses.
  This makes the background refresh work properly by clearing the cached
  metadata entries when an object is dropped outside of Presto.
  In particular, this fixes the common case where a table is dropped using
  Hive but Presto thinks it still exists.
* Fix metastore socket leak when SOCKS connect fails.

SPI Changes
-----------

* Changed the internal representation of structural types.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector that uses structural types, you will need
    to update your code to the new APIs.
