=========================
Hive Connector Procedures
=========================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Use the :doc:`/sql/call` statement to perform data manipulation or
administrative tasks. Procedures must include a qualified catalog name, if your
Hive catalog is called ``web``::

    CALL web.system.example_procedure()

Create Empty Partition
----------------------

* ``system.create_empty_partition(schema_name, table_name, partition_columns, partition_values)``

  Create an empty partition in the specified table.

Sync Partition Metadata
-----------------------

* ``system.sync_partition_metadata(schema_name, table_name, mode, case_sensitive)``

  Check and update partitions list in metastore. There are three modes available:

  * ``ADD`` : add any partitions that exist on the file system but not in the metastore.
  * ``DROP``: drop any partitions that exist in the metastore but not on the file system.
  * ``FULL``: perform both ``ADD`` and ``DROP``.

  The ``case_sensitive`` argument is optional. The default value is ``true`` for compatibility
  with Hive's ``MSCK REPAIR TABLE`` behavior, which expects the partition column names in
  file system paths to use lowercase (e.g. ``col_x=SomeValue``). Partitions on the file system
  not conforming to this convention are ignored, unless the argument is set to ``false``.

Invalidate Directory List Cache
-------------------------------

* ``system.invalidate_directory_list_cache()``

  Flush full directory list cache.

* ``system.invalidate_directory_list_cache(directory_path)``

  Invalidate directory list cache for specified directory_path.

How to invalidate the directory list cache
------------------------------------------

Invalidating directory list cache is useful when the files are added or deleted in the cache directory path and you want to make the changes visible to Presto immediately.
There are a couple of ways for invalidating this cache and are listed below -

* The Hive connector exposes a procedure over JMX (``com.facebook.presto.hive.CachingDirectoryLister#flushCache``) to invalidate the directory list cache. You can call this procedure to invalidate the directory list cache by connecting via jconsole or jmxterm. This procedure flushes all the cache entries.

* The Hive connector exposes ``system.invalidate_directory_list_cache`` procedure which gives the flexibility to invalidate the list cache completely or partially as per the requirement and can be invoked in various ways. See `Invalidate Directory List Cache`_ for more information.

Invalidate Metastore Cache
--------------------------

* ``system.invalidate_metastore_cache()``

  Invalidate all metastore caches.

* ``system.invalidate_metastore_cache(schema_name)``

  Invalidate all metastore cache entries linked to a specific schema.

* ``system.invalidate_metastore_cache(schema_name, table_name)``

  Invalidate all metastore cache entries linked to a specific table.

* ``system.invalidate_metastore_cache(schema_name, table_name, partition_columns, partition_values)``

  Invalidate all metastore cache entries linked to a specific partition.

  .. note::

    To enable ``system.invalidate_metastore_cache`` procedure, ``hive.invalidate-metastore-cache-procedure-enabled`` must be set to ``true``.
    See the properties in :ref:`connector/hive/hive-configuration:Metastore Configuration Properties` table for more information.

How to invalidate the metastore cache
-------------------------------------

Invalidating metastore cache is useful when the Hive metastore is updated outside of Presto and you want to make the changes visible to Presto immediately.
There are a couple of ways for invalidating this cache and are listed below -

* The Hive connector exposes a procedure over JMX (``com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore#invalidateAll``) to invalidate the metastore cache. You can call this procedure to invalidate the metastore cache by connecting via jconsole or jmxterm. However, this procedure flushes the cache for all the tables in all the schemas.

* The Hive connector exposes ``system.invalidate_metastore_cache`` procedure which enables users to invalidate the metastore cache completely or partially as per the requirement and can be invoked with various arguments. See `Invalidate Metastore Cache`_ for more information.

