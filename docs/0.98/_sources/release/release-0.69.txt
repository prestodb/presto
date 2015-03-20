============
Release 0.69
============

.. warning::

    The following config properties must be removed from the
    ``etc/config.properties`` file on both the coordinator and workers:

    * ``presto-metastore.db.type``
    * ``presto-metastore.db.filename``

    Additionally, the ``datasources`` property is now deprecated
    and should also be removed (see `Datasource Configuration`_).

Prevent Scheduling Work on Coordinator
--------------------------------------

We have a new config property, ``node-scheduler.include-coordinator``,
that allows or disallows scheduling work on the coordinator.
Previously, tasks like final aggregations could be scheduled on the
coordinator. For larger clusters, processing work on the coordinator
can impact query performance because the machine's resources are not
available for the critical task of scheduling, managing and monitoring
query execution.

We recommend setting this property to ``false`` for the coordinator.
See :ref:`config_properties` for an example.

Datasource Configuration
------------------------

The ``datasources`` config property has been deprecated.
Please remove it from your ``etc/config.properties`` file.
The datasources configuration is now automatically generated based
on the ``node-scheduler.include-coordinator`` property
(see `Prevent Scheduling Work on Coordinator`_).

Raptor Connector
----------------

Presto has an extremely experimental connector that was previously called
the ``native`` connector and was intertwined with the main Presto code
(it was written before Presto had connectors). This connector is now
named ``raptor`` and lives in a separate plugin.

As part of this refactoring, the ``presto-metastore.db.type`` and
``presto-metastore.db.filename`` config properties no longer exist
and must be removed from ``etc/config.properties``.

The Raptor connector stores data on the Presto machines in a
columnar format using the same layout that Presto uses for in-memory
data. Currently, it has major limitations: lack of replication,
dropping a table does not reclaim the storage, etc. It is only
suitable for experimentation, temporary tables, caching of data from
slower connectors, etc. The metadata and data formats are subject to
change in incompatible ways between releases.

If you would like to experiment with the connector, create a catalog
properties file such as ``etc/catalog/raptor.properties`` on both the
coordinator and workers that contains the following:

.. code-block:: none

    connector.name=raptor
    metadata.db.type=h2
    metadata.db.filename=var/data/db/MetaStore

Machine Learning Functions
--------------------------

Presto now has functions to train and use machine learning models
(classifiers and regressors). This is currently only a proof of concept
and is not ready for use in production. Example usage is as follows::

    SELECT evaluate_classifier_predictions(label, classify(features, model))
    FROM (
        SELECT learn_classifier(label, features) AS model
        FROM training_data
    )
    CROSS JOIN validation_data

In the above example, the column ``label`` is a ``bigint`` and the column
``features`` is a map of feature identifiers to feature values. The feature
identifiers must be integers (encoded as strings because JSON only supports
strings for map keys) and the feature values are numbers (floating point).

Variable Length Binary Type
---------------------------

Presto now supports the ``varbinary`` type for variable length binary data.
Currently, the only supported function is :func:`length`.
The Hive connector now maps the Hive ``BINARY`` type to ``varbinary``.

General Changes
---------------

* Add missing operator: ``timestamp with time zone`` - ``interval year to month``
* Support explaining sampled queries
* Add JMX stats for abandoned and canceled queries
* Add ``javax.inject`` to parent-first class list for plugins
* Improve error categorization in event logging
