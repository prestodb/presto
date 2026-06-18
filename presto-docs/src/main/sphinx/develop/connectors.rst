==========
Connectors
==========

Connectors are the source of all data for queries in Presto. Even if
your data source doesn't have underlying tables backing it, as long as
you adapt your data source to the API expected by Presto, you can write
queries against this data.

ConnectorSplit
--------------

Instances of your connector splits.

The ``getNodeSelectionStrategy`` method indicates the node affinity
of a Split,it has three options:

``HARD_AFFINITY``: Split is NOT remotely accessible and has to be on
specific nodes

``SOFT_AFFINITY``: Connector split provides a list of preferred nodes
for engine to pick from but not mandatory.

``NO_PREFERENCE``: Split is remotely accessible and can be on any nodes

The ``getPreferredNodes`` method provides a list of preferred nodes
for scheduler to pick.


The scheduler will respect the preference if the strategy is
HARD_AFFINITY. Otherwise, the scheduler will prioritize the provided
nodes if the strategy is SOFT_AFFINITY.
But there is no guarantee that the scheduler will pick them
if the provided nodes are busy. Empty list indicates no preference.

ConnectorFactory
----------------

Instances of your connector are created by a ``ConnectorFactory``
instance which is created when Presto calls ``getConnectorFactory()`` on the
plugin. The connector factory is a simple interface responsible for creating an
instance of a ``Connector`` object that returns instances of the
following services:

* ``ConnectorMetadata``
* ``ConnectorSplitManager``
* ``ConnectorHandleResolver``
* ``ConnectorRecordSetProvider``

ConnectorMetadata
^^^^^^^^^^^^^^^^^

The connector metadata interface has a large number of important
methods that are responsible for allowing Presto to look at lists of
schemas, lists of tables, lists of columns, and other metadata about a
particular data source.

This interface is too big to list in this documentation, but if you
are interested in seeing strategies for implementing these methods,
look at the :doc:`example-http` and the Cassandra connector. If
your underlying data source supports schemas, tables and columns, this
interface should be straightforward to implement. If you are attempting
to adapt something that is not a relational database (as the Example HTTP
connector does), you may need to get creative about how you map your
data source to Presto's schema, table, and column concepts.

ConnectorSplitManger
^^^^^^^^^^^^^^^^^^^^

The split manager partitions the data for a table into the individual
chunks that Presto will distribute to workers for processing.
For example, the Hive connector lists the files for each Hive
partition and creates one or more split per file.
For data sources that don't have partitioned data, a good strategy
here is to simply return a single split for the entire table. This
is the strategy employed by the Example HTTP connector.

ConnectorRecordSetProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^

Given a split and a list of columns, the record set provider is
responsible for delivering data to the Presto execution engine.
It creates a ``RecordSet``, which in turn creates a ``RecordCursor``
that is used by Presto to read the column values for each row.

Node Selection Strategy
-----------------------

The node selection strategy is specified by a connector on each split.  The possible values are:

* HARD_AFFINITY - The Presto runtime must schedule this split on the nodes specified on ``ConnectorSplit#getPreferredNodes``.
* SOFT_AFFINITY - The Presto runtime should prefer ``ConnectorSplit#getPreferredNodes`` nodes, but doesn't have to. Use this value primarily for caching.
* NO_PREFERENCE - No preference. 

Use the ``node_selection_strategy`` session property in Hive and Iceberg to override this. 