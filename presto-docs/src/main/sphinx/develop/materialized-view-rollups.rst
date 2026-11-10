===================================
Materialized View Rollup Extensions
===================================

Presto plugins can describe how materialized views compose aggregate results
and how derived materialized view partition values map to source partition
values. The engine uses these descriptions when analyzing supported
materialized view rollups.

A plugin can provide two kinds of extension:

* an aggregate merge strategy, which identifies the aggregate that combines
  partial results produced by another aggregate; and
* a partition transform provider, which analyzes explicitly supported scalar
  functions and reconstructs their partition mapping from persisted metadata.

These extension points describe semantics to the engine. They do not create,
refresh, or select materialized views by themselves.

Registering Rollup Extensions
-----------------------------

Implement the relevant methods on ``Plugin`` to register extensions when the
plugin is installed:

.. code-block:: java

    public final class ExampleRollupPlugin
            implements Plugin
    {
        @Override
        public Iterable<MaterializedViewAggregationMergeStrategy>
                getMaterializedViewAggregationMergeStrategies()
        {
            return ImmutableList.of(
                    new MaterializedViewAggregationMergeStrategy(
                            "partial_mean",
                            Optional.of("merge_mean"),
                            false));
        }

        @Override
        public Iterable<MaterializedViewPartitionTransformProvider>
                getMaterializedViewPartitionTransformProviders()
        {
            return ImmutableList.of(new ExamplePartitionTransformProvider());
        }
    }

Both methods have empty default implementations, so existing plugins do not
need to implement them.

Aggregate Merge Strategies
--------------------------

``MaterializedViewAggregationMergeStrategy`` describes how the result of a
source aggregate can be combined at a coarser grouping level. It contains:

* the source aggregate function name;
* the optional merge aggregate function name; and
* whether the aggregate result is insensitive to duplicate input values.

For example, partial ``count`` results are combined with ``sum``. Partial
``min`` results are combined with ``min`` and remain correct when a join
duplicates an input value.

Presto registers the following strategies by default:

+------------------+-----------------+-----------------------+
| Source aggregate | Merge aggregate | Duplicate insensitive |
+==================+=================+=======================+
| ``count``        | ``sum``         | No                    |
+------------------+-----------------+-----------------------+
| ``sum``          | ``sum``         | No                    |
+------------------+-----------------+-----------------------+
| ``min``          | ``min``         | Yes                   |
+------------------+-----------------+-----------------------+
| ``max``          | ``max``         | Yes                   |
+------------------+-----------------+-----------------------+
| ``arbitrary``    | None            | Yes                   |
+------------------+-----------------+-----------------------+
| ``any_value``    | None            | Yes                   |
+------------------+-----------------+-----------------------+

An empty merge aggregate means that a source aggregate has no supported
rollup form. Function names are case-insensitive. A plugin cannot replace an
existing strategy, and registration fails if one batch contains the same
source function more than once.

Partition Transform Providers
-----------------------------

``MaterializedViewPartitionTransformProvider`` owns a set of scalar function
names. The same names identify persisted transforms. The provider analyzes a
supported function call and returns a ``MaterializedViewPartitionTransform``
that describes its partition mapping.

Function and Transform Ownership
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``getFunctionNames()`` returns the function names handled by the provider. The
set must be non-empty. Names are case-insensitive and must be unique across all
installed providers. A provider owns every supported overload of a registered
name.

A registered function is eligible only when it:

* belongs to the Java built-in namespace or the configured default function
  namespace;
* is a scalar function;
* is deterministic; and
* returns ``NULL`` without invoking the function when any input is ``NULL``.

Analyzing a Transform
^^^^^^^^^^^^^^^^^^^^^

The engine calls ``analyze()`` with the resolved ``FunctionMetadata`` and a
list of ``MaterializedViewPartitionTransformArgument`` values. An argument
represents either the source partition column and its type, or a constant
configuration value.

The engine requires exactly one direct source-column argument and constant
configuration arguments. The provider validates the argument count, order,
types and values for the registered function. It returns a transform for a
supported call and throws ``IllegalArgumentException`` otherwise.

Version one transforms are deterministic, preserve nulls, and are
monotonically non-decreasing. Each canonical target partition value maps to
either one exact source partition value or one contiguous half-open range of
source values.

Persisting and Decoding a Transform
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``MaterializedViewPartitionTransform.encode()`` returns all configuration
needed to reconstruct the transform. The returned map must include the
``transform`` property with one of the provider's registered function names.

Presto persists this map in materialized view metadata. When the metadata is
read, the engine selects the provider by the stored function name and calls
``decode()``. Re-encoding the decoded transform must produce the same name.

Registered names are therefore a persistent compatibility contract. A
provider must retain backward-compatible decoding and semantics for every name
that it has stored.

Canonical Values and Source Mappings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``getSourceMapping()`` returns the exact source mapping represented by a target
value. It throws ``IllegalArgumentException`` when the value has the wrong
type, is malformed or is not canonical for the transform.

``MaterializedViewPartitionMapping.singleValue()`` represents one exact source
partition value. ``MaterializedViewPartitionMapping.halfOpenRange()`` represents
one exact, contiguous half-open interval.

``MaterializedViewPartitionRange`` represents a half-open interval
``[lowerBound, upperBound)``. Both bounds must be non-null and have the same
type.

Thread Safety
-------------

Presto retains provider instances after plugin installation and may invoke
them concurrently. Implementations of
``MaterializedViewPartitionTransformProvider`` must therefore be thread-safe.
