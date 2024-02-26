=================================
Logical Properties of Query Plans
=================================

Presto implements a framework for associating logical properties with the
result sets produced by the nodes of a query plan. These logical properties
might either initially derive from constraints defined on tables, or from
operations performed by intermediate nodes in the query plan, such as
aggregations, limits, or the application of predicates. The Presto optimizer
may then exploit these logical properties to perform optimizations such as
removing redundant operations or other logical transformations.

The propagation of logical properties in query plans is enabled by the 
``exploit_constraints`` session property or ``optimizer.exploit_constraints``
configuration property set in ``etc/config.properties`` of the coordinator.
Logical property propagation is enabled by default.


Types of Logical Properties
---------------------------

Currently Presto detects and propagates the following logical properties:

* ``KeyProperty`` - Represents a collection of distinct attributes that hold for
  a final or intermediate result set produced by a plan node.

* ``MaxCardProperty`` - Represents a provable maximum number of rows in a final or
  intermediate result set produced by a plan node.

* ``EquivalenceClassProperty`` - Represents classes of equivalent variable and
  constant references that hold for a final or intermediate result set produced
  by a plan node.
