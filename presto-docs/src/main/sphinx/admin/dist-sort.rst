================
Distributed sort
================

Distributed sort allows to sort data which exceeds ``query.max-memory-per-node``.
Distributed sort is enabled via ``distributed_sort`` session property or
``distributed-sort`` configuration property set in
``etc/config.properties`` of the coordinator. Distributed sort is enabled by
default.

When distributed sort is enabled, sort operator executes in parallel on multiple
nodes in the cluster. Partially sorted data from each Presto worker node is then streamed
to a single worker node for a final merge. This technique allows to utilize memory of multiple
Presto worker nodes for sorting. The primary purpose of distributed sort is to allow for sorting
of data sets which don't normally fit into single node memory. Performance improvement
can be expected, but it won't scale linearly with the number of nodes since the
data needs to be merged by a single node.
