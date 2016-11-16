**********************************
Repartitioned and Replicated Joins
**********************************

Background
----------

Presto can perform two types of distributed joins: repartitioned and replicated. In a repartitioned join, both inputs to a
join get hash partitioned across the nodes of the cluster. In a replicated join, one of the inputs is distributed to all of
the nodes on the cluster that have data from the other input.

Repartitioned joins are good for larger inputs, as they need less memory on each node and allow Presto to handle larger
joins overall. However, they can be much slower than replicated joins because they typically require more data to be
transferred over the network.

Replicated joins can be much faster than repartitioned joins if the replicated table is small enough (especially if it is
much smaller than the other input). But, if the replicated input is too large, the query can run out of memory.

Choosing the Distribution Type in Presto
----------------------------------------

The choice between replicated and repartitioned joins is controlled by the property ``join-distribution-type``. Its possible
values are ``repartitioned``, ``replicated``, and ``automatic``. The default value is ``automatic``. The property can also be
set per session using the session property ``join_distribution_type``.

.. code-block:: sql

    SET SESSION join_distribution_type = repartitioned;

When ``join-distribution-type`` is set to ``repartitioned``, repartitioned distribution is used. When it is set to
``replicated``, replicated distribution is used. In ``replicated`` mode, the right input to the join is the one that gets
replicated.

When the property is set to ``automatic``, the optimizer will choose which type of join to use based on the size of the join
inputs. If the size of either input to the join is less than 10% of ``query.max-memory-per-node``, Presto will replicate the
smaller of the inputs. If neither input is small enough or there is insufficient information to determine their size, a
repartitioned join is performed.
