=================
Router Schedulers
=================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Presto router provides multiple scheduling algorithms for load balancing across
multiple clusters.

* ``RANDOM_CHOICE``

Randomly selecting a cluster from a list of candidates.

* ``ROUND_ROBIN``

Selecting clusters from a list of candidates in turn. Note that as the algorithm
keeps the state of the selected index, it can only be used when the candidates
are always consistent.

* ``USER_HASH``

Selecting a clusters by hashing the username. This ensures queries from the same
user will always be routed to the same cluster.

* ``WEIGHTED_RANDOM_CHOICE``

Randomly selecting a cluster from a list of candidates with pre-defined weights.
Clusters with higher weights have higher opportunity to be selected.
