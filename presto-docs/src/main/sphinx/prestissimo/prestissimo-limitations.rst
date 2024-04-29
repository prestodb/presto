=======================
Prestissimo Limitations
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Functions
=========

reduce_agg
----------

In Prestissimo, ``reduce_agg`` is not permitted to return ``null`` in either the 
``inputFunction`` or the ``combineFunction``. In Presto Java, this is permitted 
but undefined behavior. For more information about ``reduce_agg`` in Presto, 
see `reduce_agg <../functions/aggregate.html#reduce_agg>`_. 