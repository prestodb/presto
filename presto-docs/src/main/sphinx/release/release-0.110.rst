=============
Release 0.110
=============

General Changes
---------------

* Fix result truncation bug in window function :func:`row_number` when performing a
  partitioned top-N that chooses the maximum or minimum ``N`` rows. For example::

    SELECT * FROM (
        SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderdate) AS rn,
            custkey, orderdate, orderstatus
        FROM orders
    ) WHERE rn <= 5;

