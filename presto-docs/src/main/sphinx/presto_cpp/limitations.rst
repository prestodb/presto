======================
Presto C++ Limitations
======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

General Limitations
===================

The C++ evaluation engine has a number of limitations:

* Not all built-in functions are implemented in C++. Attempting to use unimplemented functions results in a query failure. For supported functions, see `Function Coverage <https://facebookincubator.github.io/velox/functions/presto/coverage.html>`_.

* Not all built-in types are implemented in C++. Attempting to use unimplemented types will result in a query failure.

  * All basic and structured types in :doc:`../language/types` are supported, except for ``CHAR``, ``TIME``, and ``TIME WITH TIMEZONE``. These are subsumed by ``VARCHAR``, ``TIMESTAMP`` and ``TIMESTAMP WITH TIMEZONE``.

  * Presto C++ only supports unlimited length ``VARCHAR``, and does not honor the length ``n`` in ``varchar[n]``.

  * The following types are not supported: ``IPADDRESS``, ``IPPREFIX``, ``KHYPERLOGLOG``, ``P4HYPERLOGLOG``, ``QDIGEST``, ``TDIGEST``, ``GEOMETRY``, ``BINGTILE``.

* Certain parts of the plugin SPI are not used by the C++ evaluation engine. In particular, C++ workers will not load any plugin in the plugins directory, and certain plugin types are either partially or completely unsupported.

  * ``PageSourceProvider``, ``RecordSetProvider``, and ``PageSinkProvider`` do not work in the C++ evaluation engine.

  * User-supplied functions, types, parametric types and block encodings are not supported.

  * The event listener plugin does not work at the split level.

  * User-defined functions do not work in the same way, see `Remote Function Execution <features.html#remote-function-execution>`_.

* Memory management works differently in the C++ evaluation engine. In particular:

  * The OOM killer is not supported.
  * The reserved pool is not supported.
  * In general, queries may use more memory than they are allowed to through memory arbitration. See `Memory Management <https://facebookincubator.github.io/velox/develop/memory.html>`_.

Functions
=========

reduce_agg
----------

In C++ based Presto, ``reduce_agg`` is not permitted to return ``null`` in either the 
``inputFunction`` or the ``combineFunction``. In Presto (Java), this is permitted 
but undefined behavior. For more information about ``reduce_agg`` in Presto, 
see `reduce_agg <../functions/aggregate.html#reduce_agg>`_.

approx_set
----------

Cardinality estimation with ``approx_set`` has a maximum standard error of ``e``
(default value of ``0.01625``). ``approx_set`` returns different values in Presto C++
and Presto (Java). For more information about ``approx_set`` in Presto, see
`approx_set <../functions/aggregate.html#approx_set>`_.