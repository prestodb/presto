=============
Release 0.259
=============

.. warning::
This release includes a regression on jdbc connector.

**Details**
===========

General Changes
_______________
* Fix ``ClassCastException`` that sometimes occurred for ``EXPLAIN`` queries when the query contained ``LIKE`` predicates.
* Add Weibull distribution CDF :func:`weibull_cdf` and inverse CDF :func:`inverse_weibull_cdf` functions.
* Enable verbose error messages for ``EXCEEDED_LOCAL_MEMORY_LIMIT`` by default.  This can be disabled by setting the configuration property ``memory.verbose-exceeded-memory-limit-errors-enabled`` to ``false``.

JDBC Driver Changes
___________________
* Add ``getConnectionProperties`` method to ``PrestoConnection`` to retrieve connection properties after a connection is established. (:pr:`16329`)

JDBC Connector Changes
______________________
* Add support for partial pushdown of JDBC filters.

Resource Groups Changes
_______________________
* Add support for specifying query limits (cpu time, total memory and execution time) at the resource group level. See :doc:`/admin/resource-groups`.

SPI Changes
___________
* Add ``ResourceGroupQueryLimits`` to the SPI and the corresponding getter and setter functions in ``ResourceGroups``.

**Credits**
===========

Andrii Rosa, Arunachalam Thirupathi, Beinan, Grace Xin, Ivan Millan, James Petty, James Sun, Jeremy Craig Sawruk, Julian Zhuoran Zhao, Kyle B Campbell, Naveen Kumar Mahadevuni, Nikhil Collooru, Pranjal Shankhdhar, Rebecca Schlussel, Rongrong Zhong, Seba Villalobos, Shixuan Fan, Sreeni Viswanadha, Suryadev Sahadevan Rajesh, Swapnil Tailor, Tim Meehan, Yang Yang, Zac Wen, yangping.wyp
