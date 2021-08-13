=============
Release 0.259
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fixed a casting error that sometimes occurred for EXPLAIN queries when the query plan contains LIKE predicates.
* Add Weibull distribution CDF and inverse CDF functions to MathFunctions.java.
* Enable verbose memory exceeded errors by default.
* Support CREATE TYPE syntax.

Resource Groups Changes
_______________________
* Add support to specify query limits (Cpu time, total memory and execution time) at the resource group level. See :doc:`/admin/resource-groups`.

SPI Changes
___________
* Add `ResourceGroupQueryLimits` to the SPI and the corresponding getter and setter functions in `ResourceGroups`.

JDBC Changes
____________
* Add method ``getConnectionProperties`` to PrestoConnection to allow for connection properties retrieval after a connection establishes. (:pr:`16329`).
* Support partial pushdown of JDBC filters.

Kafka Changes
_____________
* Extract pluggable interface for kafka cluster supplier.

**Credits**
===========

Andrii Rosa, Arunachalam Thirupathi, Beinan, Grace Xin, Ivan Millan, James Petty, James Sun, Jeremy Craig Sawruk, Julian Zhuoran Zhao, Kyle B Campbell, Naveen Kumar Mahadevuni, Nikhil Collooru, Pranjal Shankhdhar, Rebecca Schlussel, Rongrong Zhong, Seba Villalobos, Shixuan Fan, Sreeni Viswanadha, Suryadev Sahadevan Rajesh, Swapnil Tailor, Tim Meehan, Yang Yang, Zac Wen, yangping.wyp
