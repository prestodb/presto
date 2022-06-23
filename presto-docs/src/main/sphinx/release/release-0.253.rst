=============
Release 0.253
=============

.. warning::
    A CPU regression was introduced by :pr:`16027` for queries using :func:`element_at` with ``MAP``

**Details**
===========

General Changes
_______________
* Fix amplified ``Cumulative User Memory`` metric in web UI.
* Add tracking for system memory used by column statistics in ``TableFinishOperator``.
* Add support for shutting down coordinator via ``/v1/info/state`` endpoint.
* Add :func:`binomial_cdf` and :func:`inverse_binomial_cdf` functions.

Security Changes
________________
* Add support for schema access control in file based system access control.

Hive Changes
____________
* Fix S3 table creation error when the specified directory location is created from AWS console.
* Add Hive metastore impersonation access. This can be enabled with the ``hive.metastore-impersonation-enabled`` configuration property.
* Add load balancing of multiple Hive metastore instances. This can be enabled with the ``hive.metastore.load-balancing-enabled`` configuration property.

**Contributors**
================

Abhisek Gautam Saikia, Andrii Rosa, Arunachalam Thirupathi, Beinan Wang, Bin Fan, Dongliang Chen, Eran Amar, James Petty, James Sun, Ke Wang, Maria Basmanova, Mayank Garg, Naveen Kumar Mahadevuni, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Rongrong Zhong, Shixuan Fan, Tim Meehan, Vic Zhang, Xiang Fu, Yang Yang, Zhongting Hu, frankobe, vaishnavibatni
