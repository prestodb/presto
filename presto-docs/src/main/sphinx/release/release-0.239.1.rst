===============
Release 0.239.1
===============

.. warning::

   classification_precision function returns wrong results, fixed in 0.239.2 release.

General Changes
_______________
* Fix a reliability issue in ZSTD compression that causes frequent excessive GC events. (:pr:`15028`)
* Fix a memory accounting issue. (:pr:`15020`)
