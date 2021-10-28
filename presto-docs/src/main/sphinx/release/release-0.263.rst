=============
Release 0.263
=============

.. warning::
    There is a correctness bug around ``ORDER BY`` aggregations when spilling is enabled which is introduced by :pr:`16788`

**Details**
===========

General Changes
_______________
* Fix a potential correctness bug for distinct aggregations with filter expressions containing null values.
* Improve spilling CPU and memory efficiency for ``DISTINCT`` and ``ORDER BY`` aggregations.
* Add support to create a heapdump when we exceed local memory limits for a query. This can be enabled with the ``heap_dump_on_exceeded_memory_limit_enabled`` session property and the directory to create the heapdump can be set by ``exceeded_memory_limit_heap_dump_file_directory`` session property.
* Add key-based sampling feature that samples tables at query time. This feature can be enabled using session property ``key_based_sampling_enabled=true``. Additionally, default sampling rate and sampling function name can be overridden by sessions properties ``key_based_sampling_percentage`` and ``key_based_sampling_function`` respectively.

Verifier Changes
________________
* Improve false positives by skipping non-deterministic queries which have a ``LIMIT`` sub clause without ``ORDER BY``.

**Credits**
===========

Ajay George, Arjun Gupta, Arunachalam Thirupathi, Ge Gao, James Petty, James Sun, Maria Basmanova, Neerad Somanchi, Nikhil Collooru, Pranjal Shankhdhar, Rongrong Zhong, SOURAV PAL, Sreeni Viswanadha, Swapnil Tailor, Tim Meehan, Zac Wen, Zhan Yuan, abhiseksaikia
