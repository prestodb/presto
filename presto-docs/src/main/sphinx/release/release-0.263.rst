=============
Release 0.263
=============

**Details**
===========

General Changes
_______________
* Fix a correctness bug in distinct aggregations when combined with a filtering mask containing null values.
* Improve spilling CPU and memory efficiency for distinct and order by aggregations.
* Add a new session property ``heap_dump_on_exceeded_memory_limit_enabled`` to enable heapdump on exceeded memory failures. The heapdump file directory can be provided using ``exceeded_memory_limit_heap_dump_file_directory`` session property.
* Added Smart Sampling feature that samples tables at query time. This feature can be enabled using session property ``key_based_sampling_enabled=true``. Additionally, sampling rate and sampling function name can be provided by sessions properties ``key_based_sampling_percentage`` and ``key_based_sampling_function`` respectively.
* Support distinct user defined types.

Verifier Changes
________________
* Improve Verifier by skipping non-deterministic queries which have a ``LIMIT`` sub clause without ``ORDER BY``.

**Credits**
===========

Ajay George, Arjun Gupta, Arunachalam Thirupathi, Ge Gao, James Petty, James Sun, Maria Basmanova, Neerad Somanchi, Nikhil Collooru, Pranjal Shankhdhar, Rongrong Zhong, SOURAV PAL, Sreeni Viswanadha, Swapnil Tailor, Tim Meehan, Zac Wen, Zhan Yuan, abhiseksaikia
