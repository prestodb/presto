=============
Release 0.265
=============

**Details**
===========

General Changes
_______________
* Add a configuration property ``fragment-result-cache.max-cache-size`` to control the total on-disk size of fragment result cache. The default value is 100G.
* Rename function ``array_dupes`` to :func:`array_duplicates`, and rename function ``array_has_dupes`` to :func:`array_has_duplicates`. Previous names are kept as aliases but will be deprecated in the future.

Verifier Changes
________________
* Fix verifier executable jar corruption.

Hive Changes
____________
* Improve memory and CPU usage for writing DWRF files.
* Upgrade Hudi support to 0.9.0.

**Credits**
===========

Ajay George, Arjun Gupta, Arunachalam Thirupathi, Ge Gao, James Petty, James Sun, Junhyung Song, JySongWithZhangce, Masha Basmanova, Naveen Kumar Mahadevuni, Pranjal Shankhdhar, Rongrong Zhong, Sagar Sumit, Sreeni Viswanadha, Tim Meehan, Will Holen, Ying Su, Zac Wen, Zhan Yuan, abhiseksaikia, beinan
