=============
Release 0.121
=============

General Changes
---------------

* Fix regression that causes task scheduler to not retry requests in some cases.
* Throttle task info refresher on errors.
* Fix planning failure that prevented the use of large ``IN`` lists.
* Fix comparison of ``array(T)`` where ``T`` is a comparable, non-orderable type.
