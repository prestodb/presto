=============
Release 0.120
=============

General Changes
---------------

* Fix regression that causes task scheduler to not retry requests in some cases.
* Throttle task info refresher on errors.
* Fix planning failure that prevented the use of large ``IN`` lists.
