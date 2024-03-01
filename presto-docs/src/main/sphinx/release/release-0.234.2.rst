===============
Release 0.234.2
===============

General Changes
_______________
* Fix an issue where cancelling running queries will cause the query to not being pruned properly,
  leading to fewer runnable slots.
* Fix an issue where queued queries cannot be cancelled or preempted properly.
