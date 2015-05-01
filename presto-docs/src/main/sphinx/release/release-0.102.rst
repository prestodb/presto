=============
Release 0.102
=============

Unicode Support
---------------

All string functions have been updated to support Unicode. The functions assume
that the string contains valid UTF-8 encoded code points. There are no explicit
checks for valid UTF-8, and the functions may return incorrect results or fail on
invalid UTF-8.  Invalid UTF-8 data can be corrected with :func:`from_utf8`.

Additionally, the functions operate on Unicode code points and not user visible
*characters* (or *grapheme clusters*).  Some languages combine multiple code points
into a single user-perceived *character*, the basic unit of a writing system for a
language, but the functions will treat each code point as a separate unit.


General Changes
---------------

* Support returning booleans as numbers in JDBC driver
* Fix :func:`contains` to return ``NULL`` if the value was not found, but a ``NULL`` was.
* Fix nested :ref:`row_type` rendering in ``DESCRIBE``.
* Add :func:`array_join`.
* Optimize map subscript operator.
* Add :func:`from_utf8` and :func:`to_utf8` functions.
* Add ``task_writer_count`` session property to set ``task.writer-count``.

Hive Changes
------------

* Collect more metrics from ``PrestoS3FileSystem``.
* Retry when seeking in ``PrestoS3FileSystem``.
* Ignore ``InvalidRange`` error in ``PrestoS3FileSystem``.
* Implement rename and delete in ``PrestoS3FileSystem``.
* Fix assertion failure when running ``SHOW TABLES FROM schema``.
