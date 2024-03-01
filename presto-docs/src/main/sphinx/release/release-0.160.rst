=============
Release 0.160
=============

General Changes
---------------

* Fix planning failure when query has multiple unions with identical underlying columns.
* Fix planning failure when multiple ``IN`` predicates contain an identical subquery.
* Fix resource waste where coordinator floods rebooted workers if worker
  comes back before coordinator times out the query.
* Add :doc:`/functions/lambda`.

Hive Changes
------------

* Fix planning failure when inserting into columns of struct types with uppercase field names.
* Fix resource leak when using Kerberos authentication with impersonation.
* Fix creating external tables so that they are properly recognized by the Hive metastore.
  The Hive table property ``EXTERNAL`` is now set to ``TRUE`` in addition to the setting
  the table type. Any previously created tables need to be modified to have this property.
* Add ``bucket_execution_enabled`` session property.
