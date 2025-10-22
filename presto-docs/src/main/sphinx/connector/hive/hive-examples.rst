=======================
Hive Connector Examples
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

The Hive connector supports querying and manipulating Hive tables and schemas
(databases). While some uncommon operations will need to be performed using
Hive directly, most operations can be performed using Presto.

Create a schema
---------------

Create a new Hive schema named ``web`` that will store tables in an
S3 bucket named ``my-bucket``::

    CREATE SCHEMA hive.web
    WITH (location = 's3://my-bucket/')

Create a managed table
----------------------

Create a new Hive table named ``page_views`` in the ``web`` schema
that is stored using the ORC file format, partitioned by date and
country, and bucketed by user into ``50`` buckets (note that Hive
requires the partition columns to be the last columns in the table)::

    CREATE TABLE hive.web.page_views (
      view_time timestamp,
      user_id bigint,
      page_url varchar,
      ds date,
      country varchar
    )
    WITH (
      format = 'ORC',
      partitioned_by = ARRAY['ds', 'country'],
      bucketed_by = ARRAY['user_id'],
      bucket_count = 50
    )

Drop a partition
----------------

Drop a partition from the ``page_views`` table::

    DELETE FROM hive.web.page_views
    WHERE ds = DATE '2016-08-09'
      AND country = 'US'

Add an empty partition
----------------------

Add an empty partition to the ``page_views`` table::

    CALL system.create_empty_partition(
        schema_name => 'web',
        table_name => 'page_views',
        partition_columns => ARRAY['ds', 'country'],
        partition_values => ARRAY['2016-08-09', 'US']);

Query a table
-------------

Query the ``page_views`` table::

    SELECT * FROM hive.web.page_views

List partitions
---------------

List the partitions of the ``page_views`` table::

    SELECT * FROM hive.web."page_views$partitions"

Create an external table
------------------------

Create an external Hive table named ``request_logs`` that points at
existing data in S3::

    CREATE TABLE hive.web.request_logs (
      request_time timestamp,
      url varchar,
      ip varchar,
      user_agent varchar
    )
    WITH (
      format = 'TEXTFILE',
      external_location = 's3://my-bucket/data/logs/'
    )

Drop external table
-------------------

Drop the external table ``request_logs``. This only drops the metadata
for the table. The referenced data directory is not deleted::

    DROP TABLE hive.web.request_logs

Drop schema
-----------

Drop a schema::

    DROP SCHEMA hive.web
