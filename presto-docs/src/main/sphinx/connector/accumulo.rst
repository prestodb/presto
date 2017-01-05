Accumulo Connector
==================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The Accumulo connector supports reading and writing data from Apache Accumulo.
Please read this page thoroughly to understand the capabilities and features of the connector.

Installing the Iterator Dependency
----------------------------------

The Accumulo connector uses custom Accumulo iterators in
order to push various information in a SQL predicate clause to Accumulo for
server-side filtering, known as *predicate pushdown*. In order
for the server-side iterators to work, you need to add the ``presto-accumulo``
jar file to Accumulo's ``lib/ext`` directory on each TabletServer node.

.. code-block:: bash

    # For each TabletServer node:
    scp $PRESTO_HOME/plugins/accumulo/presto-accumulo-*.jar [tabletserver_address]:$ACCUMULO_HOME/lib/ext

    # TabletServer should pick up new JAR files in ext directory, but may require restart

Note that this uses Java 8.  If your Accumulo cluster is using Java 7,
you'll receive an ``Unsupported major.minor version 52.0`` error in your TabletServer logs when you
attempt to create an indexed table.  You'll instead need to use the *presto-accumulo-iterators* jar file
that is located at `https://github.com/bloomberg/presto-accumulo <https://github.com/bloomberg/presto-accumulo>`_.

Connector Configuration
-----------------------

Create ``etc/catalog/accumulo.properties``
to mount the ``accumulo`` connector as the ``accumulo`` catalog,
replacing the ``accumulo.xxx`` properties as required:

.. code-block:: none

    connector.name=accumulo
    accumulo.instance=xxx
    accumulo.zookeepers=xxx
    accumulo.username=username
    accumulo.password=password

Configuration Variables
-----------------------

================================================ ====================== ========== =====================================================================================
Property Name                                    Default Value          Required   Description
================================================ ====================== ========== =====================================================================================
``accumulo.instance``                            (none)                 Yes        Name of the Accumulo instance
``accumulo.zookeepers``                          (none)                 Yes        ZooKeeper connect string
``accumulo.username``                            (none)                 Yes        Accumulo user for Presto
``accumulo.password``                            (none)                 Yes        Accumulo password for user
``accumulo.zookeeper.metadata.root``             ``/presto-accumulo``   No         Root znode for storing metadata. Only relevant if using default Metadata Manager
``accumulo.cardinality.cache.size``              ``100000``             No         Sets the size of the index cardinality cache
``accumulo.cardinality.cache.expire.duration``   ``5m``                 No         Sets the expiration duration of the cardinality cache.
================================================ ====================== ========== =====================================================================================

Unsupported Features
--------------------

The following features are not supported:

* Adding columns via ``ALTER TABLE``: While you cannot add columns via SQL, you can using a tool.
  See the below section on `Adding Columns <#adding-columns>`__ for more details.
* ``DELETE``: Deletion of rows is not yet implemented for the connector.

Usage
-----

Simply begin using SQL to create a new table in Accumulo to begin
working with data. By default, the first column of the table definition
is set to the Accumulo row ID. This should be the primary key of your
table, and keep in mind that any ``INSERT`` statements containing the same
row ID is effectively an UPDATE as far as Accumulo is concerned, as any
previous data in the cell will be overwritten. The row ID can be
any valid Presto datatype. If the first column is not your primary key, you
can set the row ID column using the ``row_id`` table property within the ``WITH``
clause of your table definition.

Simply issue a ``CREATE TABLE`` statement to create a new Presto/Accumulo table::

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    );

.. code-block:: sql

    DESCRIBE myschema.scientists;

.. code-block:: none

      Column   |  Type   | Extra |                      Comment
    -----------+---------+-------+---------------------------------------------------
     recordkey | varchar |       | Accumulo row ID
     name      | varchar |       | Accumulo column name:name. Indexed: false
     age       | bigint  |       | Accumulo column age:age. Indexed: false
     birthday  | date    |       | Accumulo column birthday:birthday. Indexed: false

This command will create a new Accumulo table with the ``recordkey`` column
as the Accumulo row ID. The name, age, and birthday columns are mapped to
auto-generated column family and qualifier values (which, in practice,
are both identical to the Presto column name).

When creating a table using SQL, you can optionally specify a
``column_mapping`` table property. The value of this property is a
comma-delimited list of triples, presto column **:** accumulo column
family **:** accumulo column qualifier, with one triple for every
non-row ID column. This sets the mapping of the Presto column name to
the corresponding Accumulo column family and column qualifier.

If you don't specify the ``column_mapping`` table property, then the
connector will auto-generate column names (respecting any configured locality groups).
Auto-generation of column names is only available for internal tables, so if your
table is external you must specify the column_mapping property.

For a full list of table properties, see `Table Properties <#table-properties>`__.

For example:

.. code-block:: sql

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    )
    WITH (
      column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date'
    );

.. code-block:: sql

    DESCRIBE myschema.scientists;

.. code-block:: none

      Column   |  Type   | Extra |                    Comment
    -----------+---------+-------+-----------------------------------------------
     recordkey | varchar |       | Accumulo row ID
     name      | varchar |       | Accumulo column metadata:name. Indexed: false
     age       | bigint  |       | Accumulo column metadata:age. Indexed: false
     birthday  | date    |       | Accumulo column metadata:date. Indexed: false

You can then issue ``INSERT`` statements to put data into Accumulo.

.. note::

    While issuing ``INSERT`` statements is convenient,
    this method of loading data into Accumulo is low-throughput. You'll want
    to use the Accumulo APIs to write ``Mutations`` directly to the tables.
    See the section on `Loading Data <#loading-data>`__ for more details.

.. code-block:: sql

    INSERT INTO myschema.scientists VALUES
    ('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
    ('row2', 'Alan Turing', 103, DATE '1912-06-23' );

.. code-block:: sql

    SELECT * FROM myschema.scientists;

.. code-block:: none

     recordkey |     name     | age |  birthday
    -----------+--------------+-----+------------
     row1      | Grace Hopper | 109 | 1906-12-09
     row2      | Alan Turing  | 103 | 1912-06-23
    (2 rows)

As you'd expect, rows inserted into Accumulo via the shell or
programatically will also show up when queried. (The Accumulo shell
thinks "-5321" is an option and not a number... so we'll just make TBL a
little younger.)

.. code-block:: bash

    $ accumulo shell -u root -p secret
    root@default> table myschema.scientists
    root@default myschema.scientists> insert row3 metadata name "Tim Berners-Lee"
    root@default myschema.scientists> insert row3 metadata age 60
    root@default myschema.scientists> insert row3 metadata date 5321

.. code-block:: sql

    SELECT * FROM myschema.scientists;

.. code-block:: none

     recordkey |      name       | age |  birthday
    -----------+-----------------+-----+------------
     row1      | Grace Hopper    | 109 | 1906-12-09
     row2      | Alan Turing     | 103 | 1912-06-23
     row3      | Tim Berners-Lee |  60 | 1984-07-27
    (3 rows)

You can also drop tables using ``DROP TABLE``. This command drops both
metadata and the tables. See the below section on `External
Tables <#external-tables>`__ for more details on internal and external
tables.

.. code-block:: sql

    DROP TABLE myschema.scientists;

Indexing Columns
----------------

Internally, the connector creates an Accumulo ``Range`` and packs it in
a split. This split gets passed to a Presto Worker to read the data from
the ``Range`` via a ``BatchScanner``. When issuing a query that results
in a full table scan, each Presto Worker gets a single ``Range`` that
maps to a single tablet of the table. When issuing a query with a
predicate (i.e. ``WHERE x = 10`` clause), Presto passes the values
within the predicate (``10``) to the connector so it can use this
information to scan less data. When the Accumulo row ID is used as part
of the predicate clause, this narrows down the ``Range`` lookup to quickly
retrieve a subset of data from Accumulo.

But what about the other columns? If you're frequently querying on
non-row ID columns, you should consider using the **indexing**
feature built into the Accumulo connector. This feature can drastically
reduce query runtime when selecting a handful of values from the table,
and the heavy lifting is done for you when loading data via Presto
``INSERT`` statements (though, keep in mind writing data to Accumulo via
``INSERT`` does not have high throughput).

To enable indexing, add the ``index_columns`` table property and specify
a comma-delimited list of Presto column names you wish to index (we use the
``string`` serializer here to help with this example -- you
should be using the default ``lexicoder`` serializer).

.. code-block:: sql

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    )
    WITH (
      serializer = 'string',
      index_columns='name,age,birthday'
    );

After creating the table, we see there are an additional two Accumulo
tables to store the index and metrics.

.. code-block:: none

    root@default> tables
    accumulo.metadata
    accumulo.root
    myschema.scientists
    myschema.scientists_idx
    myschema.scientists_idx_metrics
    trace

After inserting data, we can look at the index table and see there are
indexed values for the name, age, and birthday columns. The connector
queries this index table

.. code-block:: sql

    INSERT INTO myschema.scientists VALUES
    ('row1', 'Grace Hopper', 109, DATE '1906-12-09'),
    ('row2', 'Alan Turing', 103, DATE '1912-06-23');

.. code-block:: none

    root@default> scan -t myschema.scientists_idx
    -21011 metadata_date:row2 []
    -23034 metadata_date:row1 []
    103 metadata_age:row2 []
    109 metadata_age:row1 []
    Alan Turing metadata_name:row2 []
    Grace Hopper metadata_name:row1 []

When issuing a query with a ``WHERE`` clause against indexed columns,
the connector searches the index table for all row IDs that contain the
value within the predicate. These row IDs are bundled into a Presto
split as single-value ``Range`` objects (the number of row IDs per split
is controlled by the value of ``accumulo.index_rows_per_split``) and
passed to a Presto worker to be configured in the ``BatchScanner`` which
scans the data table.

.. code-block:: sql

    SELECT * FROM myschema.scientists WHERE age = 109;

.. code-block:: none

     recordkey |     name     | age |  birthday
    -----------+--------------+-----+------------
     row1      | Grace Hopper | 109 | 1906-12-09
    (1 row)

Loading Data
------------

The Accumulo connector supports loading data via INSERT statements, however
this method tends to be low-throughput and should not be relied on when throughput
is a concern. Instead, users of the connector should use the ``PrestoBatchWriter``
tool that is provided as part of the presto-accumulo-tools subproject in the
`presto-accumulo repository <https://github.com/bloomberg/presto-accumulo>`_.

The ``PrestoBatchWriter`` is a wrapper class for the typical ``BatchWriter`` that
leverages the Presto/Accumulo metadata to write Mutations to the main data table.
In particular, it handles indexing the given mutations on any indexed columns.
Usage of the tool is provided in the README in the `repository <https://github.com/bloomberg/presto-accumulo>`_.

External Tables
---------------

By default, the tables created using SQL statements via Presto are
*internal* tables, that is both the Presto table metadata and the
Accumulo tables are managed by Presto. When you create an internal
table, the Accumulo table is created as well. You will receive an error
if the Accumulo table already exists. When an internal table is dropped
via Presto, the Accumulo table (and any index tables) are dropped as
well.

To change this behavior, set the ``external`` property to ``true`` when
issuing the ``CREATE`` statement. This will make the table an *external*
table, and a ``DROP TABLE`` command will **only** delete the metadata
associated with the table.  If the Accumulo tables do not already exist,
they will be created by the connector.

Creating an external table *will* set any configured locality groups as well
as the iterators on the index and metrics tables (if the table is indexed).
In short, the only difference between an external table and an internal table
is the connector will delete the Accumulo tables when a ``DROP TABLE`` command
is issued.

External tables can be a bit more difficult to work with, as the data is stored
in an expected format. If the data is not stored correctly, then you're
gonna have a bad time. Users must provide a ``column_mapping`` property
when creating the table. This creates the mapping of Presto column name
to the column family/qualifier for the cell of the table. The value of the
cell is stored in the ``Value`` of the Accumulo key/value pair. By default,
this value is expected to be serialized using Accumulo's *lexicoder* API.
If you are storing values as strings, you can specify a different serializer
using the ``serializer`` property of the table. See the section on
`Table Properties <#table-properties>`__ for more information.

Next, we create the Presto external table.

.. code-block:: sql

    CREATE TABLE external_table (
      a VARCHAR,
      b BIGINT,
      c DATE
    )
    WITH (
      column_mapping = 'a:md:a,b:md:b,c:md:c',
      external = true,
      index_columns = 'b,c',
      locality_groups = 'foo:b,c'
    );

After creating the table, usage of the table continues as usual:

.. code-block:: sql

    INSERT INTO external_table VALUES
    ('1', 1, DATE '2015-03-06'),
    ('2', 2, DATE '2015-03-07');

.. code-block:: sql

    SELECT * FROM external_table;

.. code-block:: none

     a | b |     c
    ---+---+------------
     1 | 1 | 2015-03-06
     2 | 2 | 2015-03-06
    (2 rows)

.. code-block:: sql

    DROP TABLE external_table;

After dropping the table, the table will still exist in Accumulo because it is *external*.

.. code-block:: none

    root@default> tables
    accumulo.metadata
    accumulo.root
    external_table
    external_table_idx
    external_table_idx_metrics
    trace

If we wanted to add a new column to the table, we can create the table again and specify a new column.
Any existing rows in the table will have a value of NULL. This command will re-configure the Accumulo
tables, setting the locality groups and iterator configuration.

.. code-block:: sql

    CREATE TABLE external_table (
      a VARCHAR,
      b BIGINT,
      c DATE,
      d INTEGER
    )
    WITH (
      column_mapping = 'a:md:a,b:md:b,c:md:c,d:md:d',
      external = true,
      index_columns = 'b,c,d',
      locality_groups = 'foo:b,c,d'
    );

    SELECT * FROM external_table;

.. code-block:: sql

     a | b |     c      |  d
    ---+---+------------+------
     1 | 1 | 2015-03-06 | NULL
     2 | 2 | 2015-03-07 | NULL
    (2 rows)

Table Properties
----------------

Table property usage example:

.. code-block:: sql

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    )
    WITH (
      column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
      index_columns = 'name,age'
    );

==================== ================ ======================================================================================================
Property Name        Default Value    Description
==================== ================ ======================================================================================================
``column_mapping``   (generated)      Comma-delimited list of column metadata: ``col_name:col_family:col_qualifier,[...]``.
                                      Required for external tables.  Not setting this property results in auto-generated column names.
``index_columns``    (none)           A comma-delimited list of Presto columns that are indexed in this table's corresponding index table
``external``         ``false``        If true, Presto will only do metadata operations for the table.
                                      Otherwise, Presto will create and drop Accumulo tables where appropriate.
``locality_groups``  (none)           List of locality groups to set on the Accumulo table. Only valid on internal tables.
                                      String format is locality group name, colon, comma delimited list of column families in the group.
                                      Groups are delimited by pipes. Example: ``group1:famA,famB,famC|group2:famD,famE,famF|etc...``
``row_id``           (first column)   Presto column name that maps to the Accumulo row ID.
``serializer``       ``default``      Serializer for Accumulo data encodings. Can either be ``default``, ``string``, ``lexicoder``
                                      or a Java class name. Default is ``default``,
                                      i.e. the value from ``AccumuloRowSerializer.getDefault()``, i.e. ``lexicoder``.
``scan_auths``       (user auths)     Scan-time authorizations set on the batch scanner.
==================== ================ ======================================================================================================

Session Properties
------------------

You can change the default value of a session property by using :doc:`/sql/set-session`.
Note that session properties are prefixed with the catalog name::

    SET SESSION accumulo.column_filter_optimizations_enabled = false;

======================================== ============= =======================================================================================================
Property Name                            Default Value Description
======================================== ============= =======================================================================================================
``optimize_locality_enabled``            ``true``      Set to true to enable data locality for non-indexed scans
``optimize_split_ranges_enabled``        ``true``      Set to true to split non-indexed queries by tablet splits. Should generally be true.
``optimize_index_enabled``               ``true``      Set to true to enable usage of the secondary index on query
``index_rows_per_split``                 ``10000``     The number of Accumulo row IDs that are packed into a single Presto split
``index_threshold``                      ``0.2``       The ratio between number of rows to be scanned based on the index over the total number of rows.
                                                       If the ratio is below this threshold, the index will be used.
``index_lowest_cardinality_threshold``   ``0.01``      The threshold where the column with the lowest cardinality will be used instead of computing an
                                                       intersection of ranges in the index. Secondary index must be enabled.
``index_metrics_enabled``                ``true``      Set to true to enable usage of the metrics table to optimize usage of the index
``scan_username``                        (config)      User to impersonate when scanning the tables. This property trumps the ``scan_auths`` table property.
======================================== ============= =======================================================================================================

Adding Columns
--------------

Adding a new column to an existing table cannot be done today via
``ALTER TABLE [table] ADD COLUMN [name] [type]`` because of the additional
metadata required for the columns to work; the column family, qualifier,
and if the column is indexed.

Instead, you can use one of the utilities in the
`presto-accumulo-tools <https://github.com/bloomberg/presto-accumulo/tree/master/presto-accumulo-tools>`__
sub-project of the ``presto-accumulo`` repository.  Documentation and usage can be found in the README.

Serializers
-----------

The Presto connector for Accumulo has a pluggable serializer framework
for handling I/O between Presto and Accumulo. This enables end-users the
ability to programatically serialized and deserialize their special data
formats within Accumulo, while abstracting away the complexity of the
connector itself.

There are two types of serializers currently available; a ``string``
serializer that treats values as Java ``String`` and a ``lexicoder``
serializer that leverages Accumulo's Lexicoder API to store values. The
default serializer is the ``lexicoder`` serializer, as this serializer
does not require expensive conversion operations back and forth between
``String`` objects and the Presto types -- the cell's value is encoded as a
byte array.

Additionally, the ``lexicoder`` serializer does proper lexigraphical ordering of
numerical types like ``BIGINT`` or ``TIMESTAMP``.  This is essential for the connector
to properly leverage the secondary index when querying for data.

You can change the default the serializer by specifying the
``serializer`` table property, using either ``default`` (which is
``lexicoder``), ``string`` or ``lexicoder`` for the built-in types, or
you could provide your own implementation by extending
``AccumuloRowSerializer``, adding it to the Presto ``CLASSPATH``, and
specifying the fully-qualified Java class name in the connector configuration.

.. code-block:: sql

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    )
    WITH (
      column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
      serializer = 'default'
    );

.. code-block:: sql

    INSERT INTO myschema.scientists VALUES
    ('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
    ('row2', 'Alan Turing', 103, DATE '1912-06-23' );

.. code-block:: none

    root@default> scan -t myschema.scientists
    row1 metadata:age []    \x08\x80\x00\x00\x00\x00\x00\x00m
    row1 metadata:date []    \x08\x7F\xFF\xFF\xFF\xFF\xFF\xA6\x06
    row1 metadata:name []    Grace Hopper
    row2 metadata:age []    \x08\x80\x00\x00\x00\x00\x00\x00g
    row2 metadata:date []    \x08\x7F\xFF\xFF\xFF\xFF\xFF\xAD\xED
    row2 metadata:name []    Alan Turing

.. code-block:: sql

    CREATE TABLE myschema.stringy_scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    )
    WITH (
      column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
      serializer = 'string'
    );

.. code-block:: sql

    INSERT INTO myschema.stringy_scientists VALUES
    ('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
    ('row2', 'Alan Turing', 103, DATE '1912-06-23' );

.. code-block:: none

    root@default> scan -t myschema.stringy_scientists
    row1 metadata:age []    109
    row1 metadata:date []    -23034
    row1 metadata:name []    Grace Hopper
    row2 metadata:age []    103
    row2 metadata:date []    -21011
    row2 metadata:name []    Alan Turing

.. code-block:: sql

    CREATE TABLE myschema.custom_scientists (
      recordkey VARCHAR,
      name VARCHAR,
      age BIGINT,
      birthday DATE
    )
    WITH (
      column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
      serializer = 'my.serializer.package.MySerializer'
    );

Metadata Management
-------------------

Metadata for the Presto/Accumulo tables is stored in ZooKeeper. You can
(and should) issue SQL statements in Presto to create and drop tables.
This is the easiest method of creating the metadata required to make the
connector work. It is best to not mess with the metadata, but here are
the details of how it is stored. Information is power.

A root node in ZooKeeper holds all the mappings, and the format is as
follows:

.. code-block:: none

    /metadata-root/schema/table

Where ``metadata-root`` is the value of ``zookeeper.metadata.root`` in
the config file (default is ``/presto-accumulo``), ``schema`` is the
Presto schema (which is identical to the Accumulo namespace name), and
``table`` is the Presto table name (again, identical to Accumulo name).
The data of the ``table`` ZooKeeper node is a serialized
``AccumuloTable`` Java object (which resides in the connector code).
This table contains the schema (namespace) name, table name, column
definitions, the serializer to use for the table, and any additional
table properties.

If you have a need to programmatically manipulate the ZooKeeper metadata
for Accumulo, take a look at
``com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager`` for some
Java code to simplify the process.

Converting Table from Internal to External
------------------------------------------

If your table is *internal*, you can convert it to an external table by deleting
the corresponding znode in ZooKeeper, effectively making the table no longer exist as
far as Presto is concerned.  Then, create the table again using the same DDL, but adding the
``external = true`` table property.

For example:

1. We're starting with an internal table ``foo.bar`` that was created with the below DDL.
If you have not previously defined a table property for ``column_mapping`` (like this example),
be sure to describe the table **before** deleting the metadata.  We'll need the column mappings
when creating the external table.

.. code-block:: sql

    CREATE TABLE foo.bar (a VARCHAR, b BIGINT, c DATE)
    WITH (
        index_columns = 'b,c'
    );

.. code-block:: sql

    DESCRIBE foo.bar;

.. code-block:: none

     Column |  Type   | Extra |               Comment
    --------+---------+-------+-------------------------------------
     a      | varchar |       | Accumulo row ID
     b      | bigint  |       | Accumulo column b:b. Indexed: true
     c      | date    |       | Accumulo column c:c. Indexed: true

2. Using the ZooKeeper CLI, delete the corresponding znode.  Note this uses the default ZooKeeper
metadata root of ``/presto-accumulo``

.. code-block:: none

    $ zkCli.sh
    [zk: localhost:2181(CONNECTED) 1] delete /presto-accumulo/foo/bar

3. Re-create the table using the same DDL as before, but adding the ``external=true`` property.
Note that if you had not previously defined the column_mapping, you'll need to add the property
to the new DDL (external tables require this property to be set).  The column mappings are in
the output of the ``DESCRIBE`` statement.

.. code-block:: sql

    CREATE TABLE foo.bar (
      a VARCHAR,
      b BIGINT,
      c DATE
    )
    WITH (
      column_mapping = 'a:a:a,b:b:b,c:c:c',
      index_columns = 'b,c',
      external = true
    );
