==========================
Hive Connector Limitations
==========================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

--------------------------

SQL DELETE
^^^^^^^^^^

:doc:`/sql/delete` is only supported if the ``WHERE`` clause matches entire partitions.

CSV Format Type Limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^

When creating tables with CSV format, all columns must be defined as ``VARCHAR`` due to 
the underlying OpenCSVSerde limitations. `OpenCSVSerde <https://github.com/apache/hive/blob/master/serde/src/java/org/apache/hadoop/hive/serde2/OpenCSVSerde.java>`_ deserializes all CSV columns 
as strings only. Using any other data type will result in an error similar to the following::

  CREATE TABLE hive.csv.csv_fail ( 
    id BIGINT, 
    value INT, 
    date_col DATE
  ) with ( format = 'CSV' ) ;

.. code-block:: none

    Query failed: Hive CSV storage format only supports VARCHAR (unbounded). 
    Unsupported columns: id integer, value integer, date_col date

To work with other data types when using CSV format:

1. Create the table with all the columns as ``VARCHAR``
2. Create a view or another table that casts the columns to their desired data types

Example::

    -- First create table with VARCHAR columns
    CREATE TABLE hive.csv.csv_data (
        id VARCHAR,
        value VARCHAR,
        date_col VARCHAR
    )
    WITH (format = 'CSV');

    -- Then create a view with the proper data types
    CREATE VIEW hive.csv.csv_data_view AS
    SELECT 
        CAST(id AS BIGINT) AS id,
        CAST(value AS INT) AS value,
        CAST(date_col AS DATE) AS date_col
    FROM hive.csv.csv_data;

    -- OR another table with the proper data types
    CREATE TABLE hive.csv.csv_data_cast AS
    SELECT 
        CAST(id AS BIGINT) AS id,
        CAST(value AS INT) AS value,
        CAST(date_col AS DATE) AS date_col
    FROM hive.csv.csv_data;