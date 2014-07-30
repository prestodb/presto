==============
Joining Tables
==============

Table joins in Presto allow you to create queries that combine data
from multiple tables. Presto supports the following join types.

Cross Join
----------

A cross join returns the Cartesian product or all possible
combinations of two tables. Presto supports explicit cross joins with
the CROSS JOIN syntax.

.. code-block:: none

    table_name CROSS JOIN table_name

For an exmaple of a cross join run the following queries against the
``tpch`` catalog.  Selecting a row count from the ``nation`` table
returns 25 rows.

.. code-block:: none

    presto:sf1> select count(*) from nation;
     _col0 
    -------
        25 
    (1 row)

    presto:sf1> select name from nation;
	  name      
    ----------------
     ALGERIA        
     ARGENTINA      
    ...

Selecting a row count from the ``region`` table returns 5 rows.

.. code-block:: none

    presto:sf1> select count(*) from region;
     _col0 
    -------
	 5 
    (1 row)

    presto:sf1> select name from region;
	name     
    -------------
     AFRICA      
     AMERICA  
    ...

The CROSS JOIN between ``nation`` and ``region`` contains all possible
combinations of both tables or 125 rows.

.. code-block:: none

    presto:sf1> select count(*) from nation cross join region;
     _col0 
    -------
       125 
    (1 row)

    presto:sf1> select n.name, r.name from nation n cross join region r;
	  name      |    name     
    ----------------+-------------
     ALGERIA        | AMERICA     
     ALGERIA        | AFRICA      
     ARGENTINA      | AMERICA     
     ARGENTINA      | AFRICA  
    ...

Ambiguous Column Names
----------------------

When two tables in a join have columns with the same name Presto will
complain about a reference to an ambiguous column name.

.. code-block:: none
    presto:sf1> select name from nation cross join region;
    Query 20140730_101154_00047_3bk2j failed: Column 'name' is ambiguous

This ambiguity can be addressed by referencing the table name in the
select expression or by assigning an alias to a table.  The three
statements shown below solve the ambiguous column reference in the
previous statement.

.. code-block:: none
    select nation.name, region.name from nation cross join region;

    select n.name, r.name from nation as n cross join region as r;

    select n.name, r.name from nation n cross join region r;
