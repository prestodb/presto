===========
JDBC Driver
===========

Presto can be accessed from Java using the JDBC driver.
Download :maven_download:`jdbc` and add it to the class path of your Java application.
The following JDBC URL formats are supported:

.. code-block:: none

    jdbc:presto://host:port
    jdbc:presto://host:port/catalog
    jdbc:presto://host:port/catalog/schema

For example, use the following URL to connect to Presto
running on ``example.net`` port ``8080`` with the catalog ``hive``
and the schema ``sales``:

.. code-block:: none

    jdbc:presto://example.net:8080/hive/sales
