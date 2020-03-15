===========================
Function Namespace Managers
===========================

.. warning::

    This is an experimental feature being actively developed. The way
    Function Namespace Managers are configured might be changed.

Function namespace managers support storing and retrieving SQL
functions, allowing the Presto engine to perform actions such as
creating, altering, deleting functions.

A function namespace is in the format of ``catalog.schema`` (e.g.
``example.test``). It can be thought of as a schema for storing
functions. However, it is not a full fledged schema as it does not
support storing tables and views, but only functions.

Each Presto function, whether built-in or user-defined, resides in
a function namespace. All built-in functions reside in the
``presto.default`` function namespace. The qualified function name of
a function is the function namespace in which it reside followed by
its function name (e.g. ``example.test.func``). Built-in functions can
be referenced in queries with their function namespaces omitted, while
user-defined functions needs to be referenced by its qualified function
name. A function is uniquely identified by its qualified function name
and parameter type list.

Each function namespace manager binds to a catalog name and manages all
functions within that catalog. Using the catalog name of an existing
connector is discouraged, as the behavior is not defined nor tested,
and will be disallowed in the future.

Currently, those catalog names do not correspond to real catalogs.
They cannot be specified as the catalog in a session, nor do they
support :doc:`/sql/create-schema`, :doc:`/sql/alter-schema`,
:doc:`/sql/drop-schema`, or :doc:`/sql/show-schemas`. Instead,
namespaces can be added using the methods described below.


Configuration
-------------

Presto currently stores all function namespace manager related
information in MySQL.

To instantiate a MySQL-based function namespace manager that manages
catalog ``example``, administrator needs to first have a running MySQL
server. Suppose the MySQL server can be reached at ``localhost:1080``,
add a file ``etc/function-namespace/example.properties`` with the
following contents::

    function-namespace-manager.name=mysql
    database-url=localhost:1080
    function-namespaces-table-name=example_function_namespaces
    functions-table-name=example_sql_functions

When Presto first starts with the above MySQL function namespace
manager configuration, two MySQL tables will be created if they do
not exist.

- ``example_function_namespaces`` stores function namespaces of
  the catalog ``example``.
- ``example_sql_functions`` stores SQL-invoked functions of the
  catalog ``example``.

Multiple function namespace managers can be instantiated by placing
multiple properties files under ``etc/function-namespace``. They
may be configured to use the same tables. If so, each manager will
only create and interact with entries of the catalog to which it binds.

To create a new function namespace, insert into the
``example_function_namespaces`` table::

    INSERT INTO example_function_namespaces (catalog_name, schema_name)
    VALUES('example', 'test');


Configuration Reference
-----------------------

``function-namespace-manager.name`` is the type of the function namespace manager to instantiate. Currently, only ``mysql`` is supported.

The following table lists all configuration properties supported by the MySQL function namespace manager.

=========================================== ==================================================================================================
Name                                        Description
=========================================== ==================================================================================================
``database-url``                            The URL of the MySQL database used by the MySQL function namespace manager.
``function-namespaces-table-name``          The name of the table that stores all the function namespaces managed by this manager.
``functions-table-name``                    The name of the table that stores all the functions managed by this manager.
=========================================== ==================================================================================================
