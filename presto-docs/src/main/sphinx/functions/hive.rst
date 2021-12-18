==============================
Hive Functions
==============================

Functions in Hive are categorized as either built-in or user-defined.
Built-in functions are the ones already available in Hive and user-defined
functions are custom functions for tasks like data cleansing and filtering.
Hive UDFs can be defined according to programmer's requirements.

Presto allows you to use the same UDFs that are available in your Hive deployment.
All Hive UDFs may exist in Presto or external storage. Parameters and return values
are converted between the types expected in Presto and Hive.

Note: of the three supported Hive function types (scalar, windowing, and aggregate functions),
only scalar functions are currently supported.

Hive Scalar Functions
---------------------

.. note::

    Hive has two different interfaces for writing Hive UDFs.
    One is the basic UDF interface (deprecated) and the other
    is the GenericUDF interface.

    We currently only support the Hive built-in GenericUDF interface.

+---------------+-----------------------------------+-----------------------------------+
|               | Hive Built-in Scalar Function     | External Hive Scalar Function     |
+---------------+-----------------------------------+-----------------------------------+
| udf           | get_json_object                   | facebook_get_json_object          |
+---------------+-----------------------------------+-----------------------------------+
| genericUdf    | str_to_map                        | facebook_str_to_map               |
+---------------+-----------------------------------+-----------------------------------+

See the `Hive Language Manual <https://cwiki.apache.org/confluence/display/hive/languagemanual+udf>`_ for all hive scalar functions information.

Hive Function Namespace Plugin
------------------------------

Hive function namespace plugin is enable by default, which is configured
in the ``hive.properties`` file. Deleting the ``hive.properties`` to turn it off.

Examples
--------

Use hive function namespace ``hive.default`` to call the hive function.

The following query calls hive built-in scalar function ``str_to_map``::

    SELECT hive.default.str_to_map();

