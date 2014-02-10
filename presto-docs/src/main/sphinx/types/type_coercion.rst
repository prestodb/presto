=================
Type Coercion
=================

Presto will implicity convert numeric and character values to the
correct type if such a conversion is possible. Presto will not convert
between character and numeric types. For example, a query that expects
a varchar will not automatically convert a bigint value to an
equivalent varchar.

When necessary, values can be explicitly cast to a particular
type. Such type conversion is necessary when working with

CAST( value as type )

  Explicitly cast a value as a type. This can be used to cast a
  varchar to a numeric value type and vice versa.

Examples
--------

While the command ``select CONCAT( 'TEST', 123 );`` will produce an
error, the integer literal can be cast to a varchar using CAST.

.. code-block:: sql

    presto:default> select CONCAT('TEST', CAST( 123 as varchar ));
      _col0  
    ---------
     TEST123 
    (1 row)