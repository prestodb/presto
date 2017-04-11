=====
Types
=====

The ``Type`` interface in Presto is used to implement a type in the SQL language.
Presto ships with a number of built-in types, like ``VarcharType`` and ``BigintType``.
The ``ParametricType`` interface is used to provide type parameters for types, to
allow types like ``VARCHAR(10)`` or ``DECIMAL(22, 5)``. A ``Plugin`` can provide
new ``Type`` objects by returning them from ``getTypes()`` and new ``ParametricType``
objects by returning them from ``getParametricTypes()``.

Below is a high level overview of the ``Type`` interface, for more details see the
JavaDocs for ``Type``.

* Native container type:

  All types define the ``getJavaType()`` method, frequently referred to as the
  "native container type". This is the Java type used to hold values during execution
  and to store them in a ``Block``. For example, this is the type used in
  the Java code that implements functions that produce or consume this ``Type``.

* Native encoding:

  The interpretation of a value in its native container type form is defined by its
  ``Type``. For some types, such as ``BigintType``, it matches the Java
  interpretation of the native container type (64bit 2's complement). However, for other
  types such as ``TimestampWithTimeZoneType``, which also uses ``long`` for its
  native container type, the value stored in the ``long`` is a 8byte binary value
  combining the timezone and the milliseconds since the unix epoch. In particular,
  this means that you cannot compare two native values and expect a meaningful
  result, without knowing the native encoding.

* Type signature:

  The signature of a type defines its identity, and also encodes some general
  information about the type, such as its type parameters (if it's parametric),
  and its literal parameters. The literal parameters are used in types like
  ``VARCHAR(10)``.
