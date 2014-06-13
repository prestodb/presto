====================================
Comparison Operators
====================================

Presto supports the following comparison operators:

* ``<`` - Less than
* ``>`` - Greater than
* ``<=`` - Less than or equal to
* ``>=`` - Greater than or equal to
* ``=`` - Equal to
* ``<>`` or ``!=`` - Not equal to

Range Operator: BETWEEN
===============================

Presto offers an operator to test if a value is within a specified
range.  The ``BETWEEN`` operator uses the syntax ``value
BETWEEN min AND max``.

The following examples demonstrate the use of the ``BETWEEN``
operator.

.. code:: sql

    SELECT 3 BETWEEN 2 AND 6;

The statement shown above is equivalent to the following statement:

.. code:: sql

    SELECT 3 >= 2 AND 3 <= 6;

To test if a value doesn't not fall within the specified range
use ``NOT BETWEEN``:

.. code:: sql

    SELECT 3 NOT BETWEEN 2 AND 6;

The statement shown above is equivalent to the following statement:

.. code:: sql

    SELECT 3 < 2 OR 3 > 6;

The presence of NULL in a ``BETWEEN`` or ``NOT BETWEEN`` statement
will result in the statement evaluating to NULL as shown in the
following examples:

.. code:: sql

    presto:default> select null between 2 and 4;
     _col0 
    -------
     NULL

    presto:default> select 2 between null and 6;
     _col0 
    -------
     NULL

The ``BETWEEN`` and ``NOT BETWEEN`` operators can also be used to
evaluate string arguments:

.. code:: sql

    presto:default> select 'Paul' between 'John' and 'Ringo';
     _col0 
    -------
     true

Not that the value, min, and max parameters to ``BETWEEN`` and ``NOT
BETWEEN`` must be the same type.  For example, Presto will produce an
error if you ask it if John is between 2.3 and 35.2.

IS NULL and IS NOT NULL
=======================

Presto supports both the ``IS NULL`` and ``IS NOT NULL`` operators to
test whether a value is null or undefined.  Both operators work for
all data types.

The following statement tests ``NULL`` with ``IS NULL`` and evaluates
to true.

.. code:: sql

    presto:default> select NULL IS NULL;
     _col0 
    -------
     true 

.. code:: sql

    presto:default> select 3.0 IS NOT NULL;
     _col0 
    -------
     true

IS DISTINCT FROM and IS NOT DISTINCT FROM
=========================================

In SQL a ``NULL`` value signifies an unknown value, so any comparison
involving a ``NULL`` will produce ``NULL``.  The  ``IS DISTINCT FROM``
and ``IS NOT DISTINCT FROM`` operators treat ``NULL`` as a known value
and both operators guarantee either a true or false outcome even in
the presence of ``NULL`` input.

The following examples demonstrate the use of ``IS DISTINCT FROM`` and
``IS NOT DISTINCT FROM``:

.. code:: sql

    presto:default> select NULL IS DISTINCT FROM NULL;
     _col0 
    -------
     false 

    presto:default> select NULL IS NOT DISTINCT FROM NULL;
     _col0 
    -------
     true 

In the example shown above, a ``NULL`` value is not considered
distinct from ``NULL``.  When you are comparing values which may
include ``NULL`` use these operators to guarantee either a ``TRUE`` or
``FALSE`` result.


The following truth table demonstrate the handling of ``NULL`` in
``IS DISTINCT FROM`` and ``IS NOT DISTINCT FROM``:

========  ========  =========  =========  ===============  ===================
A         B         A = B      A <> B     A IS DISTINCT B  A IS NOT DISTINCT B
========  ========  =========  =========  ===============  ===================
``1``     ``1``     ``TRUE``   ``FALSE``  ``FALSE``        ``TRUE``
``1``     ``2``     ``FALSE``  ``TRUE``   ``TRUE``         ``FALSE``
``1``     ``NULL``  ``NULL``   ``NULL``   ``TRUE``         ``FALSE``
``NULL``  ``NULL``  ``NULL``   ``NULL``   ``FALSE``        ``TRUE``
========  ========  =========  =========  ===============  ===================
