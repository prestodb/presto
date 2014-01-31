====================================
Logical Operators
====================================

Presto supports the following logical operators:

* ``AND`` - Commutative logical operator. "a AND b" evaluates to TRUE if both a and b are TRUE.
* ``OR`` - Commutative logical operator. "a OR b" evaluates to TRUE is either a or b are TRUE.
* ``NOT`` - The negation of logical complement operator. ``NOT`` takes an express and returns its complement. If a is TRUE, NOT a evaluates to FALSE.  If a is FALSE, NOT a evaluates to TRUE.


Effect of NULL on Logical Operators
===================================

The result of an ``AND`` comparison may be ``NULL`` if one or both
sides of the expression are ``NULL``. If at least one side of an
``AND`` operator is ``FALSE`` the expression evaluates to ``FALSE``.

.. code:: sql

    presto:default> select CAST(NULL as BOOLEAN) AND TRUE;
     _col0 
    ------- 
     NULL

    presto:default> select CAST(NULL as BOOLEAN) AND FALSE;
     _col0 
    -------
     false

    presto:default> select CAST(NULL as BOOLEAN) AND CAST(NULL as BOOLEAN);
     _col0 
    -------
     NULL  

The result of an ``OR`` comparison may be ``NULL`` if one or both
sides of the expression are ``NULL``.  If at least one side of an
``OR`` operator is ``TRUE`` the expression evaluates to ``TRUE``.

.. code:: sql

    presto:default> select CAST(NULL as BOOLEAN) OR CAST(NULL as BOOLEAN);
     _col0 
    -------
     NULL  

    presto:default> select CAST(NULL as BOOLEAN) OR FALSE;
     _col0 
    -------
     NULL  

    presto:default> select CAST(NULL as BOOLEAN) OR TRUE;
     _col0 
    -------
     true  

The logical complement of NULL is NULL as shown in the following example:

.. code:: sql

    presto:default> select NOT CAST(NULL AS BOOLEAN);
     _col0 
    -------
     NULL 