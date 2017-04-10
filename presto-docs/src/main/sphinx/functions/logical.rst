=================
Logical Operators
=================

Logical Operators
-----------------

======== ============================ =======
Operator Description                  Example
======== ============================ =======
``AND``  True if both values are true a AND b
``OR``   True if either value is true a OR b
``NOT``  True if the value is false   NOT a
======== ============================ =======

Effect of NULL on Logical Operators
-----------------------------------

The result of an ``AND`` comparison may be ``NULL`` if one or both
sides of the expression are ``NULL``. If at least one side of an
``AND`` operator is ``FALSE`` the expression evaluates to ``FALSE``::

    SELECT CAST(null AS boolean) AND true; -- null

    SELECT CAST(null AS boolean) AND false; -- false

    SELECT CAST(null AS boolean) AND CAST(null AS boolean); -- null

The result of an ``OR`` comparison may be ``NULL`` if one or both
sides of the expression are ``NULL``.  If at least one side of an
``OR`` operator is ``TRUE`` the expression evaluates to ``TRUE``::

    SELECT CAST(null AS boolean) OR CAST(null AS boolean); -- null

    SELECT CAST(null AS boolean) OR false; -- null

    SELECT CAST(null AS boolean) OR true; -- true

The following truth table demonstrates the handling of
``NULL`` in ``AND`` and ``OR``:

=========  =========  =========  =========
a          b          a AND b    a OR b
=========  =========  =========  =========
``TRUE``   ``TRUE``   ``TRUE``   ``TRUE``
``TRUE``   ``FALSE``  ``FALSE``  ``TRUE``
``TRUE``   ``NULL``   ``NULL``   ``TRUE``
``FALSE``  ``TRUE``   ``FALSE``  ``TRUE``
``FALSE``  ``FALSE``  ``FALSE``  ``FALSE``
``FALSE``  ``NULL``   ``FALSE``  ``NULL``
``NULL``   ``TRUE``   ``NULL``   ``TRUE``
``NULL``   ``FALSE``  ``FALSE``  ``NULL``
``NULL``   ``NULL``   ``NULL``   ``NULL``
=========  =========  =========  =========

The logical complement of ``NULL`` is ``NULL`` as shown in the following example::

    SELECT NOT CAST(null AS boolean); -- null

The following truth table demonstrates the handling of ``NULL`` in ``NOT``:

=========  =========
a          NOT a
=========  =========
``TRUE``   ``FALSE``
``FALSE``  ``TRUE``
``NULL``   ``NULL``
=========  =========
