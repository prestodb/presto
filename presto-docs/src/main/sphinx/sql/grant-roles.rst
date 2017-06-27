===========
GRANT ROLES
===========

Synopsis
--------

.. code-block:: none

    GRANT role [, ...]
    TO ( user | USER user | ROLE role) [, ...]
    [ GRANTED BY ( user | USER user | ROLE role | CURRENT_USER | CURRENT_ROLE ) ]
    [ WITH ADMIN OPTION ]
    [ IN catalog ]

Description
-----------

Grants the specified role(s) to the specified principal(s) in ``catalog`` or
in the current catalog if ``catalog`` is not specified.

If the ``WITH ADMIN OPTION`` clause is specified, the role(s) are granted
to the users with ``GRANT`` option.

For the ``GRANT`` statement for roles to succeed, the user executing it either should
be the role admin or should possess the ``GRANT`` option for the given role.

The optional ``GRANTED BY`` clause causes the role(s) to be granted with
the specified principal as a grantor. If the ``GRANTED BY`` clause is not
specified, the roles are granted with the current user as a grantor.

Examples
--------

Grant role ``bar`` to user ``foo`` ::

    GRANT bar TO USER foo;

Grant roles ``bar`` and ``foo`` to user ``baz`` and role ``qux`` with admin option ::

    GRANT bar, foo TO USER baz, ROLE qux WITH ADMIN OPTION;

Grant role ``bar`` to user ``foo`` in catalog ``baz`` ::

    GRANT bar TO USER foo IN baz;

Limitations
-----------

Some connectors do not support role management.
See connector documentation for more details.

See Also
--------

:doc:`create-role`, :doc:`drop-role`, :doc:`set-role`, :doc:`revoke-roles`