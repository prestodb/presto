=====
GRANT
=====

Synopsis
--------

.. code-block:: none

    GRANT ( privilege [, ...] | ( ALL PRIVILEGES ) )
    ON [ TABLE ] table_name TO ( user | USER user | ROLE role )
    [ WITH GRANT OPTION ]

Description
-----------

Grants the specified privileges to the specified grantee.

Specifying ``ALL PRIVILEGES`` grants :doc:`delete`, :doc:`insert` and :doc:`select` privileges.

Specifying ``ROLE PUBLIC`` grants privileges to the ``PUBLIC`` role and hence to all users.

The optional ``WITH GRANT OPTION`` clause allows the grantee to grant these same privileges to others.

For ``GRANT`` statement to succeed, the user executing it should possess the specified privileges as well as the ``GRANT OPTION`` for those privileges.

Examples
--------

Grant ``INSERT`` and ``SELECT`` privileges on the table ``orders`` to user ``alice``::

    GRANT INSERT, SELECT ON orders TO alice;

Grant ``SELECT`` privilege on the table ``nation`` to user ``alice``, additionally allowing ``alice`` to grant ``SELECT`` privilege to others::

    GRANT SELECT ON nation TO alice WITH GRANT OPTION;

Grant ``SELECT`` privilege on the table ``orders`` to everyone::

    GRANT SELECT ON orders TO ROLE PUBLIC;

Limitations
-----------

Some connectors have no support for ``GRANT``.
See connector documentation for more details.

See Also
--------

:doc:`revoke`, :doc:`show-grants`
