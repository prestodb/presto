=====
GRANT
=====

Synopsis
--------

.. code-block:: none

    GRANT ( privilege [, ...] | ( ALL PRIVILEGES ) )
    ON [ TABLE ] table_name TO ( grantee | PUBLIC )
    [ WITH GRANT OPTION ]

Usage of the term ``grantee`` denotes both users and roles.

Description
-----------

Grants the specified privileges to the specified grantee.

Specifying ``ALL PRIVILEGES`` grants :doc:`delete`, :doc:`insert` and :doc:`select` privileges.

Specifying ``PUBLIC`` grants privileges to all grantees.

The optional ``WITH GRANT OPTION`` clause allows the grantee to grant these same privileges to others.

For ``GRANT`` statement to succeed, the user executing it should possess the specified privileges as well as the ``GRANT OPTION`` for those privileges.

Examples
--------

Grant :doc:`insert` and :doc:`select` privileges on the table ``orders`` to user ``alice``::

    GRANT INSERT, SELECT ON orders TO alice;

Grant :doc:`select` privilege on the table ``nation`` to user ``alice``, additionally allowing ``alice`` to grant ``SELECT`` privilege to others::

    GRANT SELECT ON nation TO alice WITH GRANT OPTION;

GRANT :doc:`select`: privilege on the table ``orders`` to everyone::

    GRANT SELECT ON orders TO PUBLIC;

Limitations
-----------

Some connectors have no support for ``GRANT``.
See connector documentation for more details.

See Also
--------

:doc:`revoke`
