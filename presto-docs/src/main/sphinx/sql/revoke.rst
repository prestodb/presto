======
REVOKE
======

Synopsis
--------

.. code-block:: none

    REVOKE [ GRANT OPTION FOR ]
    ( privilege [, ...] | ALL PRIVILEGES )
    ON [ TABLE ] table_name FROM ( grantee | PUBLIC )

Usage of the term ``grantee`` denotes both users and roles.

Description
-----------

Revokes the specified privileges from the specified grantee.

Specifying ``ALL PRIVILEGES`` revokes :doc:`delete`, :doc:`insert` and :doc:`select` privileges.

Specifying ``PUBLIC`` revokes privileges from the ``PUBLIC`` role. Users will retain privileges assigned to them directly or via other roles.

The optional ``GRANT OPTION FOR`` clause also revokes the privileges to grant the specified privileges.

For ``REVOKE`` statement to succeed, the user executing it should possess the specified privileges as well as the ``GRANT OPTION`` for those privileges.

.. note::

    Ensure that authentication has been enabled before running any of the authorization commands.

Examples
--------

Revoke ``INSERT`` and ``SELECT`` privileges on the table ``orders`` from user ``alice``::

    REVOKE INSERT, SELECT ON orders FROM alice;

Revoke ``SELECT`` privilege on the table ``nation`` from everyone, additionally revoking the privilege to grant ``SELECT`` privilege::

    REVOKE GRANT OPTION FOR SELECT ON nation FROM PUBLIC;

Revoke all privileges on the table ``test`` from user ``alice``::

    REVOKE ALL PRIVILEGES ON test FROM alice;

Limitations
-----------

Some connectors have no support for ``REVOKE``.
See connector documentation for more details.

See Also
--------

:doc:`grant`
:doc:`show-grants`
