========
SET ROLE
========

Synopsis
--------

.. code-block:: none

    SET ROLE ( role | ALL | NONE )

Description
-----------

``SET ROLE`` sets the enabled role for the current session in the current catalog.

``SET ROLE role`` enables a single specified role for the current session.
For the ``SET ROLE role`` statement to succeed, the user executing it should
have a grant for the given role.

``SET ROLE ALL`` enables all roles that the current user has been granted for the
current session.

``SET ROLE NONE`` disables all the roles granted to the current user for the
current session.

Limitations
-----------

Some connectors do not support role management.
See connector documentation for more details.

See Also
--------

:doc:`create-role`, :doc:`drop-role`, :doc:`grant-roles`, :doc:`revoke-roles`
