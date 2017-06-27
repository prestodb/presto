=========
DROP ROLE
=========

Synopsis
--------

.. code-block:: none

    DROP ROLE role_name [ IN catalog ]

Description
-----------

``DROP ROLE`` drops the specified role in ``catalog`` or in the
current catalog if ``catalog`` is not specified.

For ``DROP ROLE`` statement to succeed, the user executing it should possess
admin privileges for the given role.

Examples
--------

Drop role ``admin`` ::

    DROP ROLE admin;

Drop role ``foo`` in catalog ``bar``::

    DROP ROLE foo IN bar;

Limitations
-----------

Some connectors do not support role management.
See connector documentation for more details.

See Also
--------

:doc:`create-role`, :doc:`set-role`, :doc:`grant-roles`, :doc:`revoke-roles`
