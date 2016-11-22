===========
SHOW GRANTS
===========

Synopsis
--------

.. code-block:: none

    SHOW GRANTS ON ( [ TABLE ] table_name | ALL )

Description
-----------

List the grants for the current user on the specified table in the current catalog.

Specifying ``ALL`` lists the grants for the current user on all the tables in all the schemas in the current catalog.

The command requires the current catalog to be set.

.. note::

    Ensure that authentication has been enabled before running any of the authorization commands.

Examples
--------

List the grants for the current user on table ``orders``::

    SHOW GRANTS ON TABLE ``orders``

List the grants for the current user on all the tables in all the schemas in the current catalog::

    SHOW GRANTS ON ALL;

Limitations
-----------

Some connectors have no support for ``SHOW GRANTS``.
See connector documentation for more details.

See Also
--------

:doc:`grant`
:doc:`revoke`