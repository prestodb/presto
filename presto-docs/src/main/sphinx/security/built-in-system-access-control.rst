==============================
Built-in System Access Control
==============================

A system access control plugin enforces authorization at a global level,
before any connector level authorization. You can either use one of the built-in
plugins in Presto or provide your own by following the guidelines in
:doc:`/develop/system-access-control`. Presto offers three built-in plugins:

================================================== ============================================================
Plugin Name                                        Description
================================================== ============================================================
``allow-all`` (default value)                      All operations are permitted.

``read-only``                                      Operations that read data or metadata are permitted, but
                                                   none of the operations that write data or metadata are
                                                   allowed. See :ref:`read-only-system-access-control` for
                                                   details.

``file``                                           Authorization checks are enforced using a config file
                                                   specified by the configuration property ``security.config-file``.
                                                   See :ref:`file-based-system-access-control` for details.
================================================== ============================================================

Allow All System Access Control
===============================

All operations are permitted under this plugin. This plugin is enabled by default.

.. _read-only-system-access-control:

Read Only System Access Control
===============================

Under this plugin, you are allowed to execute any operation that reads data or
metadata, such as ``SELECT`` or ``SHOW``. Setting system level or catalog level
session properties is also permitted. However, any operation that writes data or
metadata, such as ``CREATE``, ``INSERT`` or ``DELETE``, is prohibited.
To use this plugin, add an ``etc/access-control.properties``
file with the following contents:

.. code-block:: none

   access-control.name=read-only

.. _file-based-system-access-control:

File Based System Access Control
================================

This plugin allows you to specify access control rules in a file. To use this
plugin, add an ``etc/access-control.properties`` file containing two required
properties: ``access-control.name``, which must be equal to ``file``, and
``security.config-file``, which must be equal to the location of the config file.
For example, if a config file named ``rules.json``
resides in ``etc``, add an ``etc/access-control.properties`` with the following
contents:

.. code-block:: none

   access-control.name=file
   security.config-file=etc/rules.json

The config file is specified in JSON format.

* It contains the rules defining which catalog can be accessed by which user (see Catalog Rules below).
* The principal rules specifying what principals can identify as what users (see Principal Rules below).

This plugin currently only supports catalog access control rules and principal
rules. If you want to limit access on a system level in any other way, you
must implement a custom SystemAccessControl plugin
(see :doc:`/develop/system-access-control`).

Catalog Rules
-------------

These rules govern the catalogs particular users can access. The user is
granted access to a catalog based on the first matching rule read from top to
bottom. If no rule matches, access is denied. Each rule is composed of the
following fields:

* ``user`` (optional): regex to match against user name. Defaults to ``.*``.
* ``catalog`` (optional): regex to match against catalog name. Defaults to ``.*``.
* ``allow`` (required): boolean indicating whether a user has access to the catalog

.. note::

    By default, all users have access to the ``system`` catalog. You can
    override this behavior by adding a rule.

For example, if you want to allow only the user ``admin`` to access the
``mysql`` and the ``system`` catalog, allow all users to access the ``hive``
catalog, and deny all other access, you can use the following rules:

.. code-block:: json

    {
      "catalogs": [
        {
          "user": "admin",
          "catalog": "(mysql|system)",
          "allow": true
        },
        {
          "catalog": "hive",
          "allow": true
        },
        {
          "catalog": "system",
          "allow": false
        }
      ]
    }

Principal Rules
---------------

These rules serve to enforce a specific matching between a principal and a
specified user name. The principal is granted authorization as a user based
on the first matching rule read from top to bottom. If no rules are specified,
no checks will be performed. If no rule matches, user authorization is denied.
Each rule is composed of the following fields:

* ``principal`` (required): regex to match and group against principal.
* ``user`` (optional): regex to match against user name. If matched, it
  will grant or deny the authorization based on the value of ``allow``.
* ``principal_to_user`` (optional): replacement string to substitute against
  principal. If the result of the substitution is same as the user name, it will
  grant or deny the authorization based on the value of ``allow``.
* ``allow`` (required): boolean indicating whether a principal can be authorized
  as a user.

.. note::

    You would at least specify one criterion in a principal rule. If you specify
    both criteria in a principal rule, it will return the desired conclusion when
    either of criteria is satisfied.

The following implements an exact matching of the full principal name for LDAP
and Kerberos authentication:

.. code-block:: json

    {
      "catalogs": [
        {
          "allow": true
        }
      ],
      "principals": [
        {
          "principal": "(.*)",
          "principal_to_user": "$1",
          "allow": true
        },
        {
          "principal": "([^/]+)/?.*@.*",
          "principal_to_user": "$1",
          "allow": true
        }
      ]
    }

If you want to allow users to use the extractly same name as their Kerberos principal
name, and allow ``alice`` and ``bob`` to use a group principal named as
``group@example.net``, you can use the following rules.

.. code-block:: json

    {
      "catalogs": [
        {
          "allow": true
        }
      ],
      "principals": [
        {
          "principal": "([^/]+)/?.*@example.net",
          "principal_to_user": "$1",
          "allow": true
        },
        {
          "principal": "group@example.net",
          "user": "alice|bob",
          "allow": true
        }
      ]
    }
