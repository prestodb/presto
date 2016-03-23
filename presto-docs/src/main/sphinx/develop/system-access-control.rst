===================
SystemAccessControl
===================

Presto separates the concept of the principal who authenticates to the
coordinator from the username that is responsible for running queries. When
running the Presto CLI, for example, the Presto username can be specified using
the --user option.

By default, the Presto coordinator allows any principal to run queries as any
Presto user. In a secure environment, this is probably not desirable behavior.

Implementation
--------------

This behavior can be customized by implementing the
``SystemAccessControlFactory`` and ``SystemAccessControl`` interfaces.

``SystemAccessControlFactory`` is responsible for creating a
``SystemAccessControl`` instance. It also defines a ``SystemAccessControl``
name which is used by the administrator in a Presto configuration.

``SystemAccessControl`` implementations have two responsibilities:
 * Verifying whether or not a given Kerberos principal is authorized to execute
   queries as a specific user.
 * Determining whether or not a given user can alter values for a given system
   property.

The implementation of ``SystemAccessControl`` and
``SystemAccessControlFactory`` must be wrapped as a plugin and installed on the
Presto cluster.

.. _system-access-control-configuration:

Configuration
-------------

Once a plugin that implements ``SystemAccessControl`` and
``SystemAccessControlFactory`` has been installed on the coordinator, it is
configured using an access-controls.properties file. All of the properties
other than ``access-control.name`` are specific to the ``SystemAccessControl``
plugin.

.. code-block:: none

  access-control.name=custom-access-control
  custom-property1=custom-value1
  custom-property2=custom-value2
  ...

The ``access-control.name`` property is used by Presto to find a registered
``SystemAccessControlFactory``. The remaining properties are passed as a map to
``SystemAccessControlFactory.create(Map<String, String> config)``.
