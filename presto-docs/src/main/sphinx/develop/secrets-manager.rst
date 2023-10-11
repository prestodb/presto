=====================
Secrets Manager
=====================

Presto supports custom secrets manager plugins to retrieve secrets from implementations of users' choice.
By default, Hashicorp Vault is integrated as part of the presto-vault module. More implementations can be plugged into the framework by extending
the Presto SPI.


Implementation
--------------

``SecretsManagerFactory`` is responsible for creating a
``SecretsManager`` instance. It also defines a ``SecretsManager``
name which is used by the administrator in a Presto configuration.

``SecretsManager`` implementations have the responsibility to:

* Read relevant configurations for connecting to the secrets manager service.
* Fetch the secrets for all catalogs that require secrets manager.

The implementation of ``SecretsManager`` and ``SecretsManagerFactory``
must be wrapped as a plugin and installed on the Presto cluster.

Configuration
-------------

After a plugin that implements ``SecretsManager`` and
``SecretsManagerFactory`` has been installed on the coordinator, it is
configured using an ``etc/secrets-manager.properties`` file.

The ``secrets-manager.name`` property is used by Presto to find a registered
``SecretsManagerFactory`` based on the name returned by
``SecretsManagerFactory.getName()``. The remaining configuration needed by the custom implementation should be loaded from the plugin code itself.

An example of a ``secrets-manager.properties`` configuration file:

.. code-block:: none

    secrets-manager.name=custom-secrets-manager

This example configuration file defines the name custom-secrets-manager that Presto uses to find the instance created by SecretsManagerFactory.
