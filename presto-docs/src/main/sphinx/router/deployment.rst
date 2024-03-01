=======================
Deploying Presto Router
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Installing Router
-----------------

Download the Presto router tarball, :maven_download:`router`, and unpack it.
The tarball will contain a single top-level directory,
|presto_router_release|, which we will call the *installation* directory.

Router needs a *data* directory for storing logs, etc.
We recommend creating a data directory outside of the installation directory,
which allows it to be easily preserved when upgrading Presto.

Configuring Router
------------------

Create an ``etc`` directory inside the installation directory.
Similar to the installation of Presto, this will hold the following configuration:

* Node Properties: environmental configuration specific to each node
* JVM Config: command line options for the Java Virtual Machine
* Config Properties: configuration for the Presto router
* Router Properties: configuration and rules for running the router

.. _router_node_properties:

Node Properties
^^^^^^^^^^^^^^^

The node properties file, ``etc/node.properties``, shares the same configuration
as the main Presto server. Details can be found at :doc:`/installation/deployment`.

.. _router_jvm_config:

JVM Config
^^^^^^^^^^

The JVM config file, ``etc/jvm.config``, contains a list of command line
options used for launching the Java Virtual Machine.

The following provides an example of ``etc/jvm.config``.

.. code-block:: none

    -ea
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+UseGCOverheadLimit
    -XX:+ExplicitGCInvokesConcurrent
    -Xmx12G

.. _config_properties:

Config Properties
^^^^^^^^^^^^^^^^^

The config properties file, ``etc/config.properties``, contains the
configuration for the Presto router web service.

The following provides an example of ``etc/config.properties``.

.. code-block:: none

    http-server.http.port=8080
    http-server.log.max-history=3
    http-server.log.max-size=500MB
    router.config-file=etc/router-config.json

If Kerberos authentication is required, adding the following configs:

.. code-block:: none

    query-tracker.http-client.authentication.enabled=true
    query-tracker.http-client.authentication.krb5.name-type=USER_NAME
    query-tracker.http-client.authentication.krb5.principal=presto@REMOTE.BIZ
    query-tracker.http-client.authentication.krb5.remote-service-name=HTTP/PRESTO@REMOTE.BIZ
    query-tracker.http-client.authentication.krb5.service-principal-pattern=PATTERN

.. _router_properties:

Router Properties
^^^^^^^^^^^^^^^^^

Router properties contain some specific rules to run the router.

The following provides an example of ``etc/router-config.json``.

.. code-block:: none

    {
      "groups": [
        {
          "name": "all",
          "members": ["http://127.0.0.1:61381", "http://127.0.0.1:61382"],
          "weights": [1, 5]
        }
      ],
      "selectors": [
        {
          "targetGroup": "all"
        }
      ],
      "scheduler": "RANDOM_CHOICE",
      "predictor": "http://127.0.0.1:8000/v1"
    }

These properties requires some explanation:

* ``groups``:
  The groups of Presto clusters. Each group contains a required ``name`` and
  required ``members``. Each group may also contain an optional ``weights``
  field for members. Weights are used for some weights-related scheduling
  algorithms such as weighted random choice.

* ``selectors``:
  The selectors to select specific Presto clusters. Allow ``source``, ``user``,
  ``clientTags``, and ``targetGroup``.

* ``scheduler``:
  The type of scheduler used in the router service. See :doc:`/router/scheduler`
  for details.
  The default is *RANDOM_CHOICE*.

* ``predictor``:
  An optional URI for the query predictor. The router uses the URI to fetch
  query resource usage information from the predictor for scheduling.
  The default is *http://127.0.0.1:8000/v1*.

.. _running_router:

Running Router
--------------

The installation directory contains the launcher script in ``bin/launcher``.
Router can be started as a a daemon by running the following:

.. code-block:: none

    bin/launcher start

Alternatively, it can be run in the foreground, with the logs and other
output being written to stdout/stderr (both streams should be captured
if using a supervision system like daemontools):

.. code-block:: none

    bin/launcher run
