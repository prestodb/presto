==================
Native Sidecar plugin
==================

In a Prestissimo cluster, the Native sidecar plugin enables the Presto coordinator to utilize capabilities offered by the underlying C++ worker.
- Allows access to C++ functions, types and session properties enriching Presto's SQL surface with high-performance native operations.
- A Native plan checker, which verifies distributed Presto query plan fragments via a sidecar worker by converting it to a Velox plan fragment to ensure compatibility and detect failures early on.

Pre-requisites
-------------
Before using this plugin, ensure your setup includes:
- Presto >= 0.291, supporting SPI v2 integration.
- A Velox worker configured as sidecar deployed, reachable from the coordinator.

Configuration
-------------

- Coordinator properties:
    `coordinator-sidecar-enabled=true`
    `native-execution-enabled=true`
    `presto.default-namespace=native.default`

- Sidecar properties :
    `native-sidecar=true`
    `presto.default-namespace=native.default`

- Worker properties if not a sidecar :
    `presto.default-namespace=native.default`

The native sidecar plugin provides users the ability to use individual features of the plugin.
For eg. Users should be able to use the function registry if that’s what they want.

Function registry
-----------------

Create ``etc/function-namespace/native.properties`` to use the native function namespace manager from the ``NativeSidecarPlugin``.

Required configuration properties
------------------------

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``function-namespace-manager.name``          The function namespace manager name                                   native
``function-implementation-type``             The function namespace implementation language                        CPP
``supported-function-languages``             The function namespace implementation language                        CPP


Session properties
-----------------

Create ``etc/session-property-provider/native-worker.properties`` to use the native session property provider of the ``NativeSidecarPlugin``.

Required configuration properties
------------------------

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``session-property-provider.name``           The session property provider name                                    native-worker


Type Manager
-----------------

Create ``etc/type-managers/native.properties`` to use the native type manager of the ``NativeSidecarPlugin``.

Required configuration properties
------------------------

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``type-manager.name``                        The type manager name                                                 native


Plan checker
-----------------

Create ``etc/plan-checker-providers/native.properties`` to use the native plan checker of the ``NativeSidecarPlugin``.

Required configuration properties
------------------------

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``plan-checker-provider.name``               The plan checker provider name                                        native

Production Setup
----------------

You can place the plugin JARs in the production's ``plugins`` directory.

Alternatively, follow this method to ensure that the plugin is loaded during the Presto build.

1. Add the following to register the plugin in ``<fileSets>`` in ``presto-server/src/main/assembly/presto.xml``:

   .. code-block:: text

       <fileSet>
          <directory>${project.build.directory}/dependency/presto-native-sidecar-plugin-${project.version}</directory>
          <outputDirectory>plugin//presto-native-sidecar-plugin</outputDirectory>
       </fileSet>

2. Add the dependency on the module in ``presto-server/pom.xml``:

   .. code-block:: text

       <dependency>
           <groupId>com.facebook.presto</groupId>
           <artifactId>/presto-native-sidecar-plugin</artifactId>
           <version>${project.version}</version>
           <type>zip</type>
           <scope>provided</scope>
       </dependency>

