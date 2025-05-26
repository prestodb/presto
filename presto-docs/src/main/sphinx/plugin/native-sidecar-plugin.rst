==================
Native Sidecar plugin
==================

Redis HBO Provider supports loading a custom configured Redis Client for storing and retrieving historical stats for Historical Based Optimization (HBO). The Redis client is stateful and is based on
`Lettuce <https://github.com/lettuce-io/lettuce-core>`_. Both RedisClient and RedisClusterClient are supported, RedisClusterAsyncCommandsFactory is meant to be extended by the user for custom configurations.


Configuration
-------------

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

Create ``etc/session-property-provider/native.properties`` to use the native session property provider of the ``NativeSidecarPlugin``.

Required configuration properties
------------------------

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``session-property-provider.name``           The session property provider name                                    native


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

