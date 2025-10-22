==============
Native Sidecar
==============

Use the native sidecar plugin in a native cluster to extend the capabilities of the cluster by allowing the underlying execution engine to be the source of truth.
All supported functions, types and session properties are retrieved directly from the sidecar - a specialized worker node equipped with enhanced capabilities.
Only features supported by the native execution engine are exposed during function resolution, resolving session properties and the types supported.

The native sidecar plugin also provides a native plan checker that validates compatibility between Presto and the native execution engine
by sending Presto plan fragments to a sidecar endpoint `v1/velox/plan`, where they are translated into Velox compatible plan fragments.
If any incompatibilities or errors are detected during the translation, they are surfaced immediately, allowing Presto to fail fast before allocating resources or scheduling execution.

To use the sidecar functionalities, at least one sidecar worker must be present in the cluster. The system supports flexible configurations: a mixed setup with both sidecar
and regular C++ workers, or a cluster composed entirely of sidecar workers. A worker can be configured as a sidecar by adding the properties listed in :ref:`sidecar-worker-properties` section.

Configuration
-------------

Coordinator properties
^^^^^^^^^^^^^^^^^^^^^^
To enable sidecar support on the coordinator, add the following properties to your coordinator configuration:

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``coordinator-sidecar-enabled``              Enables sidecar in the coordinator                                    true
``native-execution-enabled``                 Enables native execution                                              true
``presto.default-namespace``                 Sets the default function namespace                                   `native.default`
``plugin.dir``                               Specifies which directory under installation root                     `{root-directory}/native-plugins/`
                                             to scan for plugins at startup.
============================================ ===================================================================== ==============================

.. _sidecar-worker-properties:

Sidecar worker properties
^^^^^^^^^^^^^^^^^^^^^^^^^
Enable sidecar functionality with:

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``native-sidecar``                           Enables a sidecar worker                                              true
``presto.default-namespace``                 Sets the default function namespace                                   `native.default`
============================================ ===================================================================== ==============================

Regular worker properties
^^^^^^^^^^^^^^^^^^^^^^^^^
For regular workers (not acting as sidecars), configure:

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``presto.default-namespace``                 Sets the default function namespace                                   `native.default`
============================================ ===================================================================== ==============================

The Native Sidecar plugin is designed to run with all its components enabled. Individual configuration properties must be specified in conjunction with one another to ensure the plugin operates as intended.
While the Native Sidecar plugin allows modular configuration, the recommended usage is to enable all the components for full functionality.

Function registry
-----------------

These properties must be configured in ``etc/function-namespace/native.properties`` to use the function namespace manager from the ``NativeSidecarPlugin``.

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``function-namespace-manager.name``          Identifier used to register the function namespace manager            `native`
``function-implementation-type``             Indicates the language in which functions in this namespace           CPP
                                             are implemented.
``supported-function-languages``             Languages supported by the namespace manager.                         CPP
============================================ ===================================================================== ==============================

Session properties
------------------

These properties must be configured in ``etc/session-property-providers/native-worker.properties`` to use the session property provider of the ``NativeSidecarPlugin``.

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``session-property-provider.name``           Identifier for the session property provider backed by the sidecar.   `native-worker`
                                             Enables discovery of supported session properties in native engine.
============================================ ===================================================================== ==============================

Type Manager
-----------------

These properties must be configured in ``etc/type-managers/native.properties`` to use the type manager of the ``NativeSidecarPlugin``.

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``type-manager.name``                        Identifier for the type manager. Registers types                      `native`
                                             supported by the native engine.
============================================ ===================================================================== ==============================

Plan checker
-----------------

These properties must be configured in ``etc/plan-checker-providers/native.properties`` to use the native plan checker of the ``NativeSidecarPlugin``.

============================================ ===================================================================== ==============================
Property Name                                Description                                                           Value
============================================ ===================================================================== ==============================
``plan-checker-provider.name``               Identifier for the plan checker. Enables validation of Presto         `native`
                                             query plans against native engine, ensuring execution compatibility.
============================================ ===================================================================== ==============================

