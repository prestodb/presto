==============================
OpenLineage Event Listener
==============================

The OpenLineage event listener plugin emits query events in the
`OpenLineage <https://openlineage.io/>`_ format, enabling integration with
lineage tracking systems such as `Marquez <https://marquezproject.ai/>`_,
`Atlan <https://atlan.com/>`_, and `DataHub <https://datahubproject.io/>`_.

The plugin captures:

* Query start events (``START``)
* Query completion events (``COMPLETE`` or ``FAIL``)
* Input and output dataset information including column-level lineage

Installation
------------

The OpenLineage event listener plugin is bundled with Presto and requires
no additional installation.

Configuration
-------------

Create an ``etc/event-listener.properties`` file on the coordinator with the
following required properties:

.. code-block:: none

    event-listener.name=openlineage-event-listener
    openlineage-event-listener.presto.uri=http://presto-coordinator:8080
    openlineage-event-listener.transport.type=CONSOLE

Transport Types
^^^^^^^^^^^^^^^

The plugin supports two transport types for emitting OpenLineage events:

**Console Transport**

Writes OpenLineage events as JSON to stdout. Useful for debugging and
development.

.. code-block:: none

    event-listener.name=openlineage-event-listener
    openlineage-event-listener.presto.uri=http://presto-coordinator:8080
    openlineage-event-listener.transport.type=CONSOLE

**HTTP Transport**

Sends OpenLineage events to an HTTP endpoint (e.g., Marquez API).

.. code-block:: none

    event-listener.name=openlineage-event-listener
    openlineage-event-listener.presto.uri=http://presto-coordinator:8080
    openlineage-event-listener.transport.type=HTTP
    openlineage-event-listener.transport.url=http://marquez:5000
    openlineage-event-listener.transport.endpoint=/api/v1/lineage

Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 40 10 10 40
   :header-rows: 1

   * - Property
     - Required
     - Default
     - Description
   * - ``openlineage-event-listener.presto.uri``
     - Yes
     -
     - URI of the Presto server. Used for namespace rendering in OpenLineage events.
   * - ``openlineage-event-listener.transport.type``
     - No
     - ``CONSOLE``
     - Transport type for emitting events. Supported values: ``CONSOLE``, ``HTTP``.
   * - ``openlineage-event-listener.namespace``
     - No
     -
     - Override the default namespace for OpenLineage jobs. Defaults to the Presto URI with ``presto://`` scheme.
   * - ``openlineage-event-listener.job.name-format``
     - No
     - ``$QUERY_ID``
     - Format string for the OpenLineage job name. Supported placeholders: ``$QUERY_ID``, ``$USER``, ``$SOURCE``, ``$CLIENT_IP``.
   * - ``openlineage-event-listener.presto.include-query-types``
     - No
     - ``DELETE,INSERT,MERGE,UPDATE,DATA_DEFINITION``
     - Comma-separated list of query types that generate OpenLineage events. Other query types are filtered out on completion.
   * - ``openlineage-event-listener.disabled-facets``
     - No
     -
     - Comma-separated list of facets to exclude from events. Supported values: ``PRESTO_METADATA``, ``PRESTO_QUERY_STATISTICS``, ``PRESTO_QUERY_CONTEXT``.

HTTP Transport Properties
^^^^^^^^^^^^^^^^^^^^^^^^^

These properties apply when ``openlineage-event-listener.transport.type`` is set to ``HTTP``.

.. list-table::
   :widths: 40 10 10 40
   :header-rows: 1

   * - Property
     - Required
     - Default
     - Description
   * - ``openlineage-event-listener.transport.url``
     - Yes
     -
     - URL of the OpenLineage API server.
   * - ``openlineage-event-listener.transport.endpoint``
     - No
     -
     - Custom API path for receiving events.
   * - ``openlineage-event-listener.transport.api-key``
     - No
     -
     - API key for authentication. Sent as a ``Bearer`` token.
   * - ``openlineage-event-listener.transport.timeout``
     - No
     - ``5s``
     - HTTP request timeout. Accepts duration strings (e.g., ``5s``, ``30s``, ``1m``).
   * - ``openlineage-event-listener.transport.headers``
     - No
     -
     - Custom HTTP headers as comma-separated ``key:value`` pairs.
   * - ``openlineage-event-listener.transport.url-params``
     - No
     -
     - Custom URL query parameters as comma-separated ``key:value`` pairs.
   * - ``openlineage-event-listener.transport.compression``
     - No
     - ``NONE``
     - HTTP body compression. Supported values: ``NONE``, ``GZIP``.

Event Details
-------------

The plugin emits the following OpenLineage facets:

**Run Facets**

* ``processing_engine`` - Presto server version information
* ``presto_metadata`` - Query ID, transaction ID, and query plan
* ``presto_query_context`` - User, server address, environment, source, client info
* ``presto_query_statistics`` - Detailed query execution statistics (on completion only)
* ``nominalTime`` - Query start and end times (on completion only)
* ``errorMessage`` - Failure message (on failure only)

**Job Facets**

* ``jobType`` - ``BATCH`` / ``PRESTO`` / ``QUERY``
* ``sql`` - The SQL query text with dialect ``presto``

**Dataset Facets**

* ``schema`` - Column names and types for input and output datasets
* ``dataSource`` - Catalog and schema information
* ``columnLineage`` - Column-level lineage mapping from input to output columns
