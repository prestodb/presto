==============
Open Telemetry
==============
OpenTelemetry is a powerful serviceability framework that helps us gain insights into the performance and behavior of the systems. It facilitates generation, collection, and management of telemetry data
such as traces and metrics to observability dashboards.

Configuration
-------------

To enable this feature set ``tracing-enabled`` to ``true`` and set the path for ``tracing-backend-url`` in ``etc/telemetry-tracing.properties`` .

Configuration properties
------------------------

============================================ ================================================================================================ ==================
Property Name                                Description                                                                                      Default Values
============================================ ================================================================================================ ==================
``tracing-factory.name``                     Unique identifier for factory implementation to be registered.                                   otel
``tracing-enabled``                          Boolean value controlling if tracing is on or off.                                               false
``tracing-backend-url``                      URL of the backend for exporting telemetry data.
``max-exporter-batch-size``                  Maximum number of spans to export in one batch.                                                  256
``max-queue-size``                           Maximum number of spans to queue before processing for export.                                   1024
``schedule-delay``                           Delay between batches of span export, controlling how frequently spans are exported.             1000
``exporter-timeout``                         How long the span exporter waits for a batch of spans to be successfully sent before timing out. 1024
``trace-sampling-ratio``                     Double between 0.0 and 1.0 to specify the percentage of queries to be traced.                    1.0
``span-sampling``                            Boolean to enable/disable sampling. If enabled, spans are only generated for major operations.   false
============================================ ================================================================================================ ==================