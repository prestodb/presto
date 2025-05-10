# Example - Presto Router Custom Scheduler Plugin
This package contains an example of a custom scheduler plugin.

## Adding the custom scheduler plugin to Presto router
The custom plugin jar that is built is placed in the ``presto-router/plugin/custom-scheduler`` directory,
to be picked up by the ``presto-router``
The scheduler name must be set to ``CUSTOM_PLUGIN_SCHEDULER`` in the ``router-config.json``.
    ``scheduler``: ``CUSTOM_PLUGIN_SCHEDULER``

The name of the custom scheduler factory must be set in the property file ``router-scheduler.properties`` in ``presto-router/etc/router-config``.

## Main Classes:
* ``RouterSchedulerPlugin`` - Custom Scheduler Plugin class to be loaded by the Router plugin manager.
  This class implements the interface ``com.facebook.presto.spi.RouterPlugin``.
* ``MetricsBasedSchedulerFactory`` - Factory for creating specific custom scheduler.
  This class implements the interface ``com.facebook.presto.spi.SchedulerFactory``.
* ``MetricsBasedScheduler`` - Example custom scheduler implementing the scheduling logic for clusters.
  This class implements the interface ``com.facebook.presto.spi.router.Scheduler``.
  Similar classes can be added implementing specific custom scheduling logic.