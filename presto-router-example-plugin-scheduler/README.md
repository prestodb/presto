# Example - Presto Router Custom Scheduler Plugin
This package contains an example of a custom scheduler plugin.

## Add the Custom Scheduler Plugin to the Presto Router
Place the plugin jar and all the dependent jars for the plugin in the `plugin` directory relative to the Presto install directory.

The configuration file for this plugin must be `etc/router-config/router-scheduler.properties`.  
In `router-scheduler.properties`, set the name of the custom scheduler factory.  
For example, use the following line to set the custom scheduler factory name to `metricsBased`.  
``router-scheduler.name=metricsBased``

The scheduler name must be set to `CUSTOM_PLUGIN_SCHEDULER` in `etc/router-config.json`.  
    ``scheduler``: ``CUSTOM_PLUGIN_SCHEDULER``

## Main Classes:
* `RouterSchedulerPlugin` - Custom Scheduler Plugin class to be loaded by the Router plugin manager.  
  This class implements the interface `com.facebook.presto.spi.RouterPlugin`.
* `MetricsBasedSchedulerFactory` - Factory for creating specific custom scheduler.  
  This class implements the interface `com.facebook.presto.spi.SchedulerFactory`.
* `MetricsBasedScheduler` - Example custom scheduler implementing the scheduling logic for clusters.  
  This class implements the interface `com.facebook.presto.spi.router.Scheduler`.  
  Similar classes can be added implementing specific custom scheduling logic.