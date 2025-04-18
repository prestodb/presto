# Presto Router Custom Scheduler Plugin
This package implements third party custom schedulers to be used by Presto router

## Adding the custom scheduler plugin to presto router
The custom plugin jar that is built is placed in the presto-router/plugin/custom-scheduler directory,
to be picked up by the presto-router
The scheduler name has to be set to CUSTOM_PLUGIN_SCHEDULER in the router-config.json
    "scheduler": "CUSTOM_PLUGIN_SCHEDULER"
The name of the custom scheduler factory needs to be set in the property file router-scheduler.properties in presto-router/etc/router-config

## Main Classes:
* RouterSchedulerPlugin - Custom Scheduler Plugin class to be loaded by the Router plugin manager  
  This class implements the interface com.facebook.presto.spi.RouterPlugin.
* MetricsBasedSchedulerFactory - Factory for creating specific custom scheduler  
  This class implements the interface com.facebook.presto.spi.SchedulerFactory
* MetricsBasedScheduler - Custom scheduler implementing the scheduling logic for clusters  
  This class implements the interface com.facebook.presto.spi.router.Scheduler.  
  More similar classes can be added implementing specific custom scheduling logic.  