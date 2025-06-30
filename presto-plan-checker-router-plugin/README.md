# Example - Presto Plan Checker Router Scheduler Plugin
This package contains a custom scheduler plugin - Presto Plan Checker Router Scheduler Plugin.

## Add the Custom Scheduler Plugin to the Presto Router
Place the plugin jar and all the dependent jars for the plugin in the `plugin` directory relative to the Presto install directory.

Create a configuration file for this plugin. The file must be named `router-scheduler.properties` and must be in the `etc/router-config/` directory relative to the Presto install directory.

Set the scheduler name to `CUSTOM_PLUGIN_SCHEDULER` in `etc/router-config.json`.  
``scheduler``: ``CUSTOM_PLUGIN_SCHEDULER``

## Main Classes:
* `RouterSchedulerPlugin` - Custom Scheduler Plugin class to be loaded by the Router plugin manager.  
  This class implements the interface `com.facebook.presto.spi.RouterPlugin`.
* `PlanCheckerRouterPluginSchedulerFactory` - Factory for creating plan checker custom scheduler.  
  This class implements the interface `com.facebook.presto.spi.SchedulerFactory`.
* `PlanCheckerRouterPluginScheduler` - Plan checker custom scheduler implementing the scheduling logic for clusters.  
  This class implements the interface `com.facebook.presto.spi.router.Scheduler`.

## Configuration:
The following configuration properties must be set in `etc/router-config/router-scheduler.properties`:

| Property Name                | Type    | Description                                                                                                        |                   
|------------------------------|---------|--------------------------------------------------------------------------------------------------------------------|
| router-scheduler.name        | String  | The name of the custom scheduler factory                                                                           |
|                              |         | Example: `router-scheduler.name=plan-checker`                                                                      |
| plan-check-clusters-uris     | String  | The URIs of the plan checker clusters.                                                                             |                                           |
| router-java-url              | String  | The router URI dedicated to java clusters.                                                                         |
| router-native-url            | String  | The router URI dedicated to native clusters.                                                                       |
| client-request-timeout       | String  | The maximum time the client will wait for a response before timing out.                                            |
|                              |         | Default : `2 minutes`                                                                                              |
| enable-java-cluster-fallback | boolean | Enables fallback to the Java clusters when the plan checker clusters are unavailable or fail to process a request. |
|                              |         | Default : `false`                                                                                                  |
