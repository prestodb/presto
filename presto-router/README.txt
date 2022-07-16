# Presto Router

Presto router is a service sitting in front of Presto clusters. It routes requests to Presto clusters, collects statistics from Presto clusters, and shows aggregated results in a UI.

## Running Presto router in your IDE

After building Presto, you can load the project into your IDE and run the router. In IntelliJ IDEA, use the following options to create a run configuration:

* Main Class: `com.facebook.presto.server.PrestoRouter`
* VM Options: `-Drouter.config-file=etc/router-config.json -Dnode.environment=devel`
* Working directory: `$MODULE_WORKING_DIR$` or `$MODULE_DIR$`(Depends your version of IntelliJ)
* Use classpath of module: `presto-router`

The working directory should be the `presto-router` subdirectory.

If necessary, edit the `etc/router-config.json` file with the Presto clusters' endpoints in the `groups.members` field.

## Building the Web UI

Similar to the Presto Web UI, the router Web UI is also composed of React components and is written in JSX and ES6. To update this folder after making changes, simply run:

    yarn --cwd presto-router/src/main/resources/router_ui/src install

If no JavaScript dependencies have changed (i.e., no changes to `package.json`), it is faster to run:

    yarn --cwd presto-router/src/main/resources/router_ui/src run package

To simplify iteration, you can also run in `watch` mode, which automatically re-compiles when changes to source files are detected:

    yarn --cwd presto-router/src/main/resources/router_ui/src run watch

To iterate quickly, simply re-build the project in IntelliJ after packaging is complete. Project resources will be hot-reloaded and changes are reflected on browser refresh.
