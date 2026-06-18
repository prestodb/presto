# Presto SQL Helpers

The ``presto-sql-helpers`` directory provides inline SQL-invoked functions for Presto.
It is not a library by itself, instead it contains two plugin modules, each packaging the functions needed for a specific cluster type.

## Module Overview

1. ``presto-sql-invoked-functions-plugin``

    - Loads inline SQL-invoked functions defined in the ``presto-sql-invoked-functions-plugin`` module.
    - Must be used in Java clusters and native-only clusters.

2. ``presto-native-sql-invoked-functions-plugin``

    - Loads inline SQL-invoked functions defined in the ``presto-native-sql-invoked-functions-plugin`` module.
    - Must be used in sidecar-enabled native clusters.

## Function Loading Rules

Each cluster must load exactly one SQL-Invoked functions plugin.
If both plugins are loaded, or if the wrong plugin is loaded, Presto will fail to start due to signature conflicts.

### Recommended Configuration

| Cluster Type                  | Plugin to load                               |
|-------------------------------|----------------------------------------------|
| Java cluster                  | `presto-sql-invoked-functions-plugin`        | 
| Native cluster (no sidecar)   | `presto-sql-invoked-functions-plugin`        |
| Native cluster (with sidecar) | `presto-native-sql-invoked-functions-plugin` |