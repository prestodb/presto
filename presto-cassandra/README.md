# Presto Plugin for Cassandra 2.0

This is a plugin for [Presto](http://prestodb.io/) to access a [Apache Cassandra](http://cassandra.apache.org) database.

Please note that this plugin is still in early stage.

## Deployment
After installation of Presto, you need to deploy to plugin to every presto node.

- On all presto nodes (server & worker nodes):
  - add `cassandra.properties` to `PRESTO_HOME/etc/catalog` (example see below)

## Configuration
The configuration for the Cassandra plugin is set in `PRESTO_HOME/etc/catalog/cassandra.properties`

### Minimal configuration
The `cassandra.properties` must at least contain following parameters:
```
connector.name=cassandra

# Comma separated list of contact points
# Contact points are addresses of Cassandra nodes that the driver uses 
# to discover the cluster topology. Only one contact point is required.
# THIS VALUE MUST BE ADJUSTED TO YOUR DEPLOYMENT
cassandra.contact-points=host1,host2
```

### Complete configuration
For documentation purposes, here is another example with all configuration parameters.

```
connector.name=cassandra

# Comma separated list of contact points
# Contact points are addresses of Cassandra nodes that the driver uses 
# to discover the cluster topology. Only one contact point is required.
# THIS VALUE MUST BE ADJUSTED TO YOUR DEPLOYMENT
cassandra.contact-points=host1,host2

# Port running the native Cassandra protocol
cassandra.native-protocol-port=9142

# Limit of rows to read for finding all partition keys.
# If a Cassandra table has more rows than this value, splits based on token ranges are used instead.
# Note that for larger values you may need to adjust read timeout for Cassandra 
cassandra.limit-for-partition-key-select=200

# number of splits generated if partition keys are unknown
cassandra.unpartitioned-splits=1000

# maximum number of schema cache refresh threads, i.e. maximum number of parallel requests
cassandra.max-schema-refresh-threads=10

# schema cache time to live
cassandra.schema-cache-ttl=1h

# schema refresh interval
# cached schema information will be refreshed automatically
cassandra.schema-refresh-interval=2m

# Consistency level used for Cassandra queries (ONE, TWO, QUORUM, ...)
cassandra.consistency-level=ONE

# fetch size used for Cassandra queries
# (advanced)
cassandra.fetch-size=5000      

# fetch size used for partition key select query
# (advanced)
cassandra.fetch-size-for-partition-key-select=20000

```

## Notes
- only tested with Apache Cassandra 2.0.3 (but probably works for Cassandra 1.2.x too)
- internally uses Datastax Java Driver 2.0 and CQL3
