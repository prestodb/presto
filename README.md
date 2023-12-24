# Presto

Presto is a distributed SQL query engine for big data.

See the [User Manual](https://prestodb.github.io/docs/current/) for deployment instructions and end user documentation.

## Requirements

* Mac OS X or Linux
* Java 8 Update 151 or higher (8u151+), 64-bit. Both Oracle JDK and OpenJDK are supported.
* Maven 3.3.9+ (for building)
* Python 2.4+ (for running with the launcher script)

## Presto native and Velox

[Presto native](https://github.com/prestodb/presto/tree/master/presto-native-execution) is a C++ rewrite of Presto worker. [Presto native](https://github.com/prestodb/presto/tree/master/presto-native-execution) uses [Velox](https://github.com/facebookincubator/velox) as its primary engine to run presto workloads.

[Velox](https://github.com/facebookincubator/velox) is a C++ database library which provides reusable, extensible, and high-performance data processing components.

Check out [building instructions](https://github.com/prestodb/presto/tree/master/presto-native-execution#building) to get started.


## Contributing!

Please refer to the [contribution guidelines](https://github.com/prestodb/presto/blob/master/CONTRIBUTING.md) to get started

## Questions?

[Please join our Slack channel and ask in `#dev`](https://communityinviter.com/apps/prestodb/prestodb).