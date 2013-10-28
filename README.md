# Presto

Presto is a distributed SQL query engine for big data.

See the [User Manual](http://facebook.github.io/presto/docs/current/) for end user documentation.

## Requirements

* Mac OS X or Linux
* Java 7, 64-bit
* Maven 3 (for building)
* Python 2.4+ (for running with the launcher script)

## Building Presto

Presto is a standard Maven project. Simply run the following command from the project root directory:

    mvn clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Presto has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    mvn clean install -DskipTests

## Running Presto in your IDE

### Overview

After building Presto for the first time, you can load the project into your IDE and run the server. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Presto is a standard Maven project, you can import it into your IDE using the root `pom.xml` file.

Presto comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: `com.facebook.presto.server.PrestoServer`
* VM Options: `-ea -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties`
* Working directory: `$MODULE_DIR$`
* Use classpath of module: `presto-server`

The working directory should be the `presto-server` subdirectory. In IntelliJ, using `$MODULE_DIR$` accomplishes this automatically.

Additionally, the Hive plugin needs to be configured with location of your Hive metastore Thrift service. Add the following to the list of VM options, replacing `localhost:9083` with the correct host and port:

    -Dhive.metastore.uri=thrift://localhost:9083

### Using SOCKS for Hive or HDFS

If your Hive metastore or HDFS cluster is not directly accessible to your local machine, you can use SSH port forwarding to access it. Setup a dynamic SOCKS proxy with SSH listening on local port 1080:

    ssh -v -N -D 1080 server

Then add the following to the list of VM options:

    -Dhive.metastore.thrift.client.socks-proxy=localhost:1080

### Running CLI

Start the CLI to connect to the server and run SQL queries:

    presto-cli/target/presto-cli-*-executable.jar

Run a query to see the nodes in the cluster:

    SELECT * FROM sys.node;

In the sample configuration, the Hive connector is mounted in the `hive` catalog, so you can run the following queries to show the tables in the Hive database `default`:

    SHOW TABLES FROM hive.default;

## Deploying Presto

### Installing Presto

Building Presto will produce a tarball for the service that is ready for installation. The tarball name includes the Presto version. For example, Presto version `0.50-SNAPSHOT` will produce the following tarball:

    presto-server/target/presto-server-0.50-SNAPSHOT.tar.gz

The above tarball will contain a single top-level directory, which we will call the _installation_ directory:

    presto-server-0.50-SNAPSHOT

Presto needs a _data_ directory for storing logs, local metadata, etc. We recommend creating a data directory outside of the installation directory, which allows it to be easily preserved when upgrading Presto.

### Configuring Presto

Create an `etc` directory inside the installation directory. This will hold several configuration files.

#### Node Properties

The node properties file, `etc/node.properties`, contains configuration specific to each node. A _node_ is a single installed instance of Presto on a machine. This file is typically created by the deployment system when Presto is first installed. The following is a minimal `etc/node.properties`:

    node.environment=production
    node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
    node.data-dir=/var/presto/data

The above properties are described below:

* `node.environment`: The name of the environment. All Presto nodes in a cluster must have the same environment name.
* `node.id`: The unique identifier for this installation of Presto. This must be unique for every node. This identifier should remain consistent across reboots or upgrades of Presto. If running multiple installations of Presto on a single machine (i.e. multiple nodes on the same machine), each installation must have a unique identifier.
* `node.data-dir`: The location (filesystem path) of the data directory. Presto will store logs and other data here.

#### JVM Config

The JVM config file, `etc/jvm.config`, contains a list of command line options used for launching the Java Virtual Machine. The format of the file is a list of options, one per line. These options are not interpreted by the shell, so options containing spaces or other special characters should not be quoted (as demonstrated by the `OnOutOfMemoryError` option in the example below).

The following provides a good starting point for creating `etc/jvm.config`:

    -server
    -Xmx16G
    -XX:+UseConcMarkSweepGC
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+CMSClassUnloadingEnabled
    -XX:+AggressiveOpts
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:OnOutOfMemoryError=kill -9 %p
    -XX:PermSize=150M
    -XX:MaxPermSize=150M
    -XX:ReservedCodeCacheSize=150M
    -Xbootclasspath/p:/var/presto/installation/lib/floatingdecimal-0.1.jar

Because an `OutOfMemoryError` will typically leave the JVM in an inconsistent state, we write a heap dump (for debugging) and forcibly terminate the process when this occurs.

Presto compiles queries to bytecode at runtime and thus produces many classes, so we increase the permanent generation size (the garbage collector region where classes are stored) and enable class unloading.

When choosing the JVM heap size, be aware that heap sizes below `32G` use an optimization (compressed OOPs) to save memory when storing object references. Thus, a heap size of `31G` may have more effective capacity than a `32G` heap, so you should use a substantially larger heap size when crossing this threshold.

The last option in the above configuration loads the [floatingdecimal](https://github.com/airlift/floatingdecimal) patch for the JDK that substantially improves performance when parsing floating point numbers. This is important because many Hive file formats store floating point values as text. Change the path `/var/presto/installation` to match the Presto installation directory.

#### Config Properties

The config properties file, `etc/config.properties`, contains the configuration for the Presto server. Every Presto server can function as both a coordinator and a worker, but dedicating a single machine to only perform coordination work provides the best performance on larger clusters. Also, while the architecture allows for multiple coordinators, we have never tested that configuration.

The following is a minimal configuration for the coordinator:

    coordinator=true
    datasources=jmx
    http-server.http.port=8080
    presto-metastore.db.type=h2
    presto-metastore.db.filename=var/db/MetaStore
    task.max-memory=1GB
    discovery.uri=http://example.net:8411

And this is a minimal configuration for the workers:

    coordinator=false
    datasources=jmx,hive
    http-server.http.port=8080
    presto-metastore.db.type=h2
    presto-metastore.db.filename=var/db/MetaStore
    task.max-memory=1GB
    discovery.uri=http://example.net:8411

These properties require some explanation:

* `datasources`: Specifies the list of catalog names that may have splits processed on this node. Both the coordinator and workers have `jmx` enabled because the JMX catalog enables querying JMX properties from all nodes. However, only the workers have `hive` enabled, because we do not want to process Hive splits on the coordinator, as this can interfere with query coordination and slow down everything.
* `http-server.http.port`: Specifies the port for the HTTP server. Presto uses HTTP for all communication, internal and external.
* `presto-metastore.db.filename`: The location of the local H2 database used for storing metadata. Currently, this is mainly used by features that are still in development and thus a local database suffices. Also, this should only be needed by the coordinator, but currently it is also required for workers.
* `task.max-memory=1GB`: The maximum amount of memory used by a single task (a fragment of a query plan running on a specific node). In particular, this limits the number of groups in a `GROUP BY`, the size of the right-hand table in a `JOIN`, the number of rows in an `ORDER BY` or the number of rows processed by a window function. This value should be tuned based on the number of concurrent queries and the size and complexity of queries. Setting it too low will limit the queries that can be run, while setting it too high will cause the JVM to run out of memory.
* `discovery.uri`: The URI to the Discovery server (see below for details). Replace `example.net:8411` to match the host and port of your Discovery deployment.

#### Log Levels

The optional log levels file, `etc/log.properties`, allows setting the miminum log level for named logger hierarchies. Every logger has a name, which is typically the fully qualified name of the class that uses the logger. Loggers have a hierarchy based on the dots in the name (like Java packages). For example, consider the following log levels file:

    com.facebook.presto=DEBUG

This would set the minimum level to `DEBUG` for both `com.facebook.presto.server` and `com.facebook.presto.hive`. The default minimum level is `INFO`. There are four levels: `DEBUG`, `INFO`, `WARN` and `ERROR`.

#### Catalog Properties

Presto accesses data via _connectors_, which are mounted in catalogs. The connector provides all of the schemas and tables inside of the catalog. For example, the Hive connector maps each Hive database to a schema, so if the Hive connector is mounted as the `hive` catalog, and Hive contains a table `bar` in database `foo`, that table would be accessed in Presto as `hive.foo.bar`.

Catalogs are registered by creating a catalog properties file in the `etc/catalog` directory. For example, create `etc/catalog/jmx.properties` with the following contents to mount the `jmx` connector as the `jmx` catalog:

    connector.name=jmx

Create `etc/catalog/hive.properties` with the following contents to mount the `hive-cdh4` connector as the `hive` catalog, replacing `example.net:9083` with the correct host and port for your Hive metastore Thrift service:

    connector.name=hive-cdh4
    hive.metastore.uri=thrift://example.net:9083

You can have as many catalogs as you need, so if you have additional Hive clusters, simply add another properties file to `etc/catalog` with a different name (making sure it ends in `.properties`).

### Running Presto

The installation directory contains the launcher script in `bin/launcher`. Presto can be started as a daemon by running running the following:

    bin/launcher start

Alternatively, it can be run in the foreground, with the logs and other output being written to stdout/stderr (both streams should be captured if using a supervision system like daemontools):

    bin/launcher run

Run the launcher with `--help` to see the supported commands and command line options. In particular, the `--verbose` option is very useful for debugging the installation.

After launching, you can find the log files in `var/log`:

* `launcher.log`: This log is created by the launcher and is connected to the stdout and stderr streams of the server. It will contain a few log messages that occur while the server logging is being initialized and any errors or diagnostics produced by the JVM.
* `server.log`: This is the main log file used by Presto. It will typically contain the relevant information if the server fails during initialization. It is automatically rotated and compressed.
* `http-request.log`: This is the HTTP request log which contains every HTTP request received by the server. It is automatically rotated and compressed.

### Discovery

Presto uses the [Discovery](https://github.com/airlift/discovery) service to find all the nodes in the cluster. Every Presto instance will register itself with the Discovery service on startup.

Discovery is configured and run the same way as Presto. Download [discovery-server-1.13.tar.gz](http://search.maven.org/remotecontent?filepath=io/airlift/discovery/discovery-server/1.13/discovery-server-1.13.tar.gz) and configure it to run on a different port than Presto. The standard port for Discovery is 8411.

### Running Queries

The Presto CLI is a [self-executing](http://skife.org/java/unix/2011/06/20/really_executable_jars.html) JAR file. Copy it to suitable location:

    cp presto-cli/target/presto-cli-*-executable.jar presto

Then run it:

    ./presto --server localhost:8080 --catalog hive --schema default
