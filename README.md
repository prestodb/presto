# Presto

Presto is a distributed SQL query engine for big data.

See the [User Manual](http://facebook.github.io/presto/docs/current/) for end user documentation.

## Requirements

* Mac OS X or Linux
* Java 7 64-bit
* Maven 3 (for building)
* Python 2.4+ (for running with the launcher script)
* Discovery (see below)

## Building Presto

Presto is a standard Maven project. Simply run the following command from the project root directory:

    mvn clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Presto has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    mvn clean install -DskipTests

## Discovery

### Overview

Presto uses the [Discovery](https://github.com/airlift/discovery) service to find all the nodes in the cluster and for finding external resources such as the Hive metastore.

Discovery is a simple service that manages service *announcements*. An announcement is a set of properties that describe an instance of a given service type. For example, an announcement for a service that uses HTTP will contain the HTTP URI for that service.

There are two types of announcements: *dynamic* and *static*. Dynamic announcements are used by services that natively support discovery. When Presto starts, it registers itself in Discovery using a dynamic announcement, and periodically refreshes the announcement to keep it alive. Static announcements are for external services that cannot announce themselves, such as the Hive metastore. Discovery stores static announcements permanently in a local database.

### Running Discovery

Discovery is configured and run the same way as Presto. These short instructions are suitable for running it locally for development or testing.

First, download [discovery-server-1.12.tar.gz](http://search.maven.org/remotecontent?filepath=io/airlift/discovery/discovery-server/1.12/discovery-server-1.12.tar.gz) and unpack it:

    tar xzf discovery-server-1.12.tar.gz

Next, configure it:

    cd discovery-server-1.12
    mkdir etc
    echo node.environment=test > etc/config.properties
    echo http-server.http.port=8411 >> etc/config.properties
    touch etc/jvm.config

Finally, start it:

    bin/launcher start

Wait a few seconds, then verify that it is working:

    curl http://localhost:8411/v1/service?pretty

### Announce the Hive Metastore

Presto finds the Hive metastore using Discovery. This allows updating the metastore location without having to restart the cluster (which would be necessary if it was a configuration parameter).

Create a static announcement for the metastore by making an HTTP POST request (changing ``localhost:9083`` to point to your metastore's thrift service):

    curl -H 'Content-Type: application/json' --data-binary '
    {
      "environment": "test",
      "type": "hive-metastore",
      "pool": "general",
      "properties": {
        "thrift": "localhost:9083"
      }
    }' http://localhost:8411/v1/announcement/static

Verify that you see the announcement:

    curl http://localhost:8411/v1/service/hive-metastore?pretty

## Running Presto in your IDE

### Overview

After building Presto for the first time, you can load the project into your IDE and run the server. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Presto is a standard Maven project, you can import it into your IDE using the root ``pom.xml`` file.

Presto comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: ``com.facebook.presto.server.PrestoServer``
* VM Options: ``-ea -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties``
* Working directory: ``$MODULE_DIR$``
* Use classpath of module: ``presto-server``

The working directory should be the ``presto-server`` subdirectory. In IntelliJ, using ``$MODULE_DIR$`` accomplishes this automatically.

### Using SOCKS for Hive or HDFS

If your Hive metastore or HDFS cluster is not directly accessible to your local machine, you can use SSH port forwarding to access it. Setup a dynamic SOCKS proxy with SSH listening on local port 1080:

    ssh -v -N -D 1080 server

Then add the following to the list of VM options:

    -Dhive.metastore.thrift.client.socks-proxy=localhost:1080

### Running CLI

Start the CLI to connect to the server and run SQL queries:

    presto-cli/target/presto-cli-*-standalone.jar

Run a query to see the nodes in the cluster:

    SELECT * FROM sys.node;

In the sample configuration, the Hive connector is mounted in the ``hive`` catalog, so you can run the following queries to show the tables in the Hive database ``default``:

    SHOW TABLES FROM hive.default;
