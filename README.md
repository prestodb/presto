# Presto

Presto is a distributed SQL query engine for big data.

See the [Presto installation documentation](https://prestodb.io/docs/current/installation.html) for deployment instructions.

See the [Presto documentation](https://prestodb.io/docs/current/) for general documentation.


## Mission and Architecture

See [PrestoDB: Mission and Architecture](ARCHITECTURE.md). 

## Requirements

* Mac OS X or Linux
* Java 8 Update 151 or higher (8u151+), 64-bit. Both Oracle JDK and OpenJDK are supported.
* Maven 3.6.3+ (for building)
* Python 2.4+ (for running with the launcher script)

<details> <!-- from: https://github.com/prestodb/presto/blob/master/README.md -->
  <summary><a id="building-presto"><h2>Building Presto</h2></a></summary>

### Overview (Java)

Presto is a standard Maven project. Simply run the following command from the project root directory:

    ./mvnw clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Presto has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    ./mvnw clean install -DskipTests

After building Presto for the first time, you can load the project into your IDE and run the server. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Presto is a standard Maven project, you can import it into your IDE using the root `pom.xml` file. In IntelliJ, choose Open Project from the Quick Start box or choose Open from the File menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a 1.8 JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8.0 as Presto makes use of several Java 8 language features

Presto comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: `com.facebook.presto.server.PrestoServer`
* VM Options: `-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties`
* Working directory: `$MODULE_WORKING_DIR$` or `$MODULE_DIR$`(Depends your version of IntelliJ)
* Use classpath of module: `presto-main`

The working directory should be the `presto-main` subdirectory. In IntelliJ, using `$MODULE_DIR$` accomplishes this automatically.

Additionally, the Hive plugin must be configured with location of your Hive metastore Thrift service. Add the following to the list of VM options, replacing `localhost:9083` with the correct host and port (or use the below value if you do not have a Hive metastore):

    -Dhive.metastore.uri=thrift://localhost:9083

### Using SOCKS for Hive or HDFS

If your Hive metastore or HDFS cluster is not directly accessible to your local machine, you can use SSH port forwarding to access it. Setup a dynamic SOCKS proxy with SSH listening on local port 1080:

    ssh -v -N -D 1080 server

Then add the following to the list of VM options:

    -Dhive.metastore.thrift.client.socks-proxy=localhost:1080

### Running the CLI

Start the CLI to connect to the server and run SQL queries:

    presto-cli/target/presto-cli-*-executable.jar

Run a query to see the nodes in the cluster:

    SELECT * FROM system.runtime.nodes;

In the sample configuration, the Hive connector is mounted in the `hive` catalog, so you can run the following queries to show the tables in the Hive database `default`:

    SHOW TABLES FROM hive.default;

### Building the Documentation

To build the Presto docs, see the [docs README](presto-docs/README.md).

### Building the Presto Console

The Presto Console is composed of several React components and is written in JSX and ES6. This
source code is stored in the `presto-ui/` module. The compilation process generates
browser-compatible javascript which is added as JAR resources during the maven build. When the
resource JAR is included on the classpath of Presto coordinator, it will be able to serve the
resources.

None of the Java code relies on the Presto UI project being compiled, so it is possible to exclude
this UI when building Presto. Add the property `-DskipUI` to the maven command to disable building
the `ui` maven module.

    ./mvnw clean install -DskipUI

You must have [Node.js](https://nodejs.org/en/download/) and [Yarn](https://yarnpkg.com/en/) installed to build the UI. When using  Maven to build
the project, Node and yarn are installed in the `presto-ui/target` folder. Add the node and yarn
executables to the `PATH` environment variable.

To update Presto Console after making changes, run:

    yarn --cwd presto-ui/src install

To simplify iteration, you can also run in `watch` mode, which automatically re-compiles when
changes to source files are detected:

    yarn --cwd presto-ui/src run watch

To iterate quickly, simply re-build the project in IntelliJ after packaging is complete. Project
resources will be hot-reloaded and changes are reflected on browser refresh.

## Presto native and Velox

[Presto native](https://github.com/prestodb/presto/tree/master/presto-native-execution) is a C++ rewrite of Presto worker. [Presto native](https://github.com/prestodb/presto/tree/master/presto-native-execution) uses [Velox](https://github.com/facebookincubator/velox) as its primary engine to run presto workloads.

[Velox](https://github.com/facebookincubator/velox) is a C++ database library which provides reusable, extensible, and high-performance data processing components.

Check out [building instructions](https://github.com/prestodb/presto/tree/master/presto-native-execution#build-from-source) to get started.


<hr>
</details>


## Contributing!

Please refer to the [contribution guidelines](https://github.com/prestodb/presto/blob/master/CONTRIBUTING.md) to get started.

## Questions?

[Please join our Slack channel and ask in `#dev`](https://communityinviter.com/apps/prestodb/prestodb).

## License

By contributing to Presto, you agree that your contributions will be licensed under the [Apache License Version 2.0 (APLv2)](LICENSE).

