# Presto [![Build Status](https://travis-ci.com/prestodb/presto.svg?branch=master)](https://travis-ci.com/prestodb/presto)

Presto is a distributed SQL query engine for big data.

See the [User Manual](https://prestodb.github.io/docs/current/) for deployment instructions and end user documentation.

## Requirements

* Mac OS X or Linux
* Java 8 Update 151 or higher (8u151+), 64-bit. Both Oracle JDK and OpenJDK are supported.
* Maven 3.3.9+ (for building)
* Python 2.4+ (for running with the launcher script)

## Building Presto

Presto is a standard Maven project. Simply run the following command from the project root directory:

    ./mvnw clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Presto has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    ./mvnw clean install -DskipTests

## Running Presto in your IDE

### Overview

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

## Code Style

We recommend you use IntelliJ as your IDE. The code style template for the project can be found in the [codestyle](https://github.com/airlift/codestyle) repository along with our general programming and Java guidelines. In addition to those you should also adhere to the following:

* Alphabetize sections in the documentation source files (both in table of contents files and other regular documentation files). In general, alphabetize methods/variables/sections if such ordering already exists in the surrounding code.
* When appropriate, use the Java 8 stream API. However, note that the stream implementation does not perform well so avoid using it in inner loops or otherwise performance sensitive sections.
* Categorize errors when throwing exceptions. For example, PrestoException takes an error code as an argument, `PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS)`. This categorization lets you generate reports so you can monitor the frequency of various failures.
* Ensure that all files have the appropriate license header; you can generate the license by running `mvn license:format`.
* Consider using String formatting (printf style formatting using the Java `Formatter` class): `format("Session property %s is invalid: %s", name, value)` (note that `format()` should always be statically imported). Sometimes, if you only need to append something, consider using the `+` operator.
* Avoid using the ternary operator except for trivial expressions.
* Use an assertion from Airlift's `Assertions` class if there is one that covers your case rather than writing the assertion by hand. Over time we may move over to more fluent assertions like AssertJ.
* When writing a Git commit message, follow these [guidelines](https://chris.beams.io/posts/git-commit/).

## Building the Documentation

To learn how to build the docs, see the [docs README](presto-docs/README.md).

## Building the Web UI

The Presto Web UI is composed of several React components and is written in JSX and ES6. This source code is compiled and packaged into browser-compatible Javascript, which is then checked in to the Presto source code (in the `dist` folder). You must have [Node.js](https://nodejs.org/en/download/) and [Yarn](https://yarnpkg.com/en/) installed to execute these commands. To update this folder after making changes, simply run:

    yarn --cwd presto-main/src/main/resources/webapp/src install

If no Javascript dependencies have changed (i.e., no changes to `package.json`), it is faster to run:

    yarn --cwd presto-main/src/main/resources/webapp/src run package

To simplify iteration, you can also run in `watch` mode, which automatically re-compiles when changes to source files are detected:

    yarn --cwd presto-main/src/main/resources/webapp/src run watch

To iterate quickly, simply re-build the project in IntelliJ after packaging is complete. Project resources will be hot-reloaded and changes are reflected on browser refresh.

## Release Notes

When authoring a pull request, the PR description should include its relevant release notes.
Follow [Release Notes Guidelines](https://github.com/prestodb/presto/wiki/Release-Notes-Guidelines) when authoring release notes. 
