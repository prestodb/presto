# Presto product tests

The product tests make use of user visible interfaces (e.g. `presto-cli`)
to test Presto for correctness. The product tests complement the unit tests
because unlike the unit tests, they exercise the Presto codebase end-to-end.

To keep the execution of the product tests as lightweight as possible we
decided to use [Docker](http://www.docker.com/). The general execution
setup is as follows: a single Docker container runs Hadoop in pseudo-distributed
mode and Presto runs either in Docker container(s) (both pseudo-distributed
and distributed setups are possible) or manually from IntelliJ (for
debugging Presto). The tests run in a separate JVM and they can be started
using the scripts found in `presto-product-tests/bin`. The product
tests are run using the [Tempto](https://github.com/prestodb/tempto) harness. 

Developers should consider writing product tests in addition to any unit tests
when making changes to user visible features. The product tests should also
be run after all code changes to ensure no existing functionality has been
broken.

## Requirements

*Running the Presto product tests requires at least 4GB of free memory*

### GNU/Linux
* [`docker >= 1.10`](https://docs.docker.com/installation/#installation)

    ```
    wget -qO- https://get.docker.com/ | sh
    ```

* [`docker-compose >= 1.8.1`](https://docs.docker.com/compose/install/)

    ```
    pip install docker-compose
    ```

### OS X using Docker for Mac (macOS 10.10.3 Yosemite or newer) [PREFERRED WAY]

* Install Docker for Mac: https://docs.docker.com/docker-for-mac/

* Add entries in `/etc/hosts` for all services running in docker containers:
`hadoop-master`, `mysql`, `postgres`, `cassandra`, `presto-master`.
They should point to your external IP address (shown by `ifconfig` on your Mac (not inside docker)).

### OS X using Docker Toolbox (macOS 10.8 "Mountain Lion" or newer) [NOT RECOMMENDED]

* [`VirtualBox >= 5.0`](https://www.virtualbox.org/wiki/Downloads)

The Docker daemon cannot run natively on OS X because it uses Linux-specific
kernel features. Instead, the Docker daemon runs in a Linux VM created by
the `docker-machine` binary. Docker containers run in the Linux VM and are
controlled by the `docker` client binary that runs natively on OS X.
Both `docker-machine` and `docker` are included in the `docker-toolbox`
package, which must be installed.

* [`docker-toolbox >= 1.10`](https://www.docker.com/products/docker-toolbox)

In addition to `docker-machine` and `docker`, the `docker-toolbox`
package also install `docker-compose`, which is a multi-container
orchestration Python utility. To gain access to these utilities, start the
pre-configured shell environment by double-clicking on the "Docker Quickstart
Terminal" icon located in ~/Applications/Docker. Note that all commands listed
in subsequent parts of this tutorial must be run within such a pre-configured
shell.

#### Setting up a Linux VM for Docker Toolbox

The `docker-toolbox` installation creates a VirtualBox VM called `default`.
To run product-tests on the `default` VM, it must be re-configured to use
4GB of memory with the following commands:

```
docker-machine stop
vboxmanage modifyvm default --memory 4096
docker-machine start
```

Alternatively, if you do not want to use the `default` VM to run the
product tests, you can create a new VM with the commands below. Note that
the `default` VM will always be running when you start a new pre-configured
shell environment. Permanently removing or replacing the `default` VM
is beyond the scope of this tutorial.

* Create a VM called <machine>. This should be done only once and not
every time a pre-configured shell is started:

    ```
    docker-machine create -d virtualbox --virtualbox-memory 4096 <machine>
    ```

* After the new VM is created, the pre-configured shell environment must be
told to use the `<machine>` VM instead of the `default` VM to run Docker
containers. These commands must be run every time a new pre-configured
shell is started:

    ```
    docker-machine start <machine>
    eval $(docker-machine env <machine>)
    ```
    
Note that for every new VM, the docker images on the previous
VM will have to be re-downloaded when the product tests are kicked
off. To avoid this unnecessary re-download, do not create new
VMs often.

## Use the `docker-compose` wrappers

We're using [multiple compose files](https://docs.docker.com/compose/extends/#multiple-compose-files)
because of the number of overrides needed for different environments,
and deficiencies of `extends:` syntax (see the note
[here](https://docs.docker.com/compose/extends/#extending-services)).


To ease the pain of passing multiple `-f` arguments to `docker-compose`,
each environment has a `compose.sh` wrapper script. Thanks to it, instead of e.g.

`docker-compose -f ./docker-compose.yml -f ../common/standard.yml -f ../common/jdbc_db.yml [compose commands]`

one can simply write

`compose.sh [compose commands]`

## Running the product tests

The Presto product tests must be run explicitly because they do not run
as part of the Maven build like the unit tests do. Note that the product
tests cannot be run in parallel. This means that only one instance of a
test can be run at once in a given environment. To run all product
tests and exclude the `quarantine`, `big_query` and `profile_specific_tests`
groups run the following command:

```
./mvnw install -DskipTests
presto-product-tests/bin/run_on_docker.sh <profile> -x quarantine,big_query,profile_specific_tests
```

where [profile](#profile) is one of either:
- **multinode** - pseudo-distributed Hadoop installation running on a
 single Docker container and a distributed Presto installation running on
 multiple Docker containers. For multinode the default configuration is
 1 coordinator and 1 worker.
- **[singlenode](#singlenode)** - pseudo-distributed Hadoop installation running on a
 single Docker container and a single node installation of Presto also running
 on a single Docker container.
- **singlenode-hdfs-impersonation** - HDFS impersonation enabled on top of the
 environment in [singlenode](#singlenode) profile. Presto impersonates the user
 who is running the query when accessing HDFS.
- **singlenode-kerberos-hdfs-impersonation** - pseudo-distributed kerberized
 Hadoop installation running on a single Docker container and a single node
 installation of kerberized Presto also running on a single Docker container.
 This profile has Kerberos impersonation. Presto impersonates the user who
 is running the query when accessing HDFS.
- **singlenode-kerberos-hdfs-no-impersonation** - pseudo-distributed Hadoop
 installation running on a single Docker container and a single node
 installation of kerberized Presto also running on a single Docker container.
 This profile runs Kerberos without impersonation.

Please keep in mind that if you run tests on Hive of version not greater than 1.0.1, you should exclude test from `post_hive_1_0_1` group by passing the following flag to tempto: `-x post_hive_1_0_1`.
First version of Hive capable of running tests from `post_hive_1_0_1` group is Hive 1.1.0.

For more information on the various ways in which Presto can be configured to
interact with Kerberized Hive and Hadoop, please refer to the [Hive connector documentation](https://prestodb.io/docs/current/connector/hive.html).

### Running a single test

The `run_on_docker.sh` script can also run individual product tests. Presto
product tests are either [Java based](https://github.com/prestodb/tempto#java-based-tests)
or [convention based](https://github.com/prestodb/tempto#convention-based-sql-query-tests)
and each type can be run individually with the following commands:

```
# Run single Java based test
presto-product-tests/bin/run_on_docker.sh <profile> -t com.facebook.presto.tests.functions.operators.Comparison.testLessThanOrEqualOperatorExists
# Run single convention based test
presto-product-tests/bin/run_on_docker.sh <profile> -t sql_tests.testcases.system.selectInformationSchemaTables
```

### Running groups of tests

Tests belong to a single or possibly multiple groups. Java based tests are
tagged with groups in the `@Test` annotation and convention based tests have
group information in the first line of their test file. Instead of running
single tests, or all tests, users can also run one or more test groups.
This enables users to test features in a cross functional way. To run a
particular group, use the `-g` argument as shown:

```
# Run all tests in the string_functions and create_table groups
presto-product-tests/bin/run_on_docker.sh <profile> -g string_functions,create_tables
```

Some groups of tests can only be run with certain profiles. Incorrect use of profile
for such test groups will result in test failures. We call these tests that
require a specific profile to run as *profile specific tests*. In addition to their
respective group, all such tests also belong to a parent group called
`profile_specific_tests`. To exclude such tests from a run, make sure to add the
`profile_specific_tests` group to the list of excluded groups.

Following table describes the profile specific test categories, the corresponding
test group names and the profile(s) which must be used to run tests in those test
groups.

| Tests                 | Test Group                | Profiles                                                                         |
| ----------------------|---------------------------| -------------------------------------------------------------------------------- |
| Authorization         | ``authorization``         | ``singlenode-kerberos-hdfs-impersonation``                                       |
| HDFS impersonation    | ``hdfs_impersonation``    | ``singlenode-hdfs-impersonation``, ``singlenode-kerberos-hdfs-impersonation``    |
| No HDFS impersonation | ``hdfs_no_impersonation`` | ``singlenode``, ``singlenode-kerberos-hdfs-no_impersonation``                    |

Below is a list of commands that explain how to run these profile specific tests
and also the entire test suite:

* Run **Authorization** tests:

    ```
    presto-product-tests/bin/run_on_docker.sh singlenode-kerberos-hdfs-impersonation -g authorization
    ```
* Run **HDFS impersonation** tests, where &lt;profile> is one of either
``singlenode-hdfs-impersonation`` or ``singlenode-kerberos-hdfs-impersonation``:

    ```
    presto-product-tests/bin/run_on_docker.sh <profile> -g hdfs_impersonation
    ```
* Run **no HDFS impersonation** tests, where &lt;profile> is one of either
``singlenode`` or ``singlenode-kerberos-hdfs-no-impersonation``:

    ```
    presto-product-tests/bin/run_on_docker.sh <profile> -g hdfs_no_impersonation
    ```
* Run the **entire test suite** excluding all profile specific tests, where &lt;profile> can
be any one of the available profiles:

    ```
    presto-product-tests/bin/run_on_docker.sh <profile> -x quarantine,big_query,profile_specific_tests
    ```

### Running from IntelliJ

For running Java based tests from IntelliJ see the section on
[Debugging Java based tests](#debugging-java-based-tests).

### Running with custom / downloaded artifacts

To run with custom versions of presto / presto-cli / product tests, just set the appropriate
environment variables:

```
export PRESTO_SERVER_DIR=/tmp/presto-server-dir      #unpacked presto-server.tar.gz
export PRESTO_CLI_JAR=/tmp/artifacts/presto-cli-executable.jar
export PRODUCT_TESTS_JAR=/tmp/artifacts/presto-product-tests-executable.jar
presto-product-tests/bin/run_on_docker.sh multinode -x quarantine,big_query,profile_specific_tests
```

All of the variables are optional and fall back to local sources / build artifacts if unspecified.

### Interrupting a test run

To interrupt a product test run, send a single `Ctrl-C` signal. The scripts
running the tests will gracefully shutdown all containers. Any follow up
`Ctrl-C` signals will interrupt the shutdown procedure and possibly leave
containers in an inconsistent state.

## Debugging the product tests

### Debugging Java based tests

[Java based tests](https://github.com/prestodb/tempto#java-based-tests)
can be run and debugged from IntelliJ like regular TestNG tests with the
setup outlined below:

1. Ensure all project artifacts are up to date:

    ```
    ./mvnw install -DskipTests
    ```

2. Start Presto dependant services as Docker containers:

    ```
    presto-product-tests/conf/docker/singlenode/compose.sh up -d hadoop-master
    presto-product-tests/conf/docker/singlenode/compose.sh up -d mysql
    presto-product-tests/conf/docker/singlenode/compose.sh up -d postgres
    presto-product-tests/conf/docker/singlenode/compose.sh up -d cassandra
    ```
    
    Tip: To display container logs run:

    ```
    presto-product-tests/conf/docker/singlenode/compose.sh logs
    ```
    
3. Add an IP-to-host mapping for the `hadoop-master`, `mysql`, `postgres` and `cassandra` hosts in `/etc/hosts`.
The format of `/etc/hosts` entries is `<ip> <host>`:

    - On GNU/Linux add the following mapping: `<container ip> hadoop-master`.
    The container IP can be obtained by running:

        ```
        docker inspect $(presto-product-tests/conf/docker/singlenode/compose.sh ps -q hadoop-master) | grep -i IPAddress
        ```

    Similarly add mappings for MySQL, Postgres and Cassandra containers (`mysql`, `postgres` and `cassandra` hostnames respectively). To check IPs for those containers run:

        ```
        docker inspect $(presto-product-tests/conf/docker/singlenode/compose.sh ps -q mysql) | grep -i IPAddress
        docker inspect $(presto-product-tests/conf/docker/singlenode/compose.sh ps -q postgres) | grep -i IPAddress
        docker inspect $(presto-product-tests/conf/docker/singlenode/compose.sh ps -q cassandra) | grep -i IPAddress

    Alternatively you can use below script to obtain hosts ip mapping

        ```
        presto-product-tests/bin/hosts.sh singlenode
        ```

    Note that above command requires [jq](https://stedolan.github.io/jq/) to be installed in your system

    - On OS X:
        - Docker for Mac:
        Add the following mapping to `/etc/hosts`: `<IP-of-your-Mac> hadoop-master mysql postgres cassandra`.

        - Docker Toolbox:
        Add the following mapping to `/etc/hosts`: `<docker machine ip> hadoop-master mysql postgres cassandra`.
        Since Docker containers run inside a Linux VM, on OS X we map the VM IP to
        the `hadoop-master`, `mysql`, `postgres` and `cassandra` hostnames. To obtain the IP of the Linux VM run:

            ```
            docker-machine ip <machine>
            ```
    
4. [Create a run configuration in IntelliJ](https://www.jetbrains.com/help/idea/2016.1/creating-and-editing-run-debug-configurations.html)
with the following parameters:
    
    - Use classpath of module: `presto-main`
    - Main class: `com.facebook.presto.server.PrestoServer`
    - Working directory: `presto-product-tests/conf/presto`
    - VM options: `-ea -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -DHADOOP_USER_NAME=hive -Duser.timezone=UTC`

5. MAKE SURE PRESTO CONFIGURATION IS ALIGNED WITH THE ONE IN `presto-product-tests/conf/presto`!

    If you use custom configuration, make sure to configure the Hive catalog such that it uses a socks proxy running inside the container.
    To do so, please make sure your Hive properties file under `etc/catalog/` has the below entry:
    ```
    hive.metastore.thrift.client.socks-proxy=hadoop-master:1180
    ```
    Also, `hadoop-master` must point to a correct IP (as described in step 3 of this section).

6. Start the Presto server with the newly created run configuration.

7. In IntelliJ, right click on a test method name or test class to run
or debug the respective test(s).

8. Remember to stop the Hadoop container once debugging is done with the
following command:

    ```
    presto-product-tests/conf/docker/singlenode/compose.sh down
    ```

### Debugging convention based tests

Some of the product tests are implemented in a
[convention based](https://github.com/prestodb/tempto#convention-based-sql-query-tests)
manner. Such tests can not be run directly from IntelliJ and the following
steps explain how to debug convention based tests:

1. Follow steps [1-6] from the [Debugging Java based tests](#debugging-java-based-tests)
section.

2. Run a convention based test with the following JVM debug flags:
    
    ```
    PRODUCT_TESTS_JVM_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=<debug port>" presto-product-tests/bin/run.sh -t sql_tests.testcases.system.selectInformationSchemaTables
    ```
    
    where `<debug port>` is the port over which the test JVM and the
    debugger will communicate. By default IntelliJ uses `5005`. Also
    note that execution of the product test will be suspended until a
    debugger is attached.

3. Set a breakpoint at the beginning of the `com.teradata.tempto.internal.convention.ConventionBasedTestFactory#createTestCases`
method. This is the main entry point for the convention based tests. When
opening the `ConventionBasedTestFactory` class for the first time, IntelliJ
will display a de-compiled version because `ConventionBasedTestFactory` is
part of Tempto and not Presto. To download the actual sources make sure
to the press the 'Download sources' button that should appear in the upper
right hand corner of the editor window.

4. [Create a remote run configuration in IntelliJ](https://www.jetbrains.com/help/idea/2016.1/run-debug-configuration-remote.html?origin=old_help).
If the debug port in the step above was `5005`, then no changes to the
remote run configuration are required. This configuration will act as
the debugger.

5. Run the remote configuration. If the debugger successfully attaches,
execution of the product test will resume until the breakpoint is hit. At
this stage the following components should be running: a container with
Hadoop, an IntelliJ run configuration JVM running Presto, a JVM
running the product tests and an IntelliJ remote run configuration JVM
running the debugger.

## Troubleshooting

Use the `docker-compose` (probably using a [wrapper](#use-the-docker-compose-wrappers))
and `docker` utilities to control and troubleshoot containers.
In the following examples ``<profile>`` is [profile](#profile).

1. Use the following command to view output from running containers:

    ```
    presto-product-tests/conf/docker/<profile>/compose.sh logs
    ```

2. To connect to a running container in an interactive Bash shell to
examine state and debug as necessary, use the following commands:

    ```
    # Find the ID of the desired container
    docker ps
    # Connect to running container
    docker exec -ti <container-id> /bin/bash
    ```

3. To reset the Hadoop container when debugging tests, first teardown the
container and then pull from Docker Hub to ensure the latest version has
been downloaded:

    ```
    # Stop Hadoop container (the down command stops and removes containers,
    # network, images, and volumes). This effectively resets the container.
    presto-product-tests/conf/docker/<profile>/compose.sh down
    # Pull from Docker Hub to ensure the latest version of the image is
    # downloaded.
    presto-product-tests/conf/docker/<profile>/compose.sh pull
    ```

## Known issues

### Port 1180 already allocated

If you see the error
```
ERROR: for hadoop-master  Cannot start service hadoop-master: driver failed programming external connectivity on endpoint common_hadoop-master_1 Error starting userland proxy: Bind for 0.0.0.0:1180 failed: port is already allocated
```
You most likely have some application listening on port 1180 on either docker-machine or on your local machine if you are running docker natively.
You can override the default socks proxy port (1180) used by dockerized Hive deployment in product tests using the
`HIVE_PROXY_PORT` environment variable, e.g. `export HIVE_PROXY_PORT=1180`. This will run all of the dockerized tests using the custom port for the socks proxy.
When you change the default socks proxy port (1180) and want to use Hive provided by product tests from outside docker (e.g. access it from Presto running in your IDE), you have to modify the property `hive.metastore.thrift.client.socks-proxy=hadoop-master:1180` in your `hive.properties` file accordingly.
Presto inside docker (used while starting tests using `run_on_docker.sh`) will still use default port (1180) though.

### Malformed reply from SOCKS server

If you see an error similar to
```
Failed on local exception: java.net.SocketException: Malformed reply from SOCKS server; Host Details : local host is [...]
```
Make sure your `/etc/hosts` points to proper IP address (see [Debugging Java based tests](#debugging-java-based-tests), step 3).
Also it's worth confirming that your Hive properties file accounts for the socks proxy used in Hive container (steps 4-5 of [Debugging Java based tests](#debugging-java-based-tests)).

If `/etc/hosts` entries have changed since the time when Docker containers were provisioned it's worth removing them and re-provisioning.
To do so, use `docker rm` on each container used in product tests.
