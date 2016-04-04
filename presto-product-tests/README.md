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

* [`docker-compose >= 1.60`](https://docs.docker.com/compose/install/)

    ```
    pip install docker-compose
    ```

### OS X (10.8 "Mountain Lion" or newer)

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

#### Setting up a Linux VM for Docker

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

## Running the product tests

The Presto product tests must be run explicitly because they do not run
as part of the Maven build like the unit tests do. Note that the product
tests cannot be run in parallel. This means that only one instance of a
test can be run at once in a given environment. To run all product
tests and exclude the `quarantine` and `big_query` groups run the
following command:

```
./mvnw install -DskipTests
presto-product-tests/bin/run_on_docker.sh <profile> -x quarantine,big_query
```

where `<profile>` is one of either:
- **multinode** - pseudo-distributed Hadoop installation running on a
 single Docker container and a distributed Presto installation running on
 multiple Docker containers.
- **singlenode** - pseudo-distributed Hadoop installation running on a
 single Docker container and a single node installation of Presto also running
 on a single Docker container.

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

For running Java based tests from IntelliJ see the section on
[Debugging Java based tests](#debugging-java-based-tests).

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

2. Start Hadoop in pseudo-distributed mode in a Docker container:

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml up -d hadoop-master
    ```
    
    Tip: To display container logs run:

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml logs
    ```
    
3. Add an IP-to-host mapping for the `hadoop-master` host in `/etc/hosts`.
The format of `/etc/hosts` entries is `<ip> <host>`:

    - On GNU/Linux add the following mapping: `<container ip> hadoop-master`.
    The container IP can be obtained by running:

        ```
        docker inspect $(docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml ps -q hadoop-master) | grep -i IPAddress
        ```

    - On OS X add the following mapping: `<docker machine ip> hadoop-master`.
    Since Docker containers run inside a Linux VM, on OS X we map the VM IP to
    the `hadoop-master` hostname. To obtain the IP of the Linux VM run:

        ```
        docker-machine ip <machine>
        ```
    
4. [Create a run configuration in IntelliJ](https://www.jetbrains.com/help/idea/2016.1/creating-and-editing-run-debug-configurations.html)
with the following parameters:
    
    - Use classpath of module: `presto-main`
    - Main class: `com.facebook.presto.server.PrestoServer`
    - Working directory: `presto-product-tests/conf/presto`
    - VM options: `-ea -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -DHADOOP_USER_NAME=hive -Duser.timezone=UTC`

5. Start the Presto server with the newly created run configuration.

6. In IntelliJ, right click on a test method name or test class to run
or debug the respective test(s).

7. Remember to stop the Hadoop container once debugging is done with the
following command:

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml down
    ```

### Debugging convention based tests

Some of the product tests are implemented in a
[convention based](https://github.com/prestodb/tempto#convention-based-sql-query-tests)
manner. Such tests can not be run directly from IntelliJ and the following
steps explain how to debug convention based tests:

1. Follow steps [1-5] from the [Debugging Java based tests](#debugging-java-based-tests)
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

Use the `docker-compose` and `docker` utilities to control and troubleshoot
containers. In the following examples `<profile>` is either `singlenode` or
`multinode`.

1. Use the following command to view output from running containers:

    ```
    docker-compose -f presto-product-tests/conf/docker/<profile>/docker-compose.yml logs
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
    docker-compose -f presto-product-tests/conf/docker/<profile>/docker-compose.yml down
    # Pull from Docker Hub to ensure the latest version of the image is
    # downloaded.
    docker-compose -f presto-product-tests/conf/docker/<profile>/docker-compose.yml pull
    ```
