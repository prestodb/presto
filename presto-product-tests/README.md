# Presto product tests

## Installing dependencies

*Running product tests requires at least 4GB free memory.*

#### On GNU/Linux
* [```docker >= 1.10```](https://docs.docker.com/installation/#installation)

    ```
    wget -qO- https://get.docker.com/ | sh
    ```

* [```docker-compose >= 1.60```](https://docs.docker.com/compose/install/)

    ```
    pip install docker-compose
    ```

#### On Mac OS X

* [```docker-toolbox >= 1.10```](https://www.docker.com/products/docker-toolbox)

On Mac OS X installing docker-toolbox gives access to a preconfigured shell environment
with ```docker``` and ```docker-compose``` available. The shortcut to this preconfigured
shell environment for docker may be found in Applications/Docker as "Docker Quickstart Terminal."
Note that all commands given in later parts of these instructions should be run from this
environment.

##### Setting up virtual machine for docker

As part of the ```docker-toolbox``` installation process a VirtualBox machine with the
name ```default``` is created. In order to use this machine to run product-tests it must be
 configured with at least 4GB memory. This configuration can be done using following commands:

```
docker-machine stop
vboxmanage modifyvm default --memory 4096
docker-machine start
```

If one does not want to use the ```default``` machine to run product tests, the following
 steps may be used to create and use a new machine named ```<machine>```:

* To create a virtual machine with name <machine> (required one time):

    ```
    docker-machine create -d virtualbox --virtualbox-memory 4096 <machine>
    ```

* To set up an environment to use ```<machine>``` (required at docker bash startup):

    ```
    eval $(docker-machine env <machine>)
    ```


## Running tests

Product tests are not run as part of the maven build process. To run them use the following command:

```
presto-product-tests/bin/run_on_docker.sh <profile> -x quarantine,big_query
```

Possible profiles are:
- **multinode** - consists of single node (pseudo-distributed) Hadoop cluster and multiple node Presto cluster
    with one coordinator node and at least one worker node.
- **singlenode** - consists of single node (pseudo-distributed) Hadoop cluster and one node Presto cluster

## Debugging tests

The following steps explain how you can run the product tests from your IDE as regular TestNG tests:

1. Navigate to the presto root folder

2. Start the Hadoop container

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml up -d hadoop-master
    ```
    
    Tip: In order to display container logs run

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml logs
    ```
    
3. Set ```hadoop-master``` host in ```/etc/hosts```

    - On GNU/Linux: ```<container ip> hadoop-master```. Container IP can be obtained with:

        ```
        docker inspect $(docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml ps -q hadoop-master) | grep -i IPAddress
        ```

    - On Mac OS X: ```<docker machine ip> hadoop-master```. Docker machine IP can be obtained with:

        ```
        docker-machine ip <machine|default>
        ```
    
4. Create the run configuration and then start the Presto server
    
    - Use classpath of module: ```presto-main```
    - Main class: ```com.facebook.presto.server.PrestoServer```
    - Working directory: ```presto-product-tests/conf/presto```
    - VM options:

        ```
        -ea
        -Xmx2G
        -Dconfig=etc/config.properties
        -Dlog.levels-file=etc/log.properties
        -DHADOOP_USER_NAME=hive
        -Duser.timezone=UTC
        ```

5. Run product tests

    There are multiple methods of running tests listed below. Note that product tests are
    not parallelizable.  This means that only one instance of only one method may be run
    in the same environment at the same time:

    - From your IDE
    - Full suite from CLI:

        ```
        presto-product-tests/bin/run.sh -x quarantine,big_query
        ```

    - Single TestNG based test from CLI:

        ```
        presto-product-tests/bin/run.sh -t com.facebook.presto.tests.functions.operators.Comparison.testLessThanOrEqualOperatorExists
        ```

    - Single convention based test from CLI:

        ```
        presto-product-tests/bin/run.sh -t sql_tests.testcases.system.selectInformationSchemaTables
        ```
    
6. Stop Hadoop container once debugging is done:

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml down
    ```

### Debugging convention based tests

Some of the product tests are implemented in 
[convention based](https://github.com/prestodb/tempto#convention-based-sql-query-tests) manner.
Those tests can not be run from the IDE directly.
 
The following steps explain how you can debug convention based tests:
 
1. Follow the [1-4] steps from the previous section.
2. Run convention based test with the debugger port exposed:
    
    ```
    PRODUCT_TESTS_JVM_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=<debug port>" presto-product-tests/bin/run.sh -t sql_tests.testcases.system.selectInformationSchemaTables
    ```
    
    Product tests execution will be suspended until debugger is attached.

3. Set a breakpoint at the beginning of the ```com.teradata.tempto.internal.convention.ConventionBasedTestFactory#createTestCases```
    method. This is the main entry point for the convention based tests.
4. Attach debugger to the ```localhost:<debug port>```. On this step the product tests execution will be resumed.
