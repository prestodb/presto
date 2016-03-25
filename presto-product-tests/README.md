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

On Mac OS X installing docker-toolbox gives access to a preconfigured bash environment
with ```docker``` and ```docker-compose``` available. To start this bash environment
select "Docker Quickstart Terminal" from Launchpad. Note that all commands given in
later parts of these instructions should be run from this environment.

##### Setting up virtual machine for docker

As part of the ```docker-toolbox``` installation process VirtualBox machine with the
name ```default``` is created. It's settings should be adequate for most cases,
but setting up docker machine to have at least 4GB is required. Memory size can be
adjusted using the VirtualBox GUI, or new docker machine with the desired memory size
can be created.

* To create virtual machine with name <machine> (required one time):

    ```
    docker-machine create -d virtualbox --virtualbox-memory 4096 <machine>
    ```

* To set up enviroment to use `<machine>` instead of ```default``` (required at docker bash startup):

    ```
    eval $(docker-machine env <machine>)
    ```


## Running tests

Product tests are not run as part of the maven build process. To start them use run following command:

```
presto-product-tests/bin/run_on_docker.sh <profile> -x quarantine,big_query
```

Possible profiles are:
- **distributed** - consists of single node (pseudo-distributed) Hadoop cluster and multiple node Presto cluster
    with one coordinator node and at least one worker node.
- **singlenode** - consists of single node (pseudo-distributed) Hadoop cluster and one node Presto cluster

*Docker container logs can be found in ```/tmp/presto_docker_logs```*

## Debugging tests

The following steps explain how you can run the product tests from your IDE as regular TestNG tests:

1. Start the Hadoop container

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml up -d hadoop-master
    ```
    
    Tip: In order to display container logs run

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml logs
    ```
    
2. Set ```hadoop-master``` host in ```/etc/hosts```

    - On GNU/Linux: ```<container ip> hadoop-master```. Container IP can be obtained with:

        ```
        docker inspect $(docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml ps -q hadoop-master) | grep -i IPAddress
        ```

    - On Mac OS X: ```<docker machine ip> hadoop-master```. Docker machine IP can be obtained with:

        ```
        docker-machine ip <machine|default>
        ```
    
3. Create the run configuration and then start the Presto server
    
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

4. Run product tests

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
    
5. Stop hadoop container once debugging is done: 

    ```
    docker-compose -f presto-product-tests/conf/docker/singlenode/docker-compose.yml down
    ```
