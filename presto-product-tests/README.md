# Presto product tests

## Test configuration

Test assume that you have hadoop and Presto cluster running. 

To make it work you need either:
 - define environment variables for hadoop ${HADOOP_MASTER} and Presto ${PRESTO_MASTER} with their IP numbers
 - create test-configuration-local.yaml with following (example content):

```
databases:
  hive:
    host: 172.16.2.10

  presto:
    host: 192.168.205.1
```

## Running tests

Product tests are not run by default. To start them use run following command:

* On GNU/Linux
```
java -jar target/presto-product-tests-*-executable.jar --config-local tempto-configuration-local.yaml
```

* On Mac OS X
```
java -jar target/presto-product-tests-*-executable.jar --config-local file:///`pwd`/tempto-configuration-local.yaml
```

## Running tests with preconfigured docker based clusters

In case you do not have an access to hadoop cluster or do not want to spent your time on configuration, this section
describes how to run product tests with preconfigured docker based clusters.

*Running product test in docker environment requires at least 4GB free memory to be available.*

### Installing dependencies

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

On Mac OS X installing docker-toolbox gives access to preconfigured bash environment
with ```docker``` and ```docker-compose``` available. To start this bash environment
select "Docker Quickstart Terminal" from Launchpad. Note that all commands given in
further parts of this docs should be run from this environment.

##### Setting up virtual machine for docker

The default ```docker-toolbox``` setting should be decent in most cases, but setting
up docker virtual machine to have at least 4GB is required.

* To create virtual machine with name <machine> (required one time):
    ```
    docker-machine create -d virtualbox --virtualbox-memory 4096 <machine>
    ```
* To set up enviroment to use <machine> (required at docker bash startup):
    ```
    eval $(docker-machine env <machine>)
    ```

> Tip: In order to keep configuration as simple as possible one may want to change
> memory available for "default" machine directly in virtual box.


### Running product tests
Below manual describe how to set up and teardown docker clusters needed to run product tests.
It also covers actual product tests execution with usage of these clusters.

#### Configuration profiles
Configuration profiles are stored in `presto-product-tests/etc` diretory. There are two such profiles:

**distributed** - consists of single node (pseudo-distributed) hadoop cluster and multiple node presto cluster
 with one coordinator node and at least one worker node. You can select number of workers by below command:

    cd presto-product-tests/etc/distributed
    docker-compose scale presto-worker=<N>

**singlenode** - consists of single node (pseudo-distributed) hadoop cluster and one node presto cluster

#### Execution

1. Build Presto project (to have presto-server and product tests compiled).

    ```
    ./mvnw install
    ```

2. Create and start Presto cluster

    ```
    cd presto-product-tests/etc/<profile>
    docker-compose pull
    docker-compose up -d
    ```

    > Note that ```docker-compose pull``` command will ensure that latest version of docker
    > images are downloaded from docker hub.

    > Note that you can only start up hadoop dockers by using docker-compose command
    > ```docker-compose up -d hadoop-master```

3. Wait for Presto to be ready.

    To see if all the components are ready you can use below query.

    ```
    select count(node_id) from system.runtime.nodes where state = 'active';
    ```

4. Run product tests

    ```
    cd presto-product-tests/etc/<profile>
    java -jar ../../target/presto-product-tests-<version>-executable.jar
    ```

    > Note that some tests may run queries too big to fit into docker resource constraints.
    > To exclude these tests from execution you use below switch to run product tests command.
    > `-x big_query,quarantine`

    You can run product tests from your IDE, all you need to set is to set build directory to ```presto-product-tests/etc```.

5. Stop clusters

    ```
    cd presto-product-tests/etc/<profile>
    docker-compose stop
    ```
