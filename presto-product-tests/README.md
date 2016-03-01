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

```
java -jar target/presto-product-tests-*-executable.jar --config-local tempto-configuration-local.yaml
```

## Running tests with Using preconfigured docker based clusters

In case you do not have an access to hadoop cluster or do not want to spent your time on configuration, this section
describes how to run product tests with preconfigured docker based clusters.

### Prerequisites

* docker >= 1.10

[https://docs.docker.com/installation/#installation](https://docs.docker.com/installation/#installation)

For linux users:
```
wget -qO- https://get.docker.com/ | sh
```

For Mac OS X you need to install docker-machine.

[https://docs.docker.com/engine/installation/mac/](https://docs.docker.com/engine/installation/mac/).

* docker-compose >= 1.6

[https://docs.docker.com/compose/install/](https://docs.docker.com/compose/install/).

```
pip install docker-compose
```

> Note that if you are using Mac OS X and installed docker-toolbox to have docker, 
> then docker-compose should be already installed on your system.

### Running product tests

Below manual describe how to set up and teardown docker clusters needed to run product tests.
It also covers actual product tests execution with usage of these clusters.

> Note that if you using Mac OS X you may need to configure your environment to make docker commands working. 
> Additionally, please make sure that your docker machine is able to allocate at least 4GB memory.

```
docker-machine create -d virtualbox --virtualbox-memory 4096 <machine>
eval $(docker env <machine>)
```

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
    docker-compose up -d
    ```

3. Wait for Presto to be ready.

    To see if all the components are ready you can use below command.

    ```
    select count(node_id) from system.runtime.nodes where state = 'active';
    ```

4. Run product tests

    ```
    cd presto-product-tests/etc/<profile>
    java -jar ../target/presto-product-tests-<version>-executable.jar 
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
