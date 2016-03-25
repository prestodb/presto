# Presto product tests

## Installing dependencies

*Running product test requires at least 4GB free memory to be available.*

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


## Running tests

Product tests are not run by default. To start them use run following command:

```
presto-product-tests/bin/run_on_docker.sh <profile> -x quarantine,big_query
```


#### Configuration profiles

**distributed** - consists of single node (pseudo-distributed) hadoop cluster and multiple node presto cluster
 with one coordinator node and at least one worker node.

**singlenode** - consists of single node (pseudo-distributed) hadoop cluster and one node presto cluster
