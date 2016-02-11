# Presto product tests

## Test configuration

Test assume that you have hadoop and presto cluster running. For setting up development environment we
recommend presto-docker-devenv project. This presto-docker-devenv contains tools for building and running
docker images to be used on local developer machine for easy setup of presto instance together with dependencies
like hadoop. You can also start presto on your machine locally from IDE (please check main presto README).

To make it work you need either:
 - define environment variables for hadoop ${HADOOP_MASTER} and presto ${PRESTO_MASTER} with their IP numbers,
and ${PRESTO_PRODUCT_TESTS_ROOT} with path to the presto product test directory
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
java -jar target/presto-product-tests-*-executable.jar --config-local file://`pwd`/tempto-configuration-local.yaml
```

## Using preconfigured docker based clusters

In case you do not have an access to hadoop cluster or do not want to spent your time on configuration, this section
describes how to run product tests with preconfigured docker based clusters.

### Prerequisites

* docker >= 1.5

[https://docs.docker.com/installation/#installation](https://docs.docker.com/installation/#installation)

TLDR (Ubuntu 14.04):
```
wget -qO- https://get.docker.com/ | sh
```

* Python >= 2.6

* Presto docker development environment tool

```
pip install presto-docker-devenv
```

### Running product tests

This way of running product tests is recommended for linux users who are using docker directly (not via boot2docker).
You can do it manually using steps described below or just by running ```presto-product-tests/bin/run.sh```.

1. Set up the hadoop docker-based cluster

```
devenv hadoop build-image
devenv hadoop start
# This requires sudo access to set up network bridge between docker cluster and localhost
devenv hadoop set-ip
```

2. Build presto project

3. Set up the presto docker-based cluster

```
devenv presto build-image --presto-source . --master-config presto-product-tests/etc/master/ --worker-config presto-product-tests/etc/worker/  --no-build
devenv presto start
devenv presto set-ip
```

4. Run product tests

```
java -jar ...
```

5. Stop clusters

```
devenv presto stop
devenv hadoop stop
```

6. Clean clusters data

This is going to remove docker container and image files.

```
devenv presto clean
devenv hadoop clean
```
