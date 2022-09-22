CircleCi integration is controlled by the `./circleci/config.yml` file.  Our
config currently contains two workflows.  One is triggered on every pull request update.
The other workflow runs nightly to verify our compatibility with prestodb internal protocol.

The PR workflow is named `dist-compile` and has 4 jobs, 2 to build and run unit tests on linux and macos
and 2 to check code formatting and license headers:
* linux-build
* macos-build
* format-check
* header-check

## Running locally

The linux container based jobs can be run locally using the `circleci` cli:

```
    circleci local execute --job JOB_NAME
```

For example to run unit tests use:

```
    circleci local execute --job linux-build
```

A Nightly build with prestodb/master sync checks that the presto_protocol library
remains in sync with Presto Java.

Run the nightly sync job locally:
```
    circleci local execute --job presto-sync
```

## Install CircleCi cli
```
    curl -fLSs https://circle.ci/cli | bash
```

To use containers Docker must be installed.  Here are instructions to [Install
Docker on macos](https://docs.docker.com/docker-for-mac/install/).  Docker deamon
must be running before issuing the `circleci` commands.

### Macos testing

Macos testing is done by using the CircleCi macos executor and installing
dependencies each time the job is run.  This executor cannot be run locally.
The script `scripts/setup-macos.sh` contains commands that are run as part of
this job to install these dependencies.

### Linux testing

Linux testing uses a Docker container. The container build depends on the Velox CircleCi container. Check
velox/.circleci/config.yml to see that the base container in circleci-container.dockfile is using the latest.
The container build uses Docker and should be run on your macos or linux laptop with Docker installed and
running.

#### Build the base container:

* In an up-to-date clone of velox (maybe you have one?)

```
git clone git@github.com:facebookincubator/velox.git
cd velox
make base-container
```
* Wait - This step takes rather a long time.  It is building clang-format v8 to be compatible with fbcode
* When the base container is finished the new container name will be printed on the console.
* Push the container to DockerHub
```
docker push prestocpp/base-container:$USER-YYYYMMDD
```
* After the push, update `scripts/velox-container.dockfile` with the newly build base container name

#### Build the dependencies container

* If you have a new base-container update scripts/velox-container.dockfile to refer to it
* Build the velox container
```
make velox-container.dockfile
```
* Wait - This takes a few minutes, but not nearly as long as the base container.
* When the velox container is finished the new container name will be printed on the console.
* Push the container to DockerHub
```
docker push prestocpp/velox-container:$USER-YYYYMMDD
```
* Update `.circleci/config.yml` with the newly built circleci container name.
  There are two places in the config.yml file that refer to the container, update
  both.
