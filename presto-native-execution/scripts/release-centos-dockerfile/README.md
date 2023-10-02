# Prestissimo - Dockerfile build

> 💡 _**PrestoDB** repository: [Presto - https://github.com/prestodb/presto](https://github.com/prestodb/presto)_

> 💡 _**Velox** repository: [Velox - https://github.com/facebookincubator/velox](https://github.com/facebookincubator/velox)_

> 📝 _**Note:** Please post in Presto Slack if you have any questions_

## Quick Start

### 1. Clone this repository

```bash
git clone https://github.com/prestodb/presto prestodb
```

### 2. (Optional) Define and export Docker registry, image name and image tag variables

> 📝 _**Note:** Remember to end your `IMAGE_REGISTRY` with `/` as this is required for full tag generation._

> 💡 _**Tip:** Depending on your configuration you may need to run all bellow commands as root user, to switch type as your first command `sudo su`_

> 💡 _**Tip:** If `IMAGE_REGISTRY` is not specified `IMAGE_PUSH` should be set `'0'` or docker image push stage will fail._

Type in you console, changing variables values to meet your needs:

```bash
export IMAGE_NAME='presto/prestissimo-'$(uname -p)'-centos'
# defaults to 'latest'
export IMAGE_TAG='latest'
# defaults to ''
export IMAGE_REGISTRY='https://my_docker_registry.com/'
# defaults to '0'
export IMAGE_PUSH='0'
```

Additionally if you are using docker registry cache for downloading images bellow values should be exported as follows:

```bash
# defaults to 'quay.io/centos/'
export IMAGE_CACHE_REGISTRY='my.company.registry.cache.xyz:5000/path/to/library/'
# this will be concatenated with (defaults to 'centos:stream8')
export IMAGE_BASE_NAME='centos:stream8'
# resulting in BASE_IMAGE:
export BASE_IMAGE="${IMAGE_CACHE_REGISTRY}${IMAGE_BASE_NAME}"
```

### 3. Make sure Docker daemon is running

(Ubuntu users) Type in your console:

```bash
systemctl status docker
```

### 4. Build Dockerfile repo

Type in your console:

```bash
cd prestodb/presto-native-execution
make runtime-container
```

The process is fully automated and require no interaction for user. The process of building images for the first time can take up to couple of hours (~1-2h using 10 processor cores).

### 5. Run container

Depending on values you have set the container tag is defined as

`PRESTO_CPP_TAG="${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}"`

for default values this will be just:

`PRESTO_CPP_TAG=presto/prestissimo-avx-centos:latest`

to run container build with default tag execute:

```bash
docker run "presto/prestissimo-avx-centos:latest" \
            --use-env-params \
            --discovery-uri=http://localhost:8080 \
            --http-server-port=8080"
```

to run container interactively, not executing entrypoint file:

```bash
docker run -it --entrypoint=/bin/bash "presto/prestissimo-avx-centos:latest"
```

## Container manual build

```bash
export IMAGE_NAME='presto/prestissimo-'$(uname -p)'-centos'
export IMAGE_TAG='latest'
export IMAGE_REGISTRY='some-registry.my-domain.com/'
export IMAGE_PUSH='0'
export PRESTODB_REPOSITORY=$(git config --get remote.origin.url)
export PRESTODB_CHECKOUT=$(git show -s --format="%H" HEAD)
```

Where `IMAGE_NAME` and `IMAGE_TAG` will be the prestissimo release image name and tag, `IMAGE_REGISTRY` will be the registry that the image will be tagged with and which will be used to download the images from previous stages in case there are no cached images locally. The `PRESTODB_REPOSITORY` and `PRESTODB_CHECKOUT` will be used as a build repository and branch inside the container. You can set them manually or as provided using git commands.

Then to manually build the Dockerfile type:

```bash
cd presto-native-execution/scripts/release-centos-dockerfile
docker build \
    --network=host \
    --build-arg http_proxy  \
    --build-arg https_proxy \
    --build-arg no_proxy    \
    --build-arg PRESTODB_REPOSITORY \
    --build-arg PRESTODB_CHECKOUT \
    --tag "${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}" .
```


## Build process - more info

> 📝 _**Note:** `prestissimo` image size is with artifacts ~35 GB, without ~10 GB_

In this step most of runtime and build time dependencies are downloaded, configured and installed. The image built in this step is a starting point for both second and third stage. It will be build 'once per breaking change' in any of repositories.
This step install Maven, Java 8, Python3-Dev, libboost-dev and lots of other massive frameworks, libraries and applications and ensures that all of steps from 2 stage will run with no errors.

Build directory structure used inside the Dockerfile:

```bash
### DIRECTORY AND MAIN BUILD ARTIFACTS
## Native Presto JAVA build artifacts:
/root/.m2/

## Build, third party dependencies, mostly for adapters
/opt/dependency/
/opt/dependency/aws-sdk-cpp
/opt/dependency/install/
/opt/dependency/install/run/
/opt/dependency/install/bin/
/opt/dependency/install/lib64/

## Root PrestoDB application directory
/opt/presto/

## Root GitHub clone of PrestoDB repository
/opt/presto/_repo/

## Root PrestoCpp subdirectory
/opt/presto/_repo/presto-native-execution/

## Root Velox GitHub repository directory, as PrestoDB submodule
/opt/presto/_repo/presto-native-execution/Velox

## Root build results directory for PrestoCpp with Velox
/opt/presto/_repo/presto-native-execution/_build/release/
/opt/presto/_repo/presto-native-execution/_build/release/velox/
/opt/presto/_repo/presto-native-execution/_build/release/presto_cpp/
```

Release image build - mostly with only the must-have runtime files, including presto_server build presto executable and some libraries. What will be used in the final released image depends on user needs and can be adjusted.
