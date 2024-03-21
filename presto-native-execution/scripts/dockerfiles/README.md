# Prestissimo - Dockerfile build

> 📝 _**Note:** Please post in Presto Slack if you have any questions_

There are two dockerfiles: 1) dependency.dockerfile 2) prestissimo.dockerfile
### dependency.dockefile
This dockerfile installs all the dependencies including those needed for testing.
By default, the dependencies are built in Release mode.
This image needs to be built only when some dependency is updated.
Prestissimo dependencies change infrequently.

### prestissimo.dockefile
This dockerfile builds Prestissimo on top of the dependency image.
There are 2 stages. The first stage builds the Prestissimo binary.
The second stage creates a Prestissimo runtime image from
a base image and copies the binary and shared libaries that are required
for execution.

## Quick Start

### 1. Clone this repository

```bash
git clone https://github.com/prestodb/presto
```

### 2. (Optional) Define and export OS name, Docker image name and image tag variables

```bash
# Possible values are centos (default), ubuntu
export OSNAME=ubuntu
# defaults to 'prestissimo/dependency-$(ARCH)-$(OSNAME)'
export DEPENDENCY_IMAGE_NAME=prestissimo/dependency-x86_64-ubuntu
# defaults to 'latest'
export DEPENDENCY_IMAGE_TAG=YYYY-MM-DD
# defaults to 'quay.io/centos/centos:stream8'
export DEPENDENCY_BASE_IMAGE=public.ecr.aws/ubuntu/ubuntu:22.04_stable
# defaults to 'prestissimo/runtime-$(ARCH)-$(OSNAME)'
export RUNTIME_IMAGE_NAME=prestissimo/runtime-x86_64-ubuntu
# defaults to 'latest'
export RUNTIME_IMAGE_TAG=YYYY-MM-DD
# defaults to 'quay.io/centos/centos:stream8'
export RUNTIME_BASE_IMAGE=public.ecr.aws/ubuntu/ubuntu:22.04_stable
```

### 3. Build Dependency Image

Specify the build type using BUILD_TYPE (defaults to Release)
environment variable.

```bash
cd presto/presto-native-execution
make dependency-image
```

### 4. Build Runtime image

Specify the build type using BUILD_TYPE (defaults to Release)
environment variable.

```bash
make runtime-image
```
