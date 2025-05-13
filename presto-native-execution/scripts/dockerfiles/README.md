# Prestissimo - Dockerfile build

> 📝 _**Note:** Please post in Presto Slack if you have any questions_

There are two kinds of Dockerfiles:
1) A platform specific file to build dependencies. Current supported platforms are:
   1) Centos-9-stream with gcc12. Clang15 is also installed.
   2) Ubuntu-22.04 with gcc11.
2) A platform-agnostic file to build Prestissimo runtime on top of the dependency image.
### Dependency Dockerfiles
These Dockerfiles install all the dependencies including those needed for testing.
A list of dependencies can be found [here](../../README.md).
These dependencies are always built in Release mode.
The Dependency Image needs to be built only when some dependency is updated.
Prestissimo dependencies change infrequently.

### Runtime Dockerfile
This Dockerfile builds Prestissimo on top of the dependency image.
There are 2 stages:
* The first stage builds the Prestissimo binary.
* The second stage creates a Prestissimo runtime image from
a base image and copies the binary and shared libraries that are required
for execution.

Run the following command to see the services available to build images.
`docker` also can be used instead of `podman`.
```
podman compose config --services
```

## Quick Start

### 1. Clone the Presto repository and checkout Prestissimo submodules

```bash
git clone https://github.com/prestodb/presto
cd presto/presto-native-execution && make submodules
```

### 2. Build Dependency Image using docker/podman compose

```bash
podman compose build centos-native-dependency
```

### 3. Build Runtime Image

Specify the build type using ``BUILD_TYPE`` (defaults to Release)
build argument. The ``BUILD_TYPE`` value is used to set the ``CMAKE_BUILD_TYPE``
CMake variable. The allowed values are specified [here](https://cmake.org/cmake/help/latest/variable/CMAKE_BUILD_TYPE.html).

```bash
podman compose build centos-native-runtime
# For a debug build, specify the BUILD_TYPE argument.
podman compose build --build-arg BUILD_TYPE=Debug centos-native-runtime
```
