# Presto native Dev Container Dockerfile

# Set base image
FROM quay.io/centos/centos:stream9

ENV PROMPT_ALWAYS_RESPOND=y
# Install necessary packages
RUN dnf config-manager --enable crb
RUN dnf update && dnf install -y epel-release
RUN dnf install -y git \
    make \
    which \
    iputils \
    hostname


# Toolchain and platform environment variables
ENV PROMPT_ALWAYS_RESPOND=n
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++

ARG OSNAME=centos
ARG BUILD_TYPE=Debug
ARG EXTRA_CMAKE_FLAGS=''
ARG NUM_THREADS=8
ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""
ENV DEPENDENCY_DIR="/deps-download"
ENV INSTALL_PREFIX="/deps-install"
# download git repo
RUN git clone https://github.com/prestodb/presto.git /source
#
WORKDIR /source/presto-native-execution

## Load Velox submodule
RUN make submodules

# Install dependencies
RUN mkdir $INSTALL_PREFIX && \
    ./scripts/setup-centos.sh && \
    ./velox/scripts/setup-centos9.sh install_adapters && \
    ./scripts/setup-adapters.sh && \
    ./velox/scripts/setup-centos9.sh install_clang15

WORKDIR /

RUN rm -rf /source

