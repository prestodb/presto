# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Build the test and build container for presto_cpp
ARG image=ghcr.io/facebookincubator/velox-dev:centos9
FROM $image
ARG cpu_target=avx
ENV CPU_TARGET=$cpu_target

COPY scripts/setup-adapters.sh /
RUN mkdir build && ( cd build &&  source /opt/rh/gcc-toolset-12/enable && \
    bash /setup-adapters.sh ) && rm -rf build && dnf remove -y conda && dnf clean all

# install miniforge
RUN curl -L -o /tmp/miniforge.sh https://github.com/conda-forge/miniforge/releases/download/23.11.0-0/Mambaforge-23.11.0-0-Linux-x86_64.sh && \
    bash /tmp/miniforge.sh -b -p /opt/miniforge && \
    rm /tmp/miniforge.sh
ENV PATH=/opt/miniforge/condabin:${PATH} 

# install test dependencies
RUN mamba create -y --name adapters python=3.8
SHELL ["mamba", "run", "-n", "adapters", "/bin/bash", "-c"]

RUN pip install https://github.com/googleapis/storage-testbench/archive/refs/tags/v0.36.0.tar.gz
RUN mamba install -y nodejs openjdk
RUN npm install -g azurite

ENV HADOOP_HOME=/usr/local/hadoop \
    HADOOP_ROOT_LOGGER="WARN,DRFA" \
    LC_ALL=C \
    LIBHDFS3_CONF=/velox/scripts/hdfs-client.xml \
    PATH=/usr/local/hadoop/bin:${PATH}

ENTRYPOINT ["/bin/bash", "-c", "source /opt/rh/gcc-toolset-12/enable && exec \"$@\"", "--"]
CMD ["/bin/bash"]
