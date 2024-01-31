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

ARG BUILD_TYPE=Release
ARG DEPENDENCY_IMAGE="localhost/centos8-prestissimo-dependency:latest"
ARG EXTRA_CMAKE_FLAGS=""

FROM ${DEPENDENCY_IMAGE} as prestissimo-image

ENV PRESTO_HOME="presto-server"
ENV PROMPT_ALWAYS_RESPOND=n
ENV BUILD_TYPE=${BUILD_TYPE}
ENV CC=/opt/rh/gcc-toolset-9/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-9/root/bin/g++
ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""

RUN mkdir -p /$PRESTO_HOME/presto_cpp
COPY CMakeLists.txt Makefile /$PRESTO_HOME/
COPY presto_cpp /$PRESTO_HOME/presto_cpp
RUN EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS} \
    make -j$(nproc) --directory="/$PRESTO_HOME/" cmake BUILD_TYPE=${BUILD_TYPE} BUILD_DIR=${BUILD_DIR} BUILD_BASE_DIR=${BUILD_BASE_DIR}
RUN EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS} \
    make -j$(nproc) --directory="/$PRESTO_HOME/" build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR=${BUILD_DIR} BUILD_BASE_DIR=${BUILD_BASE_DIR}
RUN mkdir -p /runtime-libraries
RUN ldd /$PRESTO_HOME/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }'

#/////////////////////////////////////////////
#          centos8-prestissimo-runtime
#//////////////////////////////////////////////

FROM quay.io/centos/centos:stream8

ENV PRESTO_HOME="presto-server"
ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""

COPY --chown=186:root --chmod=0775 --from=prestissimo-image /$PRESTO_HOME/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server /usr/local/bin/
COPY --chown=186:root --chmod=0775 --from=prestissimo-image /runtime-libraries/* /usr/local/lib64/
