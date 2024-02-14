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

ARG DEPENDENCY_IMAGE=prestissimo/dependency-x86_64-centos:latest
ARG BASE_IMAGE=quay.io/centos/centos:stream8
FROM ${DEPENDENCY_IMAGE} as prestissimo-image

ARG BUILD_TYPE=Release
ARG EXTRA_CMAKE_FLAGS=''
ARG OSNAME=centos

ENV PROMPT_ALWAYS_RESPOND=n
ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""

RUN mkdir -p /prestissimo/presto_cpp /runtime-libraries
COPY . /prestissimo/
RUN if [ "${OSNAME}" = "centos" ] ; then \
        export CC=/opt/rh/gcc-toolset-9/root/bin/gcc ; \
        export CXX=/opt/rh/gcc-toolset-9/root/bin/g++ ; \
    fi && EXTRA_CMAKE_FLAGS=${EXTRA_CMAKE_FLAGS} \
    make -j$(nproc) --directory="/prestissimo/" cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR=${BUILD_DIR} BUILD_BASE_DIR=${BUILD_BASE_DIR}
RUN ldd /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }'

#/////////////////////////////////////////////
#          prestissimo-runtime
#//////////////////////////////////////////////

FROM ${BASE_IMAGE}

ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""

COPY --chown=186:root --chmod=0775 --from=prestissimo-image /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server /usr/local/bin/
COPY --chown=186:root --chmod=0775 --from=prestissimo-image /runtime-libraries/* /usr/local/lib/
